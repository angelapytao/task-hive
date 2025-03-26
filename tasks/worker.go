package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"gitlab.ituchong.com/tc-common/common-task-hive/common"
	"gitlab.ituchong.com/tc-common/common-task-hive/model"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"math/rand"
	"time"
)

// 全局任务处理器映射
var taskProcessors = map[string]func(task *model.Task) (string, error){
	//"spider": ProcessSpiderTask,
}

// Worker 表示工作节点
type Worker struct {
	ID            string
	Lease         clientv3.LeaseID
	TaskCount     int // 当前正在处理的任务数
	LastHeartbeat time.Time
	Capacity      int // 最大并发任务数
}

// RegisterTaskProcessor 注册任务处理器
func RegisterTaskProcessor(taskType string, processor func(task *model.Task) (string, error)) {
	taskProcessors[taskType] = processor
}

// ProcessTasks Worker处理任务
func (w *Worker) ProcessTasks(client *clientv3.Client) {
	watchPrefix := common.ProcessingKey + w.ID + "/"
	log.Printf("Worker %s 开始监听任务: %s", w.ID, watchPrefix)

	watcher := clientv3.NewWatcher(client)
	defer watcher.Close()

	watchChan := watcher.Watch(context.Background(), watchPrefix, clientv3.WithPrefix())
	for resp := range watchChan {
		for _, ev := range resp.Events {
			if ev.Type == clientv3.EventTypePut {
				var task model.Task
				if err := json.Unmarshal(ev.Kv.Value, &task); err != nil {
					continue
				}

				// 创建带超时的上下文
				ctx, cancel := context.WithTimeout(context.Background(), common.TaskTimeout)

				// 异步处理任务
				go func(ctx context.Context, task model.Task) {
					defer cancel()

					log.Printf("model.Worker %s 开始处理任务 %s", w.ID, task.ID)

					// 处理任务（这里需要实现实际的任务处理逻辑）
					result, err := w.executeTask(ctx, task)

					if err != nil {
						task.Error = err.Error()
						task.Status = common.FAILED
						if task.RetryCount < common.MaxRetries {
							// 重试逻辑
							task.RetryCount++
							task.Status = common.PENDING

							// 如果RetryDelay为0，则使用指数退避策略计算延迟时间
							if task.RetryDelay == 0 {
								// 基础延迟为2秒，每次重试延迟时间翻倍，并添加一些随机性
								baseDelay := 2 * time.Second
								retryDelay := time.Duration(1<<(task.RetryCount-1)) * baseDelay
								// 添加最多30%的随机抖动，避免多个任务同时重试
								jitter := time.Duration(rand.Float64() * 0.3 * float64(retryDelay))
								task.RetryDelay = retryDelay + jitter
							}
							log.Printf("任务 %s 将在 %.2f 秒后重试 (第 %d 次重试)",
								task.ID, task.RetryDelay.Seconds(), task.RetryCount)

							// 使用事务原子性地处理延迟任务
							delayedTask, _ := json.Marshal(task)
							delayKey := fmt.Sprintf("%s%s", common.DelayedKey, task.ID)
							processingKey := common.ProcessingKey + w.ID + "/" + task.ID

							// 创建租约
							lease, err := client.Grant(ctx, int64(task.RetryDelay.Seconds()))
							if err != nil {
								log.Printf("创建租约失败: %v", err)
								return
							}

							triggerKey := fmt.Sprintf("%s%s", common.DelayedTriggerKey, task.ID)

							// 使用事务确保原子性
							txn := client.Txn(ctx)
							_, err = txn.Then(
								clientv3.OpPut(delayKey, string(delayedTask)),
								clientv3.OpPut(triggerKey, task.ID, clientv3.WithLease(lease.ID)),
								clientv3.OpDelete(processingKey),
							).Commit()

							if err != nil {
								log.Printf("提交延迟任务事务失败: %v", err)
							} else {
								// 事务成功，更新Worker任务计数
								//w.TaskCount--
								// 使用事务原子性地更新Worker的TaskCount
								w.updateTaskCount(ctx, client, -1)
							}
						} else {
							// 超过重试次数，移至失败队列
							taskData, _ := json.Marshal(task)
							processingKey := common.ProcessingKey + w.ID + "/" + task.ID
							failedKey := common.FailedKey + task.ID

							// 使用事务确保原子性
							txn := client.Txn(ctx)
							_, err := txn.Then(
								clientv3.OpPut(failedKey, string(taskData)),
								clientv3.OpDelete(processingKey),
							).Commit()

							if err != nil {
								log.Printf("提交失败任务事务失败: %v", err)
							} else {
								//// 事务成功，更新Worker任务计数
								//w.TaskCount--
								// 事务成功，更新Worker任务计数
								// 使用事务原子性地更新Worker的TaskCount
								w.updateTaskCount(ctx, client, -1)
							}
						}
					} else {
						// 任务成功完成
						task.Status = common.COMPLETED
						task.Result = result
						taskData, _ := json.Marshal(task)

						processingKey := common.ProcessingKey + w.ID + "/" + task.ID
						completedKey := common.CompletedKey + task.ID

						// 使用事务确保原子性
						txn := client.Txn(ctx)
						_, err := txn.Then(
							clientv3.OpPut(completedKey, string(taskData)),
							clientv3.OpDelete(processingKey),
						).Commit()

						if err != nil {
							log.Printf("提交完成任务事务失败: %v", err)
						} else {
							// 事务成功，更新Worker任务计数
							//w.TaskCount--
							log.Printf("COMPLETED worker: %s, taskCount: %d", w.ID, w.TaskCount)
							w.updateTaskCount(ctx, client, -1)
						}
					}
				}(ctx, task)
			}
		}
	}
}

// updateTaskCount 原子性地更新Worker的任务计数
func (w *Worker) updateTaskCount(ctx context.Context, client *clientv3.Client, delta int) {
	// 创建一个新的上下文，避免使用可能已取消的上下文
	updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 使用CAS操作确保原子性更新
	for retry := 0; retry < 3; retry++ {
		// 获取当前Worker状态
		resp, err := client.Get(updateCtx, common.WorkersKey+w.ID)
		if err != nil || len(resp.Kvs) == 0 {
			log.Printf("获取Worker状态失败: %v", err)
			return
		}

		var currentWorker Worker
		if err := json.Unmarshal(resp.Kvs[0].Value, &currentWorker); err != nil {
			log.Printf("解析Worker状态失败: %v", err)
			return
		}

		// 更新任务计数
		currentWorker.TaskCount += delta
		if currentWorker.TaskCount < 0 {
			currentWorker.TaskCount = 0 // 防止计数为负
		}

		// 更新本地Worker对象
		w.TaskCount = currentWorker.TaskCount

		// 更新LastHeartbeat
		currentWorker.LastHeartbeat = time.Now()

		// 序列化更新后的Worker
		workerData, _ := json.Marshal(currentWorker)

		// 使用CAS操作确保原子性更新
		txn := client.Txn(updateCtx)
		txnResp, err := txn.
			If(clientv3.Compare(clientv3.ModRevision(common.WorkersKey+w.ID), "=", resp.Kvs[0].ModRevision)).
			Then(clientv3.OpPut(common.WorkersKey+w.ID, string(workerData), clientv3.WithLease(currentWorker.Lease))).
			Commit()

		if err != nil {
			log.Printf("更新Worker状态事务失败: %v", err)
			time.Sleep(time.Duration(50*(retry+1)) * time.Millisecond) // 退避重试
			continue
		}

		if txnResp.Succeeded {
			log.Printf("Worker %s 任务计数更新为 %d", w.ID, currentWorker.TaskCount)
			return
		}

		log.Printf("Worker状态已被其他进程修改，重试更新...")
		time.Sleep(time.Duration(50*(retry+1)) * time.Millisecond) // 退避重试
	}

	log.Printf("更新Worker任务计数失败，已达到最大重试次数")
}

// 执行具体任务
func (w *Worker) executeTask(ctx context.Context, task model.Task) (string, error) {
	// 这里实现实际的任务处理逻辑
	delay := time.NewTimer(2 * time.Second)
	// 当 time.After() 的定时器未正确停止时，其资源永远不会被垃圾回收。改进方案是使用 time.NewTimer() 配合上下文管理：
	select {
	case <-ctx.Done():
		if !delay.Stop() {
			<-delay.C // 确保通道被清空
		}
		return "", fmt.Errorf("任务执行超时")
	//case <-time.After(time.Duration(rand.Intn(3)+1) * time.Second):
	case <-delay.C:
		processor, exists := taskProcessors[task.Type]
		if !exists {
			log.Printf("未找到任务类型 %s 的处理器", task.Type)
			// 更新任务状态为失败
			task.Status = common.FAILED
			task.Error = fmt.Sprintf("未知的任务类型: %s", task.Type)
			// 更新任务状态代码...
			return "", fmt.Errorf(task.Error)
		}
		result, err := processor(&task)
		return fmt.Sprintf("任务 %s 执行结果: %v", task.ID, result), err
	}
}
