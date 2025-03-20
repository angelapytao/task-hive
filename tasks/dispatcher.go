package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"gitlab.ituchong.com/tc-common/common-task-hive/common"
	"gitlab.ituchong.com/tc-common/common-task-hive/model"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 生成唯一ID
func generateID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), rand.Intn(1000))
}

// RegisterWorker 注册Worker并保持心跳
func RegisterWorker(ctx context.Context, client *clientv3.Client) *Worker {
	workerID := generateID()
	resp, err := client.Grant(ctx, 10)
	if err != nil {
		log.Fatal(err)
	}

	worker := &Worker{
		ID:       workerID,
		Lease:    resp.ID,
		Capacity: 10, // 设置默认并发处理能力
	}

	workerData, _ := json.Marshal(worker)
	_, err = client.Put(ctx, common.WorkersKey+workerID, string(workerData),
		clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}

	// 保持租约存活
	keepAliveCh, err := client.KeepAlive(ctx, resp.ID)
	if err != nil {
		log.Fatal(err)
	}

	// 异步处理续约响应
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("Worker %s 上下文已取消，停止租约续约 \n", workerID)
				return
			case resp, ok := <-keepAliveCh:
				if !ok {
					log.Printf("Worker %s 租约续约通道已关闭，Worker可能已下线 \n", workerID)
					return
				}
				if resp == nil {
					log.Printf("Worker %s 租约续约失败，尝试重新注册 \n", workerID)
					// 重新注册
					newResp, err := client.Grant(ctx, 10)
					if err != nil {
						log.Printf("Worker %s 重新获取租约失败: %v \n", workerID, err)
						return
					}

					// update lease Id
					worker.Lease = newResp.ID
					workerData, _ := json.Marshal(worker)
					_, err = client.Put(ctx, common.WorkersKey+workerID, string(workerData), clientv3.WithLease(newResp.ID))
					if err != nil {
						log.Printf("Worker %s 使用新租约重新注册失败: %v \n", workerID, err)
						return
					}
					keepAliveCh, err = client.KeepAlive(ctx, newResp.ID)
					if err != nil {
						log.Printf("Worker %s 重新建立租约续约通道失败： %v\n", workerID, err)
						return
					}

					log.Printf("Worker %s 已成功重新注册 \n", workerID)
				}
				log.Printf("Worker %s 租约成功续约，TTL: %d", workerID, resp.TTL)
			}
		}
	}()

	return worker
}

// SubmitTask 提交任务
func SubmitTask(client *clientv3.Client, task model.Task) error {
	if task.Priority == 0 {
		task.Priority = 5 // 默认优先级
	}
	task.Status = common.PENDING
	task.CreateTime = time.Now()

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	_, err = client.Put(context.Background(), common.PendingTasksKey+task.ID, string(data))
	return err
}

// StartDispatcher1 启动任务分发器 deprecated
func StartDispatcher1(client *clientv3.Client, ctx context.Context) {
	log.Println("Starting dispatcher...")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Dispatcher 收到退出信号，正在停止...")
				return
			default:
				// 获取并按优先级排序待处理任务
				pendingResp, err := client.Get(context.Background(), common.PendingTasksKey,
					clientv3.WithPrefix())
				if err != nil {
					log.Println("获取任务失败:", err)
					time.Sleep(time.Second)
					continue
				}

				tasks := make([]model.Task, 0)
				for _, kv := range pendingResp.Kvs {
					var task model.Task
					if err := json.Unmarshal(kv.Value, &task); err != nil {
						continue
					}
					tasks = append(tasks, task)
				}

				// 按优先级排序
				sort.Slice(tasks, func(i, j int) bool {
					return tasks[i].Priority > tasks[j].Priority
				})

				// 获取可用Worker并计算负载
				workersResp, err := client.Get(context.Background(), common.WorkersKey,
					clientv3.WithPrefix())
				if err != nil || len(workersResp.Kvs) == 0 {
					time.Sleep(time.Second)
					continue
				}

				workers := make([]Worker, 0)
				for _, kv := range workersResp.Kvs {
					var worker Worker
					if err := json.Unmarshal(kv.Value, &worker); err != nil {
						continue
					}
					workers = append(workers, worker)
				}

				// 按负载排序
				sort.Slice(workers, func(i, j int) bool {
					return workers[i].TaskCount < workers[j].TaskCount
				})

				// 分配任务给负载最小的Worker
				for _, task := range tasks {
					if len(workers) == 0 {
						break
					}

					selectedWorker := workers[0]
					if selectedWorker.TaskCount >= selectedWorker.Capacity {
						break
					}

					// 构建事务：原子化移动任务
					task.Status = common.PROCESSING
					taskData, _ := json.Marshal(task)

					txn := client.Txn(context.Background())
					txnResp, err := txn.
						If(clientv3.Compare(clientv3.Version(common.PendingTasksKey+task.ID), ">", 0)).
						Then(
							clientv3.OpDelete(common.PendingTasksKey+task.ID),
							clientv3.OpPut(common.ProcessingKey+selectedWorker.ID+"/"+task.ID,
								string(taskData), clientv3.WithLease(clientv3.NoLease)),
						).
						Commit()

					if err == nil && txnResp.Succeeded {
						log.Printf("任务 %s 已分配给 Worker %s", task.ID, selectedWorker.ID)
						selectedWorker.TaskCount++
						workers = workers[1:] // 移除已分配的worker
					}
				}

				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	log.Println("Dispatcher started successfully")
}

// StartDispatcher 启动任务调度器
func StartDispatcher(client *clientv3.Client, ctx context.Context) {
	log.Println("Starting task dispatcher...")

	// 创建两个watcher，一个用于监听待处理任务，一个用于监听延迟任务触发器
	pendingWatcher := clientv3.NewWatcher(client)
	delayedWatcher := clientv3.NewWatcher(client)
	defer pendingWatcher.Close()
	defer delayedWatcher.Close()

	// 监听待处理任务
	pendingWatchChan := pendingWatcher.Watch(ctx, common.PendingTasksKey, clientv3.WithPrefix())

	// 监听延迟任务触发器的删除事件（TTL过期）
	delayedWatchChan := delayedWatcher.Watch(ctx, common.DelayedTriggerKey, clientv3.WithPrefix())

	// 处理待处理任务
	go func() {
		for resp := range pendingWatchChan {
			for _, ev := range resp.Events {
				if ev.Type == clientv3.EventTypePut {
					// 解析任务
					var task model.Task
					if err := json.Unmarshal(ev.Kv.Value, &task); err != nil {
						log.Printf("解析任务失败: %v", err)
						continue
					}

					// 分配任务给可用的Worker
					assignTask(ctx, client, task)
				}
			}
		}
	}()

	// 处理延迟任务
	go func() {
		for resp := range delayedWatchChan {
			for _, ev := range resp.Events {
				// 当延迟触发器过期（被删除）时，从延迟队列中取出任务并重新提交
				if ev.Type == clientv3.EventTypeDelete {
					// 从键中提取任务ID
					taskID := string(ev.Kv.Key)[len(common.DelayedTriggerKey):]

					// 从延迟队列中获取任务
					delayKey := common.DelayedKey + taskID
					getResp, err := client.Get(ctx, delayKey)
					if err != nil {
						log.Printf("获取延迟任务失败: %v", err)
						continue
					}

					if len(getResp.Kvs) == 0 {
						log.Printf("延迟任务不存在: %s", taskID)
						continue
					}

					// 解析任务
					var task model.Task
					if err := json.Unmarshal(getResp.Kvs[0].Value, &task); err != nil {
						log.Printf("解析延迟任务失败: %v", err)
						continue
					}

					// 删除延迟队列中的任务
					client.Delete(ctx, delayKey)

					// 重新提交任务到待处理队列
					log.Printf("延迟任务 %s 触发，重新提交到待处理队列", task.ID)
					SubmitTask(client, task)
				}
			}
		}
	}()

	// 等待上下文取消
	<-ctx.Done()
	log.Println("Task dispatcher stopped")
}

// assignTask 分配任务给可用的Worker
func assignTask(ctx context.Context, client *clientv3.Client, task model.Task) {
	// 获取所有可用的Worker
	resp, err := client.Get(ctx, common.WorkersKey, clientv3.WithPrefix())
	if err != nil {
		log.Printf("获取Worker列表失败: %v", err)
		return
	}

	if len(resp.Kvs) == 0 {
		log.Println("没有可用的Worker")
		return
	}

	// 选择一个Worker（这里可以实现更复杂的负载均衡策略）
	var selectedWorker Worker
	minTasks := int(^uint(0) >> 1) // 最大整数

	for _, kv := range resp.Kvs {
		var worker Worker
		if err := json.Unmarshal(kv.Value, &worker); err != nil {
			continue
		}

		// 选择任务数最少的Worker
		if worker.TaskCount < minTasks {
			selectedWorker = worker
			minTasks = worker.TaskCount
		}
	}

	// 将任务分配给选中的Worker
	task.Status = common.PROCESSING
	taskData, _ := json.Marshal(task)

	// 更新Worker的任务计数
	selectedWorker.TaskCount++
	workerData, _ := json.Marshal(selectedWorker)

	// 使用事务确保操作的原子性
	processingKey := common.ProcessingKey + selectedWorker.ID + "/" + task.ID
	pendingKey := common.PendingTasksKey + task.ID
	workerKey := common.WorkersKey + selectedWorker.ID

	// 构建事务
	txn := client.Txn(ctx)
	txnResp, err := txn.
		If(clientv3.Compare(clientv3.Version(pendingKey), ">", 0)).
		Then(
			clientv3.OpPut(processingKey, string(taskData)),
			clientv3.OpDelete(pendingKey),
			clientv3.OpPut(workerKey, string(workerData)),
		).
		Commit()

	if err != nil {
		log.Printf("分配任务事务失败: %v", err)
		return
	}

	if !txnResp.Succeeded {
		log.Printf("任务 %s 可能已被其他调度器分配", task.ID)
		return
	}

	log.Printf("任务 %s 已分配给Worker %s", task.ID, selectedWorker.ID)
}

// StartWorkerMonitor 监控Worker故障并重新分配任务
func StartWorkerMonitor(client *clientv3.Client, ctx context.Context) {
	watcher := clientv3.NewWatcher(client)
	defer watcher.Close()

	watchChan := watcher.Watch(context.Background(), common.WorkersKey, clientv3.WithPrefix())

	// 监听上下文取消
	go func() {
		<-ctx.Done()
		log.Println("Monitor 收到退出信号，正在停止...")
		watcher.Close()
	}()

	for resp := range watchChan {
		for _, ev := range resp.Events {
			if ev.Type == clientv3.EventTypeDelete {
				workerID := string(ev.Kv.Key)[len(common.WorkersKey):]
				log.Printf("检测到Worker %s 下线，重新分配任务", workerID)

				// 获取该Worker所有未完成任务
				resp, err := client.Get(context.Background(),
					common.ProcessingKey+workerID+"/",
					clientv3.WithPrefix())
				if err != nil {
					continue
				}

				// 将任务移回pending队列
				for _, kv := range resp.Kvs {
					taskKey := string(kv.Key)
					var task model.Task
					if err := json.Unmarshal(kv.Value, &task); err != nil {
						continue
					}

					task.Status = common.PENDING
					taskData, _ := json.Marshal(task)

					txn := client.Txn(context.Background())
					txn.Then(
						clientv3.OpDelete(taskKey),
						clientv3.OpPut(common.PendingTasksKey+task.ID, string(taskData)),
					)
					if _, err := txn.Commit(); err == nil {
						log.Printf("已重新分配任务 %s", task.ID)
					}
				}
			}
		}
	}
}
