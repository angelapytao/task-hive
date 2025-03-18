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
	"spider": ProcessSpiderTask,
}

// Worker 表示工作节点
type Worker struct {
	ID            string
	Lease         clientv3.LeaseID
	TaskCount     int // 当前正在处理的任务数
	LastHeartbeat time.Time
	Capacity      int // 最大并发任务数
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
							SubmitTask(client, task)
						} else {
							// 超过重试次数，移至失败队列
							taskData, _ := json.Marshal(task)
							client.Put(ctx, common.FailedKey+task.ID, string(taskData))
						}
					} else {
						// 任务成功完成
						task.Status = common.COMPLETED
						task.Result = result
						taskData, _ := json.Marshal(task)
						client.Put(ctx, common.CompletedKey+task.ID, string(taskData))
					}

					// 删除处理中的任务
					client.Delete(ctx, common.ProcessingKey+w.ID+"/"+task.ID)
					w.TaskCount--
				}(ctx, task)
			}
		}
	}
}

// 执行具体任务
func (w *Worker) executeTask(ctx context.Context, task model.Task) (string, error) {
	// 这里实现实际的任务处理逻辑
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("任务执行超时")
	case <-time.After(time.Duration(rand.Intn(3)+1) * time.Second):
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
