package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"task-hive/common"
	"task-hive/model"
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
		for range keepAliveCh {
			// 保持租约活跃
		}
	}()

	return worker
}

// SubmitTask 提交任务
func SubmitTask(client *clientv3.Client, task model.Task) error {
	if task.Priority == 0 {
		task.Priority = 5 // 默认优先级
	}
	task.Status = "pending"
	task.CreateTime = time.Now()

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	_, err = client.Put(context.Background(), common.PendingTasksKey+task.ID, string(data))
	return err
}

// StartDispatcher 启动任务分发器
func StartDispatcher(client *clientv3.Client) {
	log.Println("Starting dispatcher...")

	go func() {
		for {
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
				task.Status = "processing"
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
	}()

	log.Println("Dispatcher started successfully")
}

// StartWorkerMonitor 监控Worker故障并重新分配任务
func StartWorkerMonitor(client *clientv3.Client) {
	watcher := clientv3.NewWatcher(client)
	defer watcher.Close()

	watchChan := watcher.Watch(context.Background(), common.WorkersKey, clientv3.WithPrefix())
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

					task.Status = "pending"
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
