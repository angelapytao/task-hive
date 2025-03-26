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

// 全局变量，用于轮询策略
var lastWorkerIndex = 0

// 生成唯一ID
func generateID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), rand.Intn(1000))
}

// RegisterWorker 注册Worker并保持心跳
func RegisterWorker(ctx context.Context, client *clientv3.Client, capacity int) *Worker {
	workerID := generateID()
	resp, err := client.Grant(ctx, 10)
	if err != nil {
		log.Fatal(err)
	}

	worker := &Worker{
		ID:            workerID,
		Lease:         resp.ID,
		Capacity:      capacity, // 设置并发处理能力
		TaskCount:     0,
		LastHeartbeat: time.Now(),
	}

	workerData, _ := json.Marshal(worker)
	_, err = client.Put(ctx, common.WorkersKey+workerID, string(workerData),
		clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}

	// 保持租约存活
	keepAliveCh, err := client.KeepAlive(ctx, resp.ID)
	log.Printf("Worker %s, LeaseId: %v", workerID, resp.ID)
	if err != nil {
		log.Fatal(err)
	}

	// 在上下文取消时主动删除Worker记录
	go func() {
		<-ctx.Done()
		log.Printf("Worker %s 正在下线，清理资源...\n", workerID)

		// 创建一个新的上下文用于清理操作
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// 主动删除Worker记录
		_, err := client.Delete(cleanupCtx, common.WorkersKey+workerID)
		if err != nil {
			log.Printf("删除Worker记录失败: %v，将等待租约过期\n", err)
		} else {
			log.Printf("Worker %s 已成功从注册表中删除\n", workerID)
		}

		// 主动撤销租约
		_, err = client.Revoke(cleanupCtx, worker.Lease)
		if err != nil {
			log.Printf("撤销租约失败: %v，将等待租约过期\n", err)
		} else {
			log.Printf("Worker %s 的租约已成功撤销\n", workerID)
		}
	}()

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
					log.Printf("Worker %s, New LeaseId: %v", workerID, newResp.ID)
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
				} else {
					// 使用CAS操作确保原子性更新心跳时间
					for retry := 0; retry < 3; retry++ {
						// 获取当前Worker状态
						getResp, err := client.Get(ctx, common.WorkersKey+workerID)
						if err != nil || len(getResp.Kvs) == 0 {
							log.Printf("获取Worker状态失败: %v", err)
							break
						}

						var currentWorker Worker
						if err := json.Unmarshal(getResp.Kvs[0].Value, &currentWorker); err != nil {
							log.Printf("解析Worker状态失败: %v", err)
							break
						}

						// 只更新心跳时间，保留当前的TaskCount
						currentWorker.LastHeartbeat = time.Now()
						// 更新本地Worker对象的TaskCount
						worker.TaskCount = currentWorker.TaskCount

						// 序列化更新后的Worker
						workerData, _ := json.Marshal(currentWorker)

						// 使用CAS操作确保原子性更新
						txn := client.Txn(ctx)
						txnResp, err := txn.
							If(clientv3.Compare(clientv3.ModRevision(common.WorkersKey+workerID), "=", getResp.Kvs[0].ModRevision)).
							Then(clientv3.OpPut(common.WorkersKey+workerID, string(workerData), clientv3.WithLease(resp.ID))).
							Commit()

						if err != nil {
							log.Printf("更新Worker心跳时间事务失败: %v", err)
							time.Sleep(time.Duration(50*(retry+1)) * time.Millisecond) // 退避重试
							continue
						}

						if txnResp.Succeeded {
							log.Printf("Worker %s 租约成功续约，TTL: %d, workerCount: %d", workerID, resp.TTL, currentWorker.TaskCount)
							break
						}

						log.Printf("Worker状态已被其他进程修改，重试更新心跳...")
						time.Sleep(time.Duration(50*(retry+1)) * time.Millisecond) // 退避重试
					}
				}

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

	// 等待Worker节点注册
	waitForWorkers(ctx, client)

	processPendingTasks(ctx, client)

	// 创建两个watcher，一个用于监听待处理任务，一个用于监听延迟任务触发器
	pendingWatcher := clientv3.NewWatcher(client)
	delayedWatcher := clientv3.NewWatcher(client)
	workerWatcher := clientv3.NewWatcher(client)
	defer pendingWatcher.Close()
	defer delayedWatcher.Close()
	defer workerWatcher.Close()

	// 监听待处理任务
	pendingWatchChan := pendingWatcher.Watch(ctx, common.PendingTasksKey, clientv3.WithPrefix())
	// 监听延迟任务触发器的删除事件（TTL过期）
	delayedWatchChan := delayedWatcher.Watch(ctx, common.DelayedTriggerKey, clientv3.WithPrefix())
	// 监听Worker状态变化
	workerWatchChan := workerWatcher.Watch(ctx, common.WorkersKey, clientv3.WithPrefix())

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
					assignTask(ctx, client, task, common.Strategy)
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
					task.Status = common.PENDING
					task.CreateTime = time.Now()
					taskData, _ := json.Marshal(task)

					// 构建事务
					txn := client.Txn(ctx)
					txnResp, err := txn.
						If(clientv3.Compare(clientv3.CreateRevision(delayKey), ">", 0)).
						Then(
							clientv3.OpDelete(delayKey),
							clientv3.OpPut(common.PendingTasksKey+taskID, string(taskData)),
						).Commit()

					if err != nil {
						log.Printf("延迟任务事务处理失败: %v", err)
						continue
					}

					if !txnResp.Succeeded {
						log.Printf("延迟任务 %s 可能已被其他进程处理", task.ID)
						continue
					}

					log.Printf("延迟任务 %s 触发，重新提交到待处理队列", task.ID)
				}
			}
		}
	}()

	// 监听Worker状态变化，当Worker空闲下来时重新分配任务
	go func() {
		for resp := range workerWatchChan {
			for _, ev := range resp.Events {
				// 当Worker状态更新时（可能是任务完成后空闲下来）
				if ev.Type == clientv3.EventTypePut {
					// 检查是否有待处理任务
					pendingResp, err := client.Get(ctx, common.PendingTasksKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
					if err != nil {
						log.Printf("获取待处理任务数量失败: %v", err)
						continue
					}

					// 如果有待处理任务，尝试重新分配
					if pendingResp.Count > 0 {
						// 获取一批待处理任务并尝试分配
						batchSize := 10 // 每次处理的任务数量
						if pendingResp.Count < int64(batchSize) {
							batchSize = int(pendingResp.Count)
						}

						pendingTasksResp, err := client.Get(ctx, common.PendingTasksKey, clientv3.WithPrefix(), clientv3.WithLimit(int64(batchSize)))
						if err != nil {
							log.Printf("获取待处理任务失败: %v", err)
							continue
						}

						for _, kv := range pendingTasksResp.Kvs {
							var task model.Task
							if err := json.Unmarshal(kv.Value, &task); err != nil {
								log.Printf("解析任务失败: %v", err)
								continue
							}

							// 尝试分配任务
							assignTask(ctx, client, task, common.Strategy)
						}
					}
				}
			}
		}
	}()

	// 定期检查待处理任务，确保没有任务被遗漏
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processPendingTasks(ctx, client)
			}
		}
	}()

	// 等待上下文取消
	<-ctx.Done()
	log.Println("Task dispatcher stopped")
}

// processPendingTasks 处理已存在的待处理任务
func processPendingTasks(ctx context.Context, client *clientv3.Client) {
	log.Println("扫描并处理已存在的待处理任务...")

	// 获取所有待处理任务
	resp, err := client.Get(ctx, common.PendingTasksKey, clientv3.WithPrefix())
	if err != nil {
		log.Printf("获取待处理任务失败: %v", err)
		return
	}

	if len(resp.Kvs) == 0 {
		log.Println("没有发现待处理任务")
		return
	}

	log.Printf("发现 %d 个待处理任务，开始分配...", len(resp.Kvs))

	// 解析并分配任务
	for _, kv := range resp.Kvs {
		var task model.Task
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			log.Printf("解析任务失败: %v", err)
			continue
		}

		// 分配任务给可用的Worker
		assignTask(ctx, client, task, common.Strategy)
	}
}

// waitForWorkers 等待固定时间，然后检查是否有 Worker 节点注册
func waitForWorkers(ctx context.Context, client *clientv3.Client) {
	log.Println("等待 Worker 节点注册...")

	maxWaitTime := common.WorkerWaitTimeout
	delay := time.NewTimer(maxWaitTime)
	// 等待固定时间
	select {
	case <-ctx.Done():
		//case <-time.After(maxWaitTime):
		if !delay.Stop() {
			<-delay.C
		}
		log.Println("等待 Worker 节点被取消")
		return
	case <-delay.C:
	}

	// 等待结束后，检查是否有 Worker 节点
	resp, err := client.Get(ctx, common.WorkersKey, clientv3.WithPrefix())
	if err != nil {
		log.Printf("获取 Worker 列表失败: %v", err)
		return
	}

	// 根据 Worker 节点数量记录日志
	if len(resp.Kvs) > 0 {
		log.Printf("检测到 %d 个 Worker 节点已注册，开始分配任务", len(resp.Kvs))
	} else {
		log.Println("等待 Worker 节点超时，未检测到任何 Worker 节点")
	}
}

// assignTask 分配任务给可用的Worker
func assignTask(ctx context.Context, client *clientv3.Client, task model.Task, strategy string) {
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

	// 解析所有Worker
	workers := make([]Worker, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var worker Worker
		if err := json.Unmarshal(kv.Value, &worker); err != nil {
			continue
		}

		// 修复负数TaskCount
		if worker.TaskCount < 0 {
			worker.TaskCount = 0
		}

		// 检查Worker是否已达到容量上限
		if worker.Capacity > 0 && worker.TaskCount >= worker.Capacity {
			continue // 跳过已满负载的Worker
		}

		workers = append(workers, worker)
	}

	if len(workers) == 0 {
		log.Println("没有可用的Worker或所有Worker都已达到容量上限")
		return
	}

	// 根据策略选择Worker
	var selectedWorker *Worker
	switch strategy {
	case common.LBStrategyLeastTasks:
		// 最小负载策略：选择任务数最少的Worker
		selectedWorker = selectLeastTasksWorker(workers)
	case common.LBStrategyRoundRobin:
		// 轮询策略：按顺序选择Worker
		selectedWorker = selectRoundRobinWorker(workers)
	case common.LBStrategyRandom:
		// 随机策略：随机选择Worker
		selectedWorker = selectRandomWorker(workers)
	default:
		// 默认使用最小负载策略
		selectedWorker = selectLeastTasksWorker(workers)
	}

	// 将任务分配给选中的Worker
	task.Status = common.PROCESSING
	taskData, _ := json.Marshal(task)

	// 更新Worker的任务计数
	//selectedWorker.TaskCount++
	//workerData, _ := json.Marshal(selectedWorker)

	// 使用事务确保操作的原子性
	processingKey := common.ProcessingKey + selectedWorker.ID + "/" + task.ID
	pendingKey := common.PendingTasksKey + task.ID
	//workerKey := common.WorkersKey + selectedWorker.ID

	// 构建事务
	txn := client.Txn(ctx)
	txnResp, err := txn.
		If(clientv3.Compare(clientv3.Version(pendingKey), ">", 0)).
		Then(
			clientv3.OpPut(processingKey, string(taskData)),
			clientv3.OpDelete(pendingKey),
			//clientv3.OpPut(workerKey, string(workerData)),
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

	// 使用Worker的updateTaskCount方法原子性地更新任务计数
	workerObj := &Worker{ID: selectedWorker.ID, Lease: selectedWorker.Lease}
	workerObj.updateTaskCount(ctx, client, 1)

	log.Printf("任务 %s 已分配给Worker %s (策略: %s)", task.ID, selectedWorker.ID, strategy)
}

// selectLeastTasksWorker 选择任务数最少的Worker
func selectLeastTasksWorker(workers []Worker) *Worker {
	if len(workers) == 0 {
		return nil
	}

	minTasks := int(^uint(0) >> 1) // 最大整数
	selectedIndex := -1

	for i, worker := range workers {
		if worker.TaskCount < minTasks {
			selectedIndex = i
			minTasks = worker.TaskCount
		}
	}

	if selectedIndex == -1 {
		return nil
	}

	return &workers[selectedIndex]
}

// selectRoundRobinWorker 轮询选择Worker
func selectRoundRobinWorker(workers []Worker) *Worker {
	if len(workers) == 0 {
		return nil
	}

	// 更新全局索引
	lastWorkerIndex = (lastWorkerIndex + 1) % len(workers)
	return &workers[lastWorkerIndex]
}

// selectRandomWorker 随机选择Worker
func selectRandomWorker(workers []Worker) *Worker {
	if len(workers) == 0 {
		return nil
	}

	// 随机选择一个Worker
	index := rand.Intn(len(workers))
	return &workers[index]
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
