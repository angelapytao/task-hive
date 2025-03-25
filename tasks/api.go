package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"gitlab.ituchong.com/tc-common/common-task-hive/common"
	"gitlab.ituchong.com/tc-common/common-task-hive/model"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

// TaskInfo 包含任务的详细信息和状态
type TaskInfo struct {
	Task       model.Task `json:"task"`
	Status     string     `json:"status"`
	WorkerID   string     `json:"worker_id,omitempty"`
	CreateTime time.Time  `json:"create_time"`
	UpdateTime time.Time  `json:"update_time,omitempty"`
}

// TaskStats 包含各状态任务的统计信息
type TaskStats struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
	Delayed    int `json:"delayed"`
	Total      int `json:"total"`
}

// WorkerInfo 包含Worker的详细信息
type WorkerInfo struct {
	ID            string    `json:"id"`
	TaskCount     int       `json:"task_count"`
	Capacity      int       `json:"capacity"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	IsActive      bool      `json:"is_active"`
}

// GetTaskByID 根据任务ID获取任务详情
func GetTaskByID(client *clientv3.Client, taskID string) (*TaskInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 检查所有可能的状态
	statusKeys := map[common.TaskStatus]string{
		common.PENDING:    common.PendingTasksKey + taskID,
		common.PROCESSING: common.ProcessingKey, // 需要前缀搜索
		common.COMPLETED:  common.CompletedKey + taskID,
		common.FAILED:     common.FailedKey + taskID,
		common.DELAYED:    common.DelayedKey + taskID,
	}

	// 先检查确切的键
	for status, key := range statusKeys {
		if status == common.PROCESSING {
			continue // 处理中的任务需要特殊处理
		}

		resp, err := client.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		if len(resp.Kvs) > 0 {
			var task model.Task
			if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
				return nil, err
			}

			return &TaskInfo{
				Task:       task,
				Status:     common.TaskStatusToString(status),
				CreateTime: task.CreateTime,
				UpdateTime: time.Now(), // 可以从etcd的修改时间获取
			}, nil
		}
	}

	// 检查处理中的任务（需要前缀搜索）
	resp, err := client.Get(ctx, common.ProcessingKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		// 检查键是否包含任务ID
		if len(key) > len(common.ProcessingKey) {
			parts := common.SplitProcessingKey(key)
			if len(parts) == 2 && parts[1] == taskID {
				var task model.Task
				if err := json.Unmarshal(kv.Value, &task); err != nil {
					return nil, err
				}

				return &TaskInfo{
					Task:       task,
					Status:     common.TaskStatusToString(common.PROCESSING),
					WorkerID:   parts[0],
					CreateTime: task.CreateTime,
					UpdateTime: time.Now(),
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("任务 %s 不存在", taskID)
}

// ListTasks 获取任务列表，可按状态筛选
func ListTasks(client *clientv3.Client, statusStr string, limit int) ([]TaskInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tasks []TaskInfo
	var prefix string

	// 将字符串状态转换为TaskStatus
	var status common.TaskStatus
	if statusStr != "" {
		status = common.StringToTaskStatus(statusStr)
	}

	// 根据状态确定前缀
	switch status {
	case common.PENDING:
		prefix = common.PendingTasksKey
	case common.PROCESSING:
		prefix = common.ProcessingKey
	case common.COMPLETED:
		prefix = common.CompletedKey
	case common.FAILED:
		prefix = common.FailedKey
	case common.DELAYED:
		prefix = common.DelayedKey
	case 99: // 空字符串转换后为 99
		// 获取所有状态的任务
		pendingTasks, _ := ListTasks(client, common.TaskStatusToString(common.PENDING), limit)
		processingTasks, _ := ListTasks(client, common.TaskStatusToString(common.PROCESSING), limit)
		completedTasks, _ := ListTasks(client, common.TaskStatusToString(common.COMPLETED), limit)
		failedTasks, _ := ListTasks(client, common.TaskStatusToString(common.FAILED), limit)
		delayedTasks, _ := ListTasks(client, common.TaskStatusToString(common.DELAYED), limit)

		// 合并结果
		tasks = append(tasks, pendingTasks...)
		tasks = append(tasks, processingTasks...)
		tasks = append(tasks, completedTasks...)
		tasks = append(tasks, failedTasks...)
		tasks = append(tasks, delayedTasks...)

		// 限制结果数量
		if limit > 0 && len(tasks) > limit {
			tasks = tasks[:limit]
		}

		return tasks, nil
	default:
		return nil, fmt.Errorf("无效的任务状态: %s", statusStr)
	}

	// 获取指定状态的任务
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(int64(limit)))
	if err != nil {
		return nil, err
	}

	for _, kv := range resp.Kvs {
		var task model.Task
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			log.Printf("解析任务失败: %v", err)
			continue
		}

		taskInfo := TaskInfo{
			Task:       task,
			Status:     common.TaskStatusToString(status),
			CreateTime: task.CreateTime,
			UpdateTime: time.Unix(0, kv.ModRevision), // 使用etcd的修改版本作为更新时间
		}

		// 如果是处理中的任务，提取WorkerID
		if status == common.PROCESSING {
			key := string(kv.Key)
			parts := common.SplitProcessingKey(key)
			if len(parts) == 2 {
				taskInfo.WorkerID = parts[0]
			}
		}

		tasks = append(tasks, taskInfo)
	}

	return tasks, nil
}

// GetTaskStats 获取任务统计信息
func GetTaskStats(client *clientv3.Client) (*TaskStats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats := &TaskStats{}

	// 获取待处理任务数量
	pendingResp, err := client.Get(ctx, common.PendingTasksKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err == nil {
		stats.Pending = int(pendingResp.Count)
	}

	// 获取处理中任务数量
	processingResp, err := client.Get(ctx, common.ProcessingKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err == nil {
		stats.Processing = int(processingResp.Count)
	}

	// 获取已完成任务数量
	completedResp, err := client.Get(ctx, common.CompletedKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err == nil {
		stats.Completed = int(completedResp.Count)
	}

	// 获取失败任务数量
	failedResp, err := client.Get(ctx, common.FailedKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err == nil {
		stats.Failed = int(failedResp.Count)
	}

	// 获取延迟任务数量
	delayedResp, err := client.Get(ctx, common.DelayedKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err == nil {
		stats.Delayed = int(delayedResp.Count)
	}

	// 计算总数
	stats.Total = stats.Pending + stats.Processing + stats.Completed + stats.Failed + stats.Delayed

	return stats, nil
}

// ListWorkers 获取所有Worker的状态
func ListWorkers(client *clientv3.Client) ([]WorkerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, common.WorkersKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var workers []WorkerInfo
	now := time.Now()

	for _, kv := range resp.Kvs {
		var worker Worker
		if err := json.Unmarshal(kv.Value, &worker); err != nil {
			log.Printf("解析Worker失败: %v", err)
			continue
		}

		// 检查Worker是否活跃（最后心跳时间在30秒内）
		isActive := now.Sub(worker.LastHeartbeat) < 30*time.Second

		workerInfo := WorkerInfo{
			ID:            worker.ID,
			TaskCount:     worker.TaskCount,
			Capacity:      worker.Capacity,
			LastHeartbeat: worker.LastHeartbeat,
			IsActive:      isActive,
		}

		workers = append(workers, workerInfo)
	}

	return workers, nil
}

// GetWorkerTasks 获取指定Worker正在处理的任务
func GetWorkerTasks(client *clientv3.Client, workerID string) ([]TaskInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prefix := common.ProcessingKey + workerID + "/"
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var tasks []TaskInfo
	for _, kv := range resp.Kvs {
		var task model.Task
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			log.Printf("解析任务失败: %v", err)
			continue
		}

		taskInfo := TaskInfo{
			Task:       task,
			Status:     common.TaskStatusToString(common.PROCESSING),
			WorkerID:   workerID,
			CreateTime: task.CreateTime,
			UpdateTime: time.Unix(0, kv.ModRevision),
		}

		tasks = append(tasks, taskInfo)
	}

	return tasks, nil
}
