package common

import "time"

const (
	EtcdEndpoints     = "localhost:2379"
	PendingTasksKey   = "/tasks/pending/"
	ProcessingKey     = "/tasks/processing/"
	CompletedKey      = "/tasks/completed/"
	FailedKey         = "/tasks/failed/"
	DelayedKey        = "/tasks/delayed/"
	DelayedTriggerKey = "/tasks/delayed_trigger/"
	WorkersKey        = "/workers/"

	LeaderKey = "/task-hive/leader/"

	RoleDispatcher = "dispatcher"
	RoleMonitor    = "monitor"

	MaxRetries  = 3
	TaskTimeout = 5 * time.Minute
)

const (
	// 负载均衡策略
	LBStrategyLeastTasks = "least_tasks" // 最小负载策略
	LBStrategyRoundRobin = "round_robin" // 轮询策略
	LBStrategyRandom     = "random"      // 随机策略
)

var (
	// WorkerWaitTimeout 等待Worker节点注册的最大时间
	WorkerWaitTimeout = 120 * time.Second
	Strategy          = LBStrategyLeastTasks
)

type TaskStatus int

const (
	// PENDING TaskStatus 任务状态
	PENDING TaskStatus = iota
	PROCESSING
	COMPLETED
	FAILED
)
