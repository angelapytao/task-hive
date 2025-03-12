package common

import "time"

const (
	EtcdEndpoints   = "localhost:2379"
	PendingTasksKey = "/tasks/pending/"
	ProcessingKey   = "/tasks/processing/"
	CompletedKey    = "/tasks/completed/"
	FailedKey       = "/tasks/failed/"
	WorkersKey      = "/workers/"

	LeaderKey = "/task-hive/leader/"

	RoleDispatcher = "dispatcher"
	RoleMonitor    = "monitor"

	MaxRetries  = 3
	TaskTimeout = 5 * time.Minute
)

type TaskStatus int

const (
	// PENDING TaskStatus 任务状态
	PENDING TaskStatus = iota
	PROCESSING
	COMPLETED
	FAILED
)
