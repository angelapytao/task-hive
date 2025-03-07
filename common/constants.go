package common

import "time"

const (
	EtcdEndpoints   = "localhost:2379"
	PendingTasksKey = "/tasks/pending/"
	ProcessingKey   = "/tasks/processing/"
	CompletedKey    = "/tasks/completed/"
	FailedKey       = "/tasks/failed/"
	WorkersKey      = "/workers/"

	MaxRetries  = 3
	TaskTimeout = 5 * time.Minute
)
