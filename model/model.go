package model

import (
	"encoding/json"
	"gitlab.ituchong.com/tc-common/common-task-hive/common"

	"time"
)

// Task 表示任务结构
type Task struct {
	ID         string            `json:"id"`
	Payload    string            `json:"payload"`
	Priority   int               `json:"priority"` // 任务优先级 1-10
	Status     common.TaskStatus `json:"status"`   // pending, processing, completed, failed
	CreateTime time.Time         `json:"createTime"`
	RetryCount int               `json:"retryCount"`
	RetryDelay time.Duration     `json:"retryDelay"`
	Result     string            `json:"result"`
	Error      string            `json:"error"`
	Type       string            `json:"type"`
}

// WechatTask 微信任务结构
type WechatTask struct {
	Task
	RowID   int      `json:"row_id"`   // Excel中的行号
	RowData []string `json:"row_data"` // Excel行数据
}

// ToTaskPayload 将WechatTask转换为Task的Payload
func (w *WechatTask) ToTaskPayload() string {
	payload, _ := json.Marshal(w)
	return string(payload)
}
