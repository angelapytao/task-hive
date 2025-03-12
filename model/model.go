package model

import (
	"encoding/json"
	"time"
)

// Task 表示任务结构
type Task struct {
	ID         string    `json:"id"`
	Payload    string    `json:"payload"`
	Priority   int       `json:"priority"` // 任务优先级 1-10
	Status     string    `json:"status"`   // pending, processing, completed, failed
	CreateTime time.Time `json:"createTime"`
	RetryCount int       `json:"retryCount"`
	Result     string    `json:"result"`
	Error      string    `json:"error"`
	Type       string    `json:"type"`
	//ExecuteFunc func(task *Task) (string, error) `json:"-"` // 使用json:"-"避免序列化函数
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
