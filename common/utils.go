package common

import (
	"math/rand"
	"strings"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func init() {
	rand.Seed(time.Now().UnixNano())
}

// GenerateRandomID 生成随机ID
func GenerateRandomID() string {
	b := make([]byte, 10)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// SplitProcessingKey 解析处理中任务的键，返回 [workerID, taskID]
func SplitProcessingKey(key string) []string {
	// 移除前缀
	key = strings.TrimPrefix(key, ProcessingKey)
	// 按 "/" 分割
	parts := strings.Split(key, "/")
	return parts
}
