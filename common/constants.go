package common

import "time"

// excel表相关常量
const (
	SpiderExcelFile = "spider.xlsx"

	SpiderExcelTypeWeb         = "web"
	SpiderExcelTypeWeibo       = "weibo"
	SpiderExcelTypeTencentNews = "news-tencent"
	SpiderExcelTypeWechat      = "wechat"

	SpiderDriveTypeSelenium = "selenium"
	SpiderDriveTypeRequest  = "request"
)

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
