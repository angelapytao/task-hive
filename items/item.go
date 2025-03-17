package items

import (
	"time"
)

type ItemData struct {
	TaskId     string   `json:"taskId"`
	RootNode   string   `json:"rootNode"`
	Page       string   `json:"page"`
	Images     []string `json:"images"`
	CreateTime string   `json:"createTime"`
	Company    string   `json:"company"`
}

type CrawlCountData struct {
	Id            int       `gorm:"column:id;primaryKey;autoIncrement"`
	CrawlTime     time.Time `gorm:"column:crawl_time;not null"`                // 抓取时间，非空
	TaskID        int       `gorm:"column:task_id;not null"`                   // 任务ID，非空
	Domain        string    `gorm:"column:domain;type:varchar(255);not null"`  // 域名，非空
	CrawlCount    int32     `gorm:"column:crawl_count;not null"`               // 抓取量，非空
	CrawlCountNew int32     `gorm:"column:crawl_count_new;default:0;not null"` // 实际抓取量，默认值为0，非空
	Extra         []byte    `gorm:"column:extra;type:json"`                    // 额外信息，JSON类型，可为空
}

//func (c CrawlCountData) TableName() string {
//	return "crawl_count_data"
//}
//
//// InsterCrawlCountData 插入爬虫计数数据
//func InsterCrawlCountData(record *CrawlCountData) error {
//	return common.DB.Create(record).Error
//}
//
//// CheckCrawkCountRecord 检查爬虫计数数据是否存在
//func CheckCrawkCountRecord(taskId int, start, end string) (bool, error) {
//	var count int64
//	return count > 0, common.DB.Model(&CrawlCountData{}).Where("task_id", taskId).
//		Where("crawl_time >= ? AND crawl_time < ?", start, end).Count(&count).Error
//}
//
//// UpsertCrawlCountData 插入或更新爬虫计数数据
//func UpsertCrawlCountData(record *CrawlCountData) error {
//	result := common.DB.Where(CrawlCountData{TaskID: record.TaskID, CrawlTime: record.CrawlTime}).
//		Assign(CrawlCountData{CrawlCountNew: record.CrawlCountNew,
//			CrawlCount: record.CrawlCount}).
//		FirstOrCreate(record)
//	return result.Error
//}
//
//// GetCrawlDataByFilter 根据传入的 CrawlCountData 对象进行筛选查询
//func GetCrawlDataByFilter(filter *CrawlCountData) (CrawlCountData, error) {
//	var record CrawlCountData
//	err := common.DB.Where(filter).First(&record).Error
//	if err != nil {
//		return record, err
//	}
//	return record, nil
//}
//
//// IsHasSSTaskRecord 检查是否有数说任务存在
//func IsHasSSTaskRecord(jobId string) (bool, error) {
//	var exists bool
//	err := common.DB.Raw(
//		"SELECT EXISTS (SELECT 1 FROM crawl_count_data WHERE JSON_EXTRACT(extra,\"$.ss_job_id\") = ? LIMIT 1)",
//		jobId).Scan(&exists).Error
//	if err != nil {
//		return false, err
//	}
//	return exists, nil
//}
//
//func CheckDoneTask(taskId string) bool {
//	id, _ := strconv.Atoi(taskId)
//	ok, _ := CheckCrawkCountRecord(id, carbon.Now().ToDateString(), carbon.Tomorrow().ToDateString())
//	return ok
//}

type Task struct {
	TaskId    string
	Limit     int    //单个任务爬取图片的上限
	RootNodes string //任务自定义数据
	NodeType  string
	Driver    string
	Company   string
	Domain    string
	Root      string
}
