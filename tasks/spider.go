package tasks

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"task-hive/model"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tealeg/xlsx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	SpiderExcelFile = "../spider.xlsx"
)

func StartSpiderTask(client *clientv3.Client) {
	logrus.Info("StartSpiderTask start...")
	// 获取当前文件所在目录
	_, filename, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(filename))
	excelPath := filepath.Join(projectRoot, "spider.xlsx")

	// 打印路径信息
	logrus.Infof("Excel文件路径: %s", excelPath)

	// 读取excel任务数据
	xlFile, err := xlsx.OpenFile(excelPath)
	if err != nil {
		logrus.Errorf("startWechatTask Error opening Excel file: %v", err)
		return
	}

	if len(xlFile.Sheets) == 0 {
		logrus.Error("Excel file has no sheets")
		return
	}

	sheet := xlFile.Sheets[0]
	// 跳过表头
	for rowIndex, row := range sheet.Rows[1:] {
		// 将每行数据转换为任务
		rowData := make([]string, len(row.Cells))
		for i, cell := range row.Cells {
			rowData[i] = cell.String()
		}

		wechatTask := &model.WechatTask{
			RowID:   rowIndex + 1,
			RowData: rowData,
		}

		// 创建分布式任务
		task := model.Task{
			ID:         fmt.Sprintf("wechat-task-%d-%d", time.Now().UnixNano(), rowIndex),
			Payload:    wechatTask.ToTaskPayload(),
			Priority:   5,
			CreateTime: time.Now(),
			Type:       "spider",
			//ExecuteFunc: ProcessSpiderTask,
		}

		// 提交任务到分布式系统
		if err := SubmitTask(client, task); err != nil {
			logrus.Errorf("Failed to submit task for row %d: %v", rowIndex+1, err)
			continue
		}

		logrus.Infof("Submitted task for row %d", rowIndex+1)
	}
}

// ProcessSpiderTask 实现任务处理逻辑
func ProcessSpiderTask(task *model.Task) (string, error) {
	var wechatTask model.WechatTask
	if err := json.Unmarshal([]byte(task.Payload), &wechatTask); err != nil {
		return "", fmt.Errorf("invalid task payload: %v", err)
	}

	// 在这里实现具体的任务处理逻辑
	logrus.Infof("Processing row %d: %v", wechatTask.RowID, wechatTask.RowData)

	// TODO: 实现实际的任务处理逻辑
	time.Sleep(time.Second * 2) // 模拟处理时间

	return fmt.Sprintf("Row %d processed successfully", wechatTask.RowID), nil
}
