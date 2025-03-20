package main

import (
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tealeg/xlsx"
	"gitlab.ituchong.com/tc-common/common-task-hive/model"
	"gitlab.ituchong.com/tc-common/common-task-hive/taskhive"
	"gitlab.ituchong.com/tc-common/common-task-hive/tasks"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// WechatTaskGenerator 微信任务生成器
type WechatTaskGenerator struct{}

// Name 返回任务生成器名称
func (w *WechatTaskGenerator) Name() string {
	return "微信任务生成器"
}

// GenerateTasks 生成微信相关任务
func (w *WechatTaskGenerator) GenerateTasks(client *clientv3.Client) error {
	log.Println("生成微信任务...")

	// 获取当前文件所在目录
	_, filename, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(filename))
	excelPath := filepath.Join(projectRoot, "./task.xlsx")

	// 打印路径信息
	logrus.Infof("Excel文件路径: %s", excelPath)

	// 读取excel任务数据
	xlFile, err := xlsx.OpenFile(excelPath)
	if err != nil {
		logrus.Errorf("startWechatTask Error opening Excel file: %v", err)
		return err
	}

	if len(xlFile.Sheets) == 0 {
		logrus.Error("Excel file has no sheets")
		return fmt.Errorf("Excel file has no sheets")
	}

	sheet := xlFile.Sheets[0]
	// 跳过表头
	var errors []error
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
			Type:       "custom",
		}

		// 提交任务到分布式系统
		if err := tasks.SubmitTask(client, task); err != nil {
			logrus.Errorf("Failed to submit task for row %d: %v", rowIndex+1, err)
			errors = append(errors, err)
			continue
		}

		logrus.Infof("Submitted task for row %d", rowIndex+1)
	}

	// 如果有错误，返回第一个错误
	if len(errors) > 0 {
		return fmt.Errorf("提交任务时发生错误: %v (总共 %d 个错误)", errors[0], len(errors))
	}

	return nil
}

func main() {
	workerCount := flag.Int("workerCount", 1, "Number of workers")
	flag.Parse()

	// 创建配置
	config := taskhive.DefaultConfig()
	config.WorkerCount = *workerCount

	// 创建TaskHive实例
	th, err := taskhive.New(config)
	if err != nil {
		log.Fatalf("创建TaskHive失败: %v", err)
	}

	// 注册自定义任务处理器
	th.RegisterTaskProcessor("custom", tasks.ProcessSpiderTask)

	th.RegisterTaskGenerator(&WechatTaskGenerator{})

	// 启动TaskHive
	if err := th.Start(); err != nil {
		log.Fatalf("启动TaskHive失败: %v", err)
	}
	defer th.Stop()

	// 处理信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)
	<-sigChan

	log.Println("收到退出信号，正在关闭...")
}
