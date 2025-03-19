package main

import (
	"flag"
	"gitlab.ituchong.com/tc-common/common-task-hive/model"
	"gitlab.ituchong.com/tc-common/common-task-hive/taskhive"
	"log"
	"os"
	"os/signal"
	"syscall"
)

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
	th.RegisterTaskProcessor("custom", func(task *model.Task) (string, error) {
		// 处理自定义任务的逻辑
		log.Printf("处理自定义任务: %s", task.ID)
		return "自定义任务处理完成", nil
	})

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
