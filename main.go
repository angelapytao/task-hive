package main

import (
	"context"
	"flag"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"task-hive/common"
	"task-hive/tasks"
	"time"
)

func main() {
	// 添加角色参数
	role := flag.String("role", "all", "Node role: worker, dispatcher, monitor, or all")

	workerCount := flag.Int("workerCount", 3, "Number of workers")
	flag.Parse()

	// 初始化etcd客户端
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{common.EtcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 根据角色启动相应服务
	switch *role {
	case "worker":
		// 只启动Worker
		ctx := context.Background()
		for i := 0; i < *workerCount; i++ {
			worker := tasks.RegisterWorker(ctx, client)
			go worker.ProcessTasks(client)
			log.Printf("Started worker %d\n", i+1)
		}
	case "dispatcher":
		// 只启动任务派发器
		log.Println("Starting dispatcher...")
		go tasks.StartDispatcher(client)
	case "monitor":
		// 只启动监控器
		log.Println("Starting worker monitor...")
		go tasks.StartWorkerMonitor(client)
	case "all":
		// 启动所有服务
		// 启动故障监控
		go tasks.StartWorkerMonitor(client)
		// 启动任务派发器
		go tasks.StartDispatcher(client)
		// 启动Worker
		ctx := context.Background()
		for i := 0; i < *workerCount; i++ {
			worker := tasks.RegisterWorker(ctx, client)
			go worker.ProcessTasks(client)
			log.Printf("Started worker %d\n", i+1)
		}
		go tasks.StartWechatTask(client)
	default:
		log.Fatalf("Unknown role: %s", *role)
	}
	select {} // 保持主线程运行
}
