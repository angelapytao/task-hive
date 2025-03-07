package main

import (
	"context"
	"flag"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"os/signal"
	"syscall"
	"task-hive/common"
	"task-hive/tasks"
	"time"
)

func main() {
	workerCount := flag.Int("workerCount", 1, "Number of workers")
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
	// get hostname
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Get hostname: %v, Use RandomId\n", err)
		hostname = common.GenerateRandomID()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建dispatcher领导者选举
	dispatcherElection, err := tasks.NewLeaderElection(client, "dispatcher", hostname, func() {
		tasks.StartDispatcher(client)
		// 启动微信任务
		tasks.StartSpiderTask(client)
	})
	if err != nil {
		log.Fatalf("创建dispatcher选举失败: %v", err)
	}
	defer dispatcherElection.Close()

	// 创建monitor领导者选举
	monitorElection, err := tasks.NewLeaderElection(client, "monitor", hostname, func() {
		tasks.StartWorkerMonitor(client)
	})
	if err != nil {
		log.Fatal("创建monitor选举失败: %v", err)
	}
	defer monitorElection.Close()

	dispatcherElection.Start(ctx)
	monitorElection.Start(ctx)

	// 所有实例都启动 Worker
	for i := 0; i < *workerCount; i++ {
		worker := tasks.RegisterWorker(ctx, client)
		go worker.ProcessTasks(client)
		log.Printf("Started worker %d on %s\n", i+1, hostname)
	}

	// 处理信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("正在关闭服务...")
	cancel()
	time.Sleep(2 * time.Second) // 给goroutines一些时间来清理
}
