package main

import (
	"context"
	"flag"
	"gitlab.ituchong.com/tc-common/common-task-hive/common"
	"gitlab.ituchong.com/tc-common/common-task-hive/tasks"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	workerCount := flag.Int("workerCount", 10, "Number of workers")
	flag.Parse()

	// 初始化etcd客户端
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{common.EtcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func(client *clientv3.Client) {
		err := client.Close()
		if err != nil {
			log.Printf("close etcd client err: %v", err)
		}
	}(client)

	// get hostname
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Get hostname: %v, Use RandomId\n", err)
		hostname = common.GenerateRandomID()
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建dispatcher领导者选举
	dispatcherElection, err := tasks.NewLeaderElection(client, common.RoleDispatcher, hostname, func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tasks.StartDispatcher(client)
			// 启动微信任务
			//tasks.StartSpiderTask(client)
		}()
	})
	if err != nil {
		log.Fatalf("创建dispatcher选举失败: %v \n", err)
	}
	defer dispatcherElection.Close()

	// 创建monitor领导者选举
	monitorElection, err := tasks.NewLeaderElection(client, common.RoleMonitor, hostname, func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tasks.StartWorkerMonitor(client)
		}()
	})
	if err != nil {
		log.Fatalf("创建monitor选举失败: %v \n", err)
	}
	defer monitorElection.Close()

	dispatcherElection.Start(ctx)
	monitorElection.Start(ctx)

	// 所有实例都启动 Worker
	for i := 0; i < *workerCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			worker := tasks.RegisterWorker(ctx, client)
			go worker.ProcessTasks(client)
			log.Printf("Started worker %d on %s\n", index+1, hostname)
		}(i)
	}

	// 处理信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)
	<-sigChan

	log.Println("正在关闭服务...")
	cancel()

	wg.Wait()
	log.Println("所有服务已经关闭")
}
