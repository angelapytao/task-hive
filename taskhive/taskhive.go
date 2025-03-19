package taskhive

import (
	"context"
	"gitlab.ituchong.com/tc-common/common-task-hive/common"
	"gitlab.ituchong.com/tc-common/common-task-hive/model"
	"gitlab.ituchong.com/tc-common/common-task-hive/tasks"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"sync"
	"time"
)

// TaskHive 是分布式任务调度系统的主要结构
type TaskHive struct {
	client          *clientv3.Client
	ctx             context.Context
	cancel          context.CancelFunc
	wg              *sync.WaitGroup
	hostname        string
	dispatcherElect *tasks.LeaderElection
	monitorElect    *tasks.LeaderElection
	workers         []*tasks.Worker
}

// Config 配置选项
type Config struct {
	EtcdEndpoints []string
	WorkerCount   int
	DialTimeout   time.Duration
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		EtcdEndpoints: []string{common.EtcdEndpoints},
		WorkerCount:   1,
		DialTimeout:   5 * time.Second,
	}
}

// New 创建新的TaskHive实例
func New(config *Config) (*TaskHive, error) {
	if config == nil {
		config = DefaultConfig()
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   config.EtcdEndpoints,
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Get hostname: %v, Use RandomId\n", err)
		hostname = common.GenerateRandomID()
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	return &TaskHive{
		client:   client,
		ctx:      ctx,
		cancel:   cancel,
		wg:       wg,
		hostname: hostname,
		workers:  make([]*tasks.Worker, 0, config.WorkerCount),
	}, nil
}

// Start 启动TaskHive
func (th *TaskHive) Start() error {
	// 创建dispatcher领导者选举
	dispatcherElection, err := tasks.NewLeaderElection(th.client, common.RoleDispatcher, th.hostname, func() {
		th.wg.Add(1)
		go func() {
			defer th.wg.Done()
			tasks.StartDispatcher(th.client, th.ctx)
		}()
	})
	if err != nil {
		return err
	}
	th.dispatcherElect = dispatcherElection

	// 创建monitor领导者选举
	monitorElection, err := tasks.NewLeaderElection(th.client, common.RoleMonitor, th.hostname, func() {
		th.wg.Add(1)
		go func() {
			defer th.wg.Done()
			tasks.StartWorkerMonitor(th.client, th.ctx)
		}()
	})
	if err != nil {
		return err
	}
	th.monitorElect = monitorElection

	// 启动选举
	th.dispatcherElect.Start(th.ctx)
	th.monitorElect.Start(th.ctx)

	// 启动Worker
	config := DefaultConfig()
	for i := 0; i < config.WorkerCount; i++ {
		th.wg.Add(1)
		go func(index int) {
			defer th.wg.Done()
			worker := tasks.RegisterWorker(th.ctx, th.client)
			th.workers = append(th.workers, worker)
			go worker.ProcessTasks(th.client)
			log.Printf("Started worker %d on %s\n", index+1, th.hostname)
		}(i)
	}

	return nil
}

// Stop 停止TaskHive
func (th *TaskHive) Stop() {
	log.Println("正在关闭TaskHive服务...")
	th.cancel()

	// 添加超时机制
	waitCh := make(chan struct{})
	go func() {
		th.wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		log.Println("所有TaskHive服务已经关闭")
	case <-time.After(time.Second * 10):
		log.Println("TaskHive服务关闭超时，强制退出")
	}

	if th.dispatcherElect != nil {
		th.dispatcherElect.Close()
	}
	if th.monitorElect != nil {
		th.monitorElect.Close()
	}
	if th.client != nil {
		th.client.Close()
	}
}

// SubmitTask 提交任务
func (th *TaskHive) SubmitTask(task model.Task) error {
	return tasks.SubmitTask(th.client, task)
}

// RegisterTaskProcessor 注册任务处理器
func (th *TaskHive) RegisterTaskProcessor(taskType string, processor func(task *model.Task) (string, error)) {
	tasks.RegisterTaskProcessor(taskType, processor)
}
