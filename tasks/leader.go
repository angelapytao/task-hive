package tasks

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"log"
	"task-hive/common"
	"time"
)

// LeaderElection 领导者选举管理
type LeaderElection struct {
	client     *clientv3.Client
	session    *concurrency.Session
	election   *concurrency.Election
	role       string
	id         string
	leaderFunc func()
	isLeader   bool
}

// NewLeaderElection 创建新的领导者选举
func NewLeaderElection(client *clientv3.Client, role, id string, leaderFunc func()) (*LeaderElection, error) {
	session, err := concurrency.NewSession(client, concurrency.WithTTL(15))
	if err != nil {
		return nil, err
	}

	election := concurrency.NewElection(session, common.LeaderKey+role)

	return &LeaderElection{
		client:     client,
		session:    session,
		election:   election,
		role:       role,
		id:         id,
		leaderFunc: leaderFunc,
		isLeader:   false,
	}, nil
}

// Start 开始领导者选举
func (le *LeaderElection) Start(ctx context.Context) {
	go func() {
		for {
			// 尝试竞选
			if err := le.election.Campaign(ctx, le.id); err != nil {
				log.Printf("竞选%s领导者失败: %v", le.role, err)
				time.Sleep(5 * time.Second)
				continue
			}

			log.Printf("成功当选为%s领导者", le.role)
			le.isLeader = true

			// 执行领导者功能
			log.Printf("执行领导者功能")
			//go le.leaderFunc()
			le.leaderFunc()

			// 监听领导权变化
			observeChan := le.election.Observe(ctx)
			for resp := range observeChan {
				if string(resp.Kvs[0].Value) != le.id {
					log.Printf("失去%s领导权", le.role)
					le.isLeader = false
					break
				}
			}

			log.Printf("重新竞选%s领导者...", le.role)
			le.isLeader = false
		}
	}()
}

// IsLeader 检查是否是领导者
func (le *LeaderElection) IsLeader() bool {
	return le.isLeader
}

// Close 关闭领导者选举
func (le *LeaderElection) Close() {
	if le.session != nil {
		le.session.Close()
	}
}
