# Task-Hive

Task-Hive是一个基于etcd的分布式任务调度系统，具有高可用性、可扩展性和容错能力。

## 功能特点

- 分布式任务调度
- 自动故障转移
- 动态扩缩容
- 领导者选举
- 任务重试机制

## 快速开始

### 启动etcd

```bash
docker run -d \
  -p 2379:2379 -p 2380:2380 \
  --name etcd1 \
  quay.io/coreos/etcd:v3.5.0 \
  etcd \
  --name etcd0 \
  --data-dir /etcd-data \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://172.17.0.3:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://172.17.0.3:2380 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster "etcd0=http://172.17.0.3:2380" \
  --initial-cluster-state new