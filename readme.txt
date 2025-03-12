docker run -d \
  -p 2379:2379 -p 2380:2380 \
  --name etcd \
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

  go build -o task-hive .
