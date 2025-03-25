package api

import (
	"encoding/json"
	"gitlab.ituchong.com/tc-common/common-task-hive/tasks"
	"go.etcd.io/etcd/client/v3"
	"log"
	"net/http"
	"strconv"
)

type Server struct {
	client *clientv3.Client
}

func NewServer(client *clientv3.Client) *Server {
	return &Server{client: client}
}

func (s *Server) Start(addr string) error {
	// 注册路由
	http.HandleFunc("/api/tasks", s.handleListTasks)
	http.HandleFunc("/api/tasks/", s.handleGetTask)
	http.HandleFunc("/api/stats", s.handleGetStats)
	http.HandleFunc("/api/workers", s.handleListWorkers)
	http.HandleFunc("/api/workers/", s.handleGetWorkerTasks)

	log.Printf("API服务器启动在 %s", addr)
	return http.ListenAndServe(addr, nil)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	limitStr := r.URL.Query().Get("limit")

	limit := 100 // 默认限制
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	tasks, err := tasks.ListTasks(s.client, status, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Path[len("/api/tasks/"):]
	if taskID == "" {
		http.Error(w, "任务ID不能为空", http.StatusBadRequest)
		return
	}

	task, err := tasks.GetTaskByID(s.client, taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := tasks.GetTaskStats(s.client)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := tasks.ListWorkers(s.client)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workers)
}

func (s *Server) handleGetWorkerTasks(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Path[len("/api/workers/"):]
	if workerID == "" {
		http.Error(w, "Worker ID不能为空", http.StatusBadRequest)
		return
	}

	tasks, err := tasks.GetWorkerTasks(s.client, workerID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}
