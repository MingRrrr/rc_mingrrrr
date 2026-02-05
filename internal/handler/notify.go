package handler

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"notification-system/internal/model"
	"notification-system/internal/queue"
)

type NotifyHandler struct {
	queue queue.Queue
}

func NewNotifyHandler(q queue.Queue) *NotifyHandler {
	return &NotifyHandler{
		queue: q,
	}
}

func (h *NotifyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req model.NotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Basic Validation
	if req.TargetURL == "" {
		http.Error(w, "target_url is required", http.StatusBadRequest)
		return
	}

	// Create Task
	// In production, ID should be a UUID.
	task := &model.Task{
		ID:         fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(10000)),
		Request:    req,
		MaxRetries: 3, // Default retry policy: 3 times
		CreatedAt:  time.Now(),
	}

	// Enqueue Task
	if err := h.queue.Enqueue(task); err != nil {
		// If internal queue is full, return 503 to backpressure the caller
		http.Error(w, "System busy, please try again later", http.StatusServiceUnavailable)
		return
	}

	// Return 202 Accepted immediately
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"accepted", "task_id":"` + task.ID + `"}`))
}
