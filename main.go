package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"notification-system/internal/model"
	"notification-system/internal/queue"
	"notification-system/internal/worker"
)

func main() {
	// 1. Initialize Queue
	// Use Redis List as the queue implementation
	// Ensure you have a running Redis instance at localhost:6379
	redisAddr := "localhost:6379"
	redisPassword := "" // no password set
	redisDB := 0        // use default DB
	queueKey := "notification_queue"

	log.Printf("Initializing Redis Queue (Addr: %s, Key: %s)...", redisAddr, queueKey)
	q := queue.NewRedisQueue(redisAddr, redisPassword, redisDB, queueKey)

	// 2. Initialize and Start Dispatcher
	// We start 5 concurrent workers to process notifications.
	d := worker.NewDispatcher(5, q)
	d.Run()

	// 3. Define HTTP Handler
	http.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
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
		if err := q.Enqueue(task); err != nil {
			// If internal queue is full, return 503 to backpressure the caller
			http.Error(w, "System busy, please try again later", http.StatusServiceUnavailable)
			return
		}

		// Return 202 Accepted immediately
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status":"accepted", "task_id":"` + task.ID + `"}`))
	})

	// 4. Start Server
	log.Println("Notification System MVP started on :8080")
	log.Println("Endpoint: POST http://localhost:8080/notify")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
