package main

import (
	"log"
	"net/http"

	"notification-system/internal/handler"
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
	notifyHandler := handler.NewNotifyHandler(q)
	http.Handle("/notify", notifyHandler)

	// 4. Start Server
	log.Println("Notification System MVP started on :8080")
	log.Println("Endpoint: POST http://localhost:8080/notify")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
