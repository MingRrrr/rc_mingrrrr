package worker

import (
	"bytes"
	"log"
	"net/http"
	"notification-system/internal/model"
	"notification-system/internal/queue"
	"time"
)

type Dispatcher struct {
	WorkerPoolSize int
	Queue          queue.Queue
	Client         *http.Client
}

func NewDispatcher(poolSize int, q queue.Queue) *Dispatcher {
	return &Dispatcher{
		WorkerPoolSize: poolSize,
		Queue:          q,
		Client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.WorkerPoolSize; i++ {
		go d.worker(i)
	}
	log.Printf("Dispatcher started with %d workers", d.WorkerPoolSize)
}

func (d *Dispatcher) worker(id int) {
	for {
		task := d.Queue.Dequeue()
		d.process(id, task)
	}
}

func (d *Dispatcher) process(workerID int, task *model.Task) {
	reqData := task.Request
	log.Printf("[Worker %d] Processing task %s: %s %s (Retry: %d)", 
		workerID, task.ID, reqData.Method, reqData.TargetURL, task.RetryCount)

	// Create Request
	req, err := http.NewRequest(reqData.Method, reqData.TargetURL, bytes.NewBufferString(reqData.Body))
	if err != nil {
		log.Printf("[Worker %d] Failed to create request for %s: %v", workerID, task.ID, err)
		return // Fatal error, do not retry
	}

	for k, v := range reqData.Headers {
		req.Header.Set(k, v)
	}

	// Execute
	resp, err := d.Client.Do(req)
	
	success := false
	if err == nil {
		defer resp.Body.Close()
		// We consider 2xx as success. 
		// Depending on requirements, 4xx might be fatal (no retry) or retryable.
		// For this MVP, we consider 200-299 as success.
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			success = true
			log.Printf("[Worker %d] Task %s Success: Status %d", workerID, task.ID, resp.StatusCode)
		} else {
			log.Printf("[Worker %d] Task %s Failed: Status %d", workerID, task.ID, resp.StatusCode)
		}
	} else {
		log.Printf("[Worker %d] Task %s Network Error: %v", workerID, task.ID, err)
	}

	if !success {
		d.handleFailure(task)
	}
}

func (d *Dispatcher) handleFailure(task *model.Task) {
	if task.RetryCount >= task.MaxRetries {
		log.Printf("Task %s reached max retries (%d). Moved to DLQ (Logged).", task.ID, task.MaxRetries)
		// In a real system, verify write to DLQ here.
		return
	}

	task.RetryCount++
	// Exponential Backoff: 1s, 2s, 4s, 8s...
	backoff := time.Duration(1<<task.RetryCount) * time.Second
	task.NextRetryAt = time.Now().Add(backoff)

	log.Printf("Task %s will retry in %v (Attempt %d/%d)", task.ID, backoff, task.RetryCount, task.MaxRetries)

	// Asynchronous wait to avoid blocking the worker thread
	// In production, we would push to a specialized DelayedQueue (e.g., Redis ZSET)
	// Here we use a goroutine to simulate it.
	go func() {
		time.Sleep(backoff)
		d.Queue.Enqueue(task)
	}()
}
