package queue

import (
	"context"
	"encoding/json"
	"log"
	"notification-system/internal/model"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue defines the interface for our task queue
// In production, this would be implemented by Redis, RabbitMQ, or Kafka drivers
type Queue interface {
	Enqueue(task *model.Task) error
	Dequeue() *model.Task // Blocking call
}

// MemoryQueue is a simple channel-based queue for MVP
type MemoryQueue struct {
	ch chan *model.Task
}

func NewMemoryQueue(size int) *MemoryQueue {
	return &MemoryQueue{
		ch: make(chan *model.Task, size),
	}
}

func (q *MemoryQueue) Enqueue(task *model.Task) error {
	q.ch <- task
	return nil
}

func (q *MemoryQueue) Dequeue() *model.Task {
	return <-q.ch
}

// RedisQueue implementation using Redis List
type RedisQueue struct {
	client *redis.Client
	key    string
}

func NewRedisQueue(addr string, password string, db int, key string) *RedisQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Try to ping to ensure connection, but don't fail fataly to allow retry
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Failed to connect to Redis at %s: %v", addr, err)
	}

	return &RedisQueue{
		client: rdb,
		key:    key,
	}
}

func (q *RedisQueue) Enqueue(task *model.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	// Use LPUSH to add to the head
	return q.client.LPush(context.Background(), q.key, data).Err()
}

func (q *RedisQueue) Dequeue() *model.Task {
	for {
		// Use BRPOP to remove from the tail (Blocking Pop)
		// 0 means block indefinitely
		result, err := q.client.BRPop(context.Background(), 0, q.key).Result()
		if err != nil {
			log.Printf("Redis Dequeue Error: %v. Retrying in 1s...", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// result is a slice: [key, value]
		if len(result) < 2 {
			continue
		}

		var task model.Task
		if err := json.Unmarshal([]byte(result[1]), &task); err != nil {
			log.Printf("Failed to unmarshal task from Redis: %v. Raw: %s", err, result[1])
			// Skip bad data
			continue
		}

		return &task
	}
}
