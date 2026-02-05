package model

import "time"

// NotificationRequest defines the payload from internal business systems
type NotificationRequest struct {
	TargetURL string            `json:"target_url"`
	Method    string            `json:"method"` // GET, POST, etc.
	Headers   map[string]string `json:"headers"`
	Body      string            `json:"body"`
}

// Task wraps the request with retry metadata
type Task struct {
	ID          string
	Request     NotificationRequest
	RetryCount  int
	MaxRetries  int
	NextRetryAt time.Time
	CreatedAt   time.Time
}
