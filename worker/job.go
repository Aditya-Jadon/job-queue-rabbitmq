package main

import "time"

type Job struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Type           string    `json:"type"`
	Payload        string    `json:"payload"`
	CreatedAt      time.Time `json:"created_at"`
	RetryCount     int       `json:"retry_count"`
}
