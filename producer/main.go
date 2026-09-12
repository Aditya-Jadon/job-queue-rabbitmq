package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("jobs", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	count := 200
	if v := os.Getenv("JOB_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			count = n
		}
	}

	start := time.Now()

	for i := 0; i < count; i++ {
		job := Job{
			ID:             uuid.NewString(),
			IdempotencyKey: uuid.NewString(),
			Type:           "example_job",
			Payload:        fmt.Sprintf("load test job #%d", i),
			CreatedAt:      time.Now(),
		}

		body, err := json.Marshal(job)
		if err != nil {
			log.Printf("Failed to marshal job %d: %v", i, err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		})
		cancel()
		if err != nil {
			log.Printf("Failed to publish job %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("Published %d jobs in %v (%.1f jobs/sec)\n", count, elapsed, float64(count)/elapsed.Seconds())
}

func connectRabbitMQ(url string, maxAttempts int) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		log.Printf("RabbitMQ connection attempt %d/%d failed: %v", attempt, maxAttempts, err)
		time.Sleep(2 * time.Second)
	}

	return nil, fmt.Errorf("failed to connect to RabbitMQ after %d attempts: %w", maxAttempts, err)
}
