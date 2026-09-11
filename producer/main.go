package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := connectRabbitMQ("amqp://guest:guest@rabbitmq:5672/", 10)
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"jobs", // queue name
		true,   // durable
		false,  // auto-delete
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	job := Job{
		ID:             uuid.NewString(),
		IdempotencyKey: uuid.NewString(),
		Type:           "slow_job",
		Payload:        "chaos test job",
		CreatedAt:      time.Now(),
	}

	body, err := json.Marshal(job)
	if err != nil {
		log.Fatalf("Failed to marshal job: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",     // exchange (default/empty routes by queue name directly)
		q.Name, // routing key = queue name, since we're using the default exchange
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		log.Fatalf("Failed to publish job: %v", err)
	}

	fmt.Printf("Published job: %s\n", job.ID)
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
