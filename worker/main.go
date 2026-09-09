package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const maxRetries = 3
const retryDelayMs = 10000

func main() {
	connectPostgres()

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

	jobsQ, err := ch.QueueDeclare("jobs", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare jobs queue: %v", err)
	}

	_, err = ch.QueueDeclare("jobs.retry", true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": "jobs",
		"x-message-ttl":             int32(retryDelayMs),
	})
	if err != nil {
		log.Fatalf("Failed to declare retry queue: %v", err)
	}

	_, err = ch.QueueDeclare("jobs.dlq", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare DLQ: %v", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}

	msgs, err := ch.Consume(
		jobsQ.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	fmt.Println("Worker started. Waiting for jobs...")

	for msg := range msgs {
		var job Job
		if err := json.Unmarshal(msg.Body, &job); err != nil {
			log.Printf("Failed to unmarshal job: %v", err)
			msg.Nack(false, false)
			continue
		}

		claimed, err := tryClaimIdempotencyKey(context.Background(), job.IdempotencyKey, job.ID)
		if err != nil {
			log.Printf("Failed to check idempotency key for job %s: %v", job.ID, err)

			body, _ := json.Marshal(job)
			pubErr := ch.Publish("", "jobs.retry", false, false, amqp.Publishing{
				ContentType:  "application/json",
				Body:         body,
				DeliveryMode: amqp.Persistent,
			})
			if pubErr != nil {
				log.Printf("Failed to route job %s to retry queue: %v", job.ID, pubErr)
			}

			msg.Ack(false)
			continue
		}
		if !claimed {
			fmt.Printf("Job %s (key=%s) already processed, skipping\n", job.ID, job.IdempotencyKey)
			msg.Ack(false)
			continue
		}

		fmt.Printf("Processing job %s (attempt %d): %s\n", job.ID, job.RetryCount+1, job.Payload)

		err = processJob(job)
		if err != nil {
			job.RetryCount++
			log.Printf("Job %s failed (attempt %d): %v", job.ID, job.RetryCount, err)

			targetQueue := "jobs.retry"
			if job.RetryCount >= maxRetries {
				targetQueue = "jobs.dlq"
				log.Printf("Job %s exceeded max retries, sending to DLQ", job.ID)
			}

			body, _ := json.Marshal(job)
			pubErr := ch.Publish("", targetQueue, false, false, amqp.Publishing{
				ContentType:  "application/json",
				Body:         body,
				DeliveryMode: amqp.Persistent,
			})
			if pubErr != nil {
				log.Printf("Failed to route job %s: %v", job.ID, pubErr)
			}

			msg.Ack(false)
			continue
		}

		msg.Ack(false)
		fmt.Printf("Completed job %s\n", job.ID)
	}
}

func processJob(job Job) error {
	if job.Type == "always_fails" {
		return fmt.Errorf("simulated failure")
	}
	return nil
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
