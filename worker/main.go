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
const staleClaimAfter = 90 * time.Second

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

	msgs, err := ch.Consume(jobsQ.Name, "", false, false, false, false, nil)
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

		owned, claimErr := tryClaim(context.Background(), job.IdempotencyKey, job.ID, staleClaimAfter)
		if claimErr != nil {
			log.Printf("Claim check failed for job %s: %v", job.ID, claimErr)
			requeueViaRetryQueue(ch, job)
			msg.Ack(false)
			continue
		}

		if !owned {
			status, _ := getClaimStatus(context.Background(), job.IdempotencyKey)
			if status == "completed" {
				fmt.Printf("Job %s already completed, skipping\n", job.ID)
				msg.Ack(false)
				continue
			}
			fmt.Printf("Job %s currently owned by another worker, deferring\n", job.ID)
			requeueViaRetryQueue(ch, job)
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

		if err := markCompleted(context.Background(), job.IdempotencyKey); err != nil {
			log.Printf("Warning: failed to mark job %s completed: %v", job.ID, err)
		}

		msg.Ack(false)
		fmt.Printf("Completed job %s\n", job.ID)
	}
}

func requeueViaRetryQueue(ch *amqp.Channel, job Job) {
	body, _ := json.Marshal(job)
	ch.Publish("", "jobs.retry", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
}

func processJob(job Job) error {
	if job.Type == "always_fails" {
		return fmt.Errorf("simulated failure")
	}
	if job.Type == "slow_job" {
		time.Sleep(60 * time.Second)
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
