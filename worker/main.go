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

	for msg := range msgs {
		var job Job
		if err := json.Unmarshal(msg.Body, &job); err != nil {
			log.Printf("Failed to unmarshal job: %v", err)
			msg.Nack(false, false)
			continue
		}

		// Quick, cheap pre-check: if we've already recorded this key as
		// done, skip without even attempting the work again. This is
		// safe to check early — we're not writing here, just reading.
		alreadyDone, checkErr := isAlreadyProcessed(context.Background(), job.IdempotencyKey)
		if checkErr != nil {
			log.Printf("Failed to check idempotency key for job %s: %v", job.ID, checkErr)
			body, _ := json.Marshal(job)
			ch.Publish("", "jobs.retry", false, false, amqp.Publishing{
				ContentType: "application/json", Body: body, DeliveryMode: amqp.Persistent,
			})
			msg.Ack(false)
			continue
		}
		if alreadyDone {
			fmt.Printf("Job %s (key=%s) already completed, skipping\n", job.ID, job.IdempotencyKey)
			msg.Ack(false)
			continue
		}

		fmt.Printf("Processing job %s (attempt %d): %s\n", job.ID, job.RetryCount+1, job.Payload)

		err := processJob(job)
		if err != nil {
			job.RetryCount++
			log.Printf("Job %s failed (attempt %d): %v", job.ID, job.RetryCount, err)

			targetQueue := "jobs.retry"
			if job.RetryCount >= maxRetries {
				targetQueue = "jobs.dlq"
				log.Printf("Job %s exceeded max retries, sending to DLQ", job.ID)
			}

			body, _ := json.Marshal(job)
			ch.Publish("", targetQueue, false, false, amqp.Publishing{
				ContentType: "application/json", Body: body, DeliveryMode: amqp.Persistent,
			})
			msg.Ack(false)
			continue
		}

		// Only record the idempotency key AFTER real work has genuinely
		// succeeded — this is the fix. Claiming it earlier meant a crash
		// mid-processing left the job permanently, silently marked done
		// without ever actually finishing.
		if claimErr := recordCompletion(context.Background(), job.IdempotencyKey, job.ID); claimErr != nil {
			log.Printf("Warning: failed to record completion for job %s: %v", job.ID, claimErr)
		}

		msg.Ack(false)
		fmt.Printf("Completed job %s\n", job.ID)
	}
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
