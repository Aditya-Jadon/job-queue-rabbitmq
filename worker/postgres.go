package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var db *pgxpool.Pool

func connectPostgres() {
	var err error
	connString := "postgres://postgres:devpassword@postgres:5432/jobqueue"

	db, err = pgxpool.New(context.Background(), connString)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Postgres: %v", err))
	}

	if err := db.Ping(context.Background()); err != nil {
		panic(fmt.Sprintf("Failed to ping Postgres: %v", err))
	}

	fmt.Println("Postgres connected")
}

// tryClaim atomically claims a job. Returns true if this call now owns
// the job and should process it. Returns false if it's already
// completed, or legitimately owned by another worker that hasn't gone
// stale yet.
func tryClaim(ctx context.Context, key, jobID string, staleAfter time.Duration) (bool, error) {
	var status string
	err := db.QueryRow(ctx, `
		INSERT INTO job_claims (idempotency_key, job_id, status, claimed_at)
		VALUES ($1, $2, 'in_progress', NOW())
		ON CONFLICT (idempotency_key) DO UPDATE
			SET job_id = EXCLUDED.job_id, claimed_at = NOW()
			WHERE job_claims.status = 'in_progress'
			  AND job_claims.claimed_at < NOW() - $3::interval
		RETURNING status
	`, key, jobID, fmt.Sprintf("%d seconds", int(staleAfter.Seconds()))).Scan(&status)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func markCompleted(ctx context.Context, key string) error {
	_, err := db.Exec(ctx,
		"UPDATE job_claims SET status = 'completed', completed_at = NOW() WHERE idempotency_key = $1",
		key,
	)
	return err
}

func getClaimStatus(ctx context.Context, key string) (string, error) {
	var status string
	err := db.QueryRow(ctx, "SELECT status FROM job_claims WHERE idempotency_key = $1", key).Scan(&status)
	return status, err
}
