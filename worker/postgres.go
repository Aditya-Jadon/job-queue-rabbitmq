package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
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

// tryClaimIdempotencyKey attempts to record this idempotency key as
// processed. Returns true if this is the first time we've seen it
// (i.e., safe to do real work), false if it's already been processed.
func tryClaimIdempotencyKey(ctx context.Context, key string, jobID string) (bool, error) {
	_, err := db.Exec(ctx,
		"INSERT INTO processed_jobs (idempotency_key, job_id) VALUES ($1, $2)",
		key, jobID,
	)
	if err != nil {
		if isDuplicateKeyError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isDuplicateKeyError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" // unique_violation
	}
	return false
}

func isAlreadyProcessed(ctx context.Context, key string) (bool, error) {
	var exists bool
	err := db.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM processed_jobs WHERE idempotency_key = $1)",
		key,
	).Scan(&exists)
	return exists, err
}

func recordCompletion(ctx context.Context, key string, jobID string) error {
	_, err := db.Exec(ctx,
		"INSERT INTO processed_jobs (idempotency_key, job_id) VALUES ($1, $2) ON CONFLICT (idempotency_key) DO NOTHING",
		key, jobID,
	)
	return err
}
