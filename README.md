# Distributed Job Queue

A Go-based distributed task processing system built around RabbitMQ, designed to survive the failures that actually happen in production: crashed workers, redelivered messages, and infrastructure hiccups — not just the happy path.

**Architecture:** Producer → RabbitMQ → 3 independent Worker replicas → Postgres (idempotency tracking)

---

## Why this project

Most job-queue demos stop at "publish a message, consume a message." This one is built around a harder, more honest question: **what happens when a worker dies in the middle of doing real work?** That question is answered here with an actual chaos test — killing a worker pod mid-job and watching what happens — not just a description of how retries are supposed to work.

That test caught a real, subtle bug in the first version of this system. Finding and fixing it is the core story of this project.

## Delivery guarantee: at-least-once, not exactly-once

True exactly-once delivery is not achievable in a distributed system — the network can always fail between "I did the work" and "I told you I did the work." This project targets what real systems actually implement: **at-least-once delivery + idempotent processing**, producing *effectively-once* behavior. RabbitMQ may occasionally redeliver a message; the worker's idempotency layer ensures redelivery never causes duplicate real-world side effects.

## The core story: a bug chaos testing was built to find

**The setup:** a `slow_job` type with a long artificial delay, giving a real window to kill a worker mid-processing and observe what RabbitMQ and the idempotency layer do.

**First run — a real bug, found twice independently:**

1. Worker A picks up a job and starts processing.
2. Worker A's pod is killed mid-job, before it can acknowledge the message.
3. RabbitMQ correctly detects the dropped connection and redelivers the message to Worker B.
4. Worker B checks the idempotency key — and finds it **already claimed**. It skips the job.
5. **The job is never actually completed anywhere.** No error, no crash — just silently lost, permanently marked "done" in Postgres despite never finishing.

**Root cause:** the idempotency key was claimed *before* the real work ran, not after it succeeded. The claim and the completion were treated as the same event — but a crash between them meant the job was marked done without ever being done. This happened consistently across two independent test runs, ruling out a fluke.

**The fix:** split the idempotency check into two steps —
- A cheap **pre-check** (read-only) before processing, to skip genuinely-completed duplicates fast.
- The actual **claim** only happens *after* `processJob` succeeds.

```go
err = processJob(job)
if err != nil {
    // retry / DLQ handling — unchanged
}

// Only recorded as done AFTER real work has genuinely succeeded.
recordCompletion(ctx, job.IdempotencyKey, job.ID)
msg.Ack(false)
```

This closes the silent-data-loss gap, but the original two-step version (separate read-then-write calls) still left a much smaller, much rarer race: two workers processing the exact same redelivered message truly simultaneously could both pass the check before either recorded completion.

**Follow-up fix: an atomic claim-with-lease pattern.** The two-step check-then-record logic was replaced with a single atomic SQL statement — an `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE <claim is stale>` — that claims a job, detects a legitimate in-progress duplicate, and detects (and safely reclaims) a stale claim left behind by a crashed worker, all in one round trip:

```sql
INSERT INTO job_claims (idempotency_key, job_id, status, claimed_at)
VALUES ($1, $2, 'in_progress', NOW())
ON CONFLICT (idempotency_key) DO UPDATE
    SET job_id = EXCLUDED.job_id, claimed_at = NOW()
    WHERE job_claims.status = 'in_progress'
      AND job_claims.claimed_at < NOW() - $3::interval
RETURNING status
```

Verified end-to-end: a worker killed mid-job leaves its claim in place; other workers correctly **defer** rather than race to steal it, for as long as the claim could still be legitimate (a configurable staleness window, 90s in this build); once that window passes, exactly one worker reclaims and completes the job. Confirmed live — 9 deferral cycles (10s apart, via the same retry-queue TTL mechanism used for job failures) followed by a successful reclaim and completion.

**Re-running the exact same chaos test after the fix:**

| | Before fix | After fix |
|---|---|---|
| Worker killed mid-job | ✅ | ✅ |
| Message redelivered | ✅ | ✅ |
| Job actually completed afterward | ❌ Silently lost | ✅ Completed by a second worker |

Full reliability loop confirmed end-to-end: kill mid-job → redelivery → genuine completion, zero data loss.

## Retries, backoff, and the dead-letter queue

Job failures are retried automatically with a delay, using RabbitMQ's TTL + dead-letter-exchange mechanism rather than any custom scheduler:

- A failed job is republished to a `jobs.retry` queue with a 10-second TTL and no consumer.
- Once the TTL expires, RabbitMQ automatically dead-letters it back into the main `jobs` queue — a delayed retry, entirely via RabbitMQ's built-in primitives.
- After 3 failed attempts, the job routes to `jobs.dlq` instead — a terminal, unconsumed queue for manual inspection, rather than retrying forever.

Verified with an always-failing job type: exactly 3 attempts, 10 seconds apart, then correctly routed to the DLQ.

## Multi-replica correctness

Three independent worker replicas consume from the same queue. Verified:
- A single published job is picked up by exactly **one** of the three workers — the other two remain idle. No duplication at the queue-distribution level.
- Under a genuine crash (the chaos test above), the job correctly migrates to a different replica rather than being lost or double-processed.

## A second real bug: a zero-delay retry loop

An early version of the idempotency error-handling path used `msg.Nack(requeue=true)` on any infrastructure failure (e.g., a Postgres blip) — causing RabbitMQ to redeliver the message *instantly*, in a tight loop, hammering Postgres as fast as the CPU allowed. Fixed by routing infrastructure errors through the same `jobs.retry` TTL-delay queue used for job failures, applying the same backoff principle to a failure class that hadn't originally been considered.

## A third real incident: Kubernetes namespace collision

Deploying this project's manifests into the same cluster as an earlier project (a URL shortener) using an identically-named `postgres` Secret and Deployment silently overwrote the other project's database configuration. No actual data was lost — Kubernetes PVCs are independent of Deployments, and Postgres only reads its init config on a genuinely empty data directory — but it was a real, avoidable collision. Fixed by giving this project its own Kubernetes namespace (`job-queue`), the correct way to run multiple unrelated applications on one shared cluster.

## Tech stack

| Layer | Technology |
|---|---|
| Language | Go |
| Message broker | RabbitMQ (AMQP 0-9-1) |
| Idempotency store | PostgreSQL |
| Containerization | Docker (multi-stage builds) |
| Local orchestration | Docker Compose |
| Orchestration | Kubernetes (Deployments, Jobs, namespaces) |
| CI/CD | GitHub Actions → GitHub Container Registry (GHCR) |

## Project layout

```
job-queue/
├── producer/          # publishes jobs onto the queue (own Go module)
├── worker/            # consumes, processes, retries, tracks idempotency
├── k8s/               # Kubernetes manifests (namespaced: job-queue)
└── docker-compose.yml
```

## Running it

### Docker Compose

```bash
docker-compose up --build
docker-compose run --rm producer
```

### Kubernetes (Minikube)

```bash
minikube start
eval $(minikube docker-env)
docker build -t job-queue-worker ./worker
docker build -t job-queue-producer ./producer

kubectl create namespace job-queue
kubectl apply -f k8s/ -n job-queue

# Run the producer as a one-shot Job
kubectl apply -f k8s/producer-job.yaml -n job-queue
kubectl logs -l app=worker -n job-queue --prefix
```

## Load testing

A 300-job batch was published to the queue and processed across the 3 worker replicas in Kubernetes:

| | Result |
|---|---|
| Publish throughput | 4,273 jobs/sec |
| Jobs processed | 300 / 300 (zero loss, zero duplication) |
| End-to-end processing time | 0.556 sec |
| Processing throughput (3 workers) | ~540 jobs/sec |

Verified via the `job_claims` table directly (one row per job, all `completed`), not just log counts — an earlier measurement attempt initially looked like it had processed jobs twice (602 completions logged), which turned out to be two separate test runs accumulating in the same table rather than a real bug; truncating the table before a clean single run resolved the ambiguity and confirmed the system behaves correctly.

## What's next

- Feed this project's structured logs into a log aggregation platform — the natural next step in this portfolio