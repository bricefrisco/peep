# Architecture

## Overview

Peep runs as a **single Python process** containing multiple
threads. PostgreSQL serves two roles simultaneously: the system of record
(all ingested/enriched data) and the job queue for enrichment work (no
external broker, no Docker, no Redis).

```
GitHub /events  ──▶  poller thread
                       - batch INSERT into raw_events (ON CONFLICT DO NOTHING)
                                │
                                ▼
                       raw_events table (append-only)
```

The poller writes directly to `raw_events` — no queue in between. The
insert is already idempotent (unique `event_id` primary key), and there's
only ever one poller, so nothing is gained by routing it through
`job_queue` first.

**Implemented:** the poller above. **Not yet built** (see the
[roadmap](./README.md#roadmap)): fanning newly-seen repos out into five
enrichment jobs, and the workers that consume them:

```
                                │
                    ┌───────────────┬───────────┼───────────┬───────────────┐
                    ▼               ▼           ▼           ▼               ▼
               'repos' queue   'users' queue 'commits'  'contributors'  'tree' queue
                    │               │           │           │               │
                    ▼               ▼           ▼           ▼               ▼
              repos worker    users worker  commits    contributors    tree worker
                                             worker      worker
                    │               │           │           │               │
                    └───────────────┴───────────┴───────────┴───────────────┘
                                                │
                                                ▼
                                   repos table (wide, incrementally filled)
                                   *_call_done flags mark completion per column group
```

All GitHub API calls — from whichever thread is making them — pass
through one shared, in-process rate limiter capped at 1 request/second.

## Why Postgres-as-queue instead of Redis/RQ/Celery

Considered and rejected:

- **RQ + Redis** — good retry/backoff semantics, but Redis has no native
  Windows build, requiring Docker/WSL2 as a dependency. Rejected to avoid
  that operational overhead.
- **Celery** — more powerful than needed; too many moving parts for a
  pipeline bottlenecked at ~1 request/second regardless of queue
  technology.
- **Kafka** — built for high-throughput distributed streaming; overkill
  for a single-machine pipeline with a hard external rate ceiling.

Given the pipeline is rate-limited to roughly 1,000–1,250 repos/hour by
GitHub itself (not by compute), queue *throughput* was never the deciding
factor. What mattered was: no extra services, cross-platform (Windows +
Linux), Python-native, and durable across restarts. Postgres, which the
project already depends on for storage, satisfies all of these without
adding a new system.

## Why a single process instead of multiple processes

Originally scoped as 6+ separate consumer processes (one per queue). This
was simplified to threads within a single process because:

- The workload is **I/O-bound**, not CPU-bound — nearly all time is spent
  blocked on the rate limiter, HTTP responses, or Postgres round-trips.
  Python's GIL is not a bottleneck for this shape of work.
- One process is simpler to start, stop, monitor, and log on a single
  machine with a single operator.
- RAM overhead is lower: one interpreter and one set of loaded libraries,
  rather than 6+ separate processes each loading their own copies
  (~60–100MB total vs. ~200–350MB for the multi-process version).
- Go was considered as an alternative for lower per-process memory
  overhead, but rejected: the memory savings are irrelevant at this scale
  (Pi 5, 4–8GB RAM, workload capped at 1 req/sec), and Go lacks a mature
  ML ecosystem equivalent to scikit-learn/LightGBM, which the project
  needs downstream for model training. Splitting the stack across two
  languages wasn't justified by a RAM saving that was never a real
  constraint.

Trade-off accepted: no OS-level process isolation. A single unhandled
interpreter-level failure could in principle affect all worker threads at
once. Mitigated by wrapping every handler call in `try/except` and setting
explicit timeouts on all network calls.

## Job lifecycle

Jobs are stored durably in the `job_queue` table (see `SCHEMA.md`), but
**claiming and dispatch happen in memory**, not row-by-row against
Postgres, for the reasons below.

### States

Only two persisted states: `pending` and `done` (plus `dead_letter` for
jobs that exhausted retries). There is **no `processing` state stored in
Postgres.**

This is a deliberate simplification, valid specifically because of two
properties of this design:

1. **Single puller.** Only one loop ever pulls jobs out of Postgres into
   memory, so there's no risk of two independent claimants racing for the
   same row (the problem `FOR UPDATE SKIP LOCKED` / a `processing` state
   would normally guard against in a multi-worker-process design).
2. **Idempotent handlers.** Every enrichment handler is an upsert
   (`UPDATE repos SET ... WHERE repo_id = ...`) — reprocessing the same
   job twice produces the same end state as processing it once. The
   enrichment fan-out itself is made idempotent via a unique constraint
   plus `ON CONFLICT DO NOTHING` on `job_queue` inserts, keyed on
   `(queue_name, repo_id)`.

Given both properties, a crash between "pulled into memory" and "marked
done" simply leaves the job as `pending` in Postgres — safe to re-pull and
reprocess on restart. This avoids an entire class of bugs seen in
`processing`-state designs, where a crash can leave rows permanently
stuck and require a separate timeout/sweep job to recover them.

In-flight duplication *within* a single running process (the puller
grabbing the same still-`pending` row twice before the first pull is
processed) is prevented by an in-memory `in_flight_ids` set — cheap,
requires no DB round-trip to check, and naturally resets to empty on
every process restart.

### Flow

```
pending ──(puller pulls into local queue.Queue, in_flight_ids updated)──▶ [in memory]
[in memory] ──(handler succeeds)──▶ done
[in memory] ──(handler fails, attempts < max)──▶ pending (available_at pushed back, exponential backoff)
[in memory] ──(handler fails, attempts >= max)──▶ dead_letter
```

### Pulling: only when a local queue is empty

The puller loop checks each local `queue.Queue` and only issues a Postgres
`SELECT` for queues that are currently empty (`local_q.empty()`), rather
than polling all queues on a fixed timer regardless of backlog. This
avoids issuing pointless queries when there's nothing to do. The loop
interval is kept short (~0.5–1s) since the check itself is cheap — it
only touches Postgres when a queue actually needs refilling.

```python
def puller_loop(conn, batch_size=50, interval=1.0):
    while True:
        for queue_name, local_q in local_queues.items():
            if not local_q.empty():
                continue
            # ... pull up to batch_size pending rows, mark in_flight, push to local_q
        time.sleep(interval)
```

A brief staleness in the `local_q.empty()` check (a known characteristic
of `queue.Queue`) is harmless here — worst case, a queue that just emptied
gets refilled on the next loop iteration a fraction of a second later, not
a correctness issue.

## Rate limiting

A single in-memory rate limiter, shared by reference across all worker
threads, enforces a global ceiling of 1 GitHub API call/second across
*all* queues combined (not 1/sec per queue):

```python
class RateLimiter:
    def __init__(self, min_interval=1.0):
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.last_call = 0.0

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self.last_call)
            if wait > 0:
                time.sleep(wait)
            self.last_call = time.monotonic()
```

This lives entirely in process memory — no Postgres round-trip — because
all rate-limited callers are threads within the same process and share
memory directly. (A Postgres-row-based limiter, using `SELECT ... FOR
UPDATE`, was an earlier design considered for coordinating *across
processes*; it was dropped once the project moved to a single-process
model, since it solves a coordination problem that no longer exists here
and adds unnecessary DB load.)

`time.monotonic()` is used instead of `time.time()` to avoid rate-limit
drift from system clock adjustments (e.g. NTP sync).

## Optional: LISTEN/NOTIFY

Not required for correctness — the puller's short polling interval
already provides low latency at this throughput. Documented here as a
future optimization: Postgres's native pub/sub (`LISTEN`/`NOTIFY`) can
wake a puller immediately when a new job is inserted, instead of waiting
for the next poll tick. Notifications are not persisted or queued by
Postgres — they only reach a currently-listening connection — so any
`LISTEN`/`NOTIFY` implementation should retain a periodic fallback poll as
a safety net against missed notifications, rather than relying on it
exclusively.

## Crash recovery summary

| Failure point | Recovery |
|---|---|
| Crash after job pulled into memory, before handled | Row still `pending` in Postgres; re-pulled on restart |
| Crash mid-fan-out (some of 5 enrichment jobs inserted, not all) | Re-inserting the remaining jobs is safe — `ON CONFLICT DO NOTHING` dedup on `(queue_name, repo_id)` makes fan-out idempotent |
| Handler throws an exception | Job rescheduled with exponential backoff, up to `max_attempts`, then moved to `dead_letter`. For `contributors`/`tree`, the repo's `*_call_done` is still set so it stays eligible for training with that feature group left `NULL`; for `repos`/`users`/`commits`, `*_call_done` stays `FALSE` and the repo is permanently excluded -- see `SCHEMA.md` |
| In-flight duplication within a single run | Prevented by in-memory `in_flight_ids` set |

## Threading model

```python
def main():
    threads = [
        threading.Thread(target=poll_events_loop, daemon=True),   # polls /events, batch-writes raw_events
        threading.Thread(target=puller_loop, daemon=True),        # not yet built
        threading.Thread(target=worker_loop, args=("repos", handle_repos_call), daemon=True),
        threading.Thread(target=worker_loop, args=("users", handle_users_call), daemon=True),
        threading.Thread(target=worker_loop, args=("commits", handle_commits_call), daemon=True),
        threading.Thread(target=worker_loop, args=("contributors", handle_contributors_call), daemon=True),
        threading.Thread(target=worker_loop, args=("tree", handle_tree_call), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
```

Each worker thread owns its own Postgres connection (`psycopg2` connections
are not thread-safe to share). The shared `RateLimiter` instance and the
`local_queues` dict of `queue.Queue` objects are the only state shared
across threads, both of which are safe for concurrent use.