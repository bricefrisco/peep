import logging
import queue
import time

from . import db
from .handlers import compute_derived_fields

logger = logging.getLogger(__name__)

QUEUE_NAMES = ("repos", "users", "commits", "contributors", "tree")

# repos/users/commits confirm the repo/account exists and are cheap to get --
# a permanent failure there means there's nothing trustworthy to train on, so
# the job stays dead-lettered and the repo never becomes eligible for
# labeling. contributors/tree are enrichment on top of an already-confirmed
# repo: when they exhaust retries, mark *_call_done anyway (fields stay
# NULL) so the repo stays eligible instead of blocking on one missing
# feature group.
MARK_DONE_ON_DEAD_LETTER = ("contributors", "tree")


def build_local_queues():
    return {name: queue.Queue() for name in QUEUE_NAMES}


def puller_loop(conn, local_queues, in_flight_ids, in_flight_lock, batch_size=25, interval=1.0):
    while True:
        for queue_name, local_q in local_queues.items():
            if not local_q.empty():
                continue

            rows = db.claim_pending_jobs(conn, queue_name, batch_size)
            with in_flight_lock:
                for row in rows:
                    job_id = row[0]
                    if job_id in in_flight_ids:
                        continue
                    in_flight_ids.add(job_id)
                    local_q.put(row)

        time.sleep(interval)


def worker_loop(queue_name, local_q, handler, conn, rate_limiter, session, in_flight_ids, in_flight_lock):
    call_done_column = f"{queue_name}_call_done"

    while True:
        job_id, owner, repo, repo_id, attempts, max_attempts = local_q.get()
        try:
            rate_limiter.acquire()
            repo_row = db.get_repo(conn, repo_id)
            fields = handler(session, conn, owner, repo, repo_row, rate_limiter)

            merged = dict(repo_row or {})
            merged.update(fields)
            fields.update(compute_derived_fields(merged))

            db.upsert_repo_fields(conn, repo_id, fields, call_done_column)
            db.mark_job_done(conn, job_id)
            logger.info("job %d (%s) done: %s/%s", job_id, queue_name, owner, repo)
        except Exception as exc:
            _handle_failure(conn, job_id, repo_id, attempts, max_attempts, queue_name, call_done_column, exc)
        finally:
            with in_flight_lock:
                in_flight_ids.discard(job_id)


def _handle_failure(conn, job_id, repo_id, attempts, max_attempts, queue_name, call_done_column, exc):
    attempts += 1
    if attempts >= max_attempts:
        db.mark_job_dead_letter(conn, job_id, attempts, exc)
        if queue_name in MARK_DONE_ON_DEAD_LETTER:
            db.upsert_repo_fields(conn, repo_id, {}, call_done_column)
            logger.error(
                "job %d (%s) dead-lettered after %d attempts -- marking %s complete with null fields: %s",
                job_id, queue_name, attempts, call_done_column, exc,
            )
        else:
            logger.error(
                "job %d (%s) dead-lettered after %d attempts -- repo excluded from training data: %s",
                job_id, queue_name, attempts, exc,
            )
    else:
        logger.warning("job %d (%s) failed (attempt %d/%d): %s", job_id, queue_name, attempts, max_attempts, exc)
        db.reschedule_job(conn, job_id, attempts, exc)
