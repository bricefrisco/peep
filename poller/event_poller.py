import logging
import time

import requests

from . import db
from .github_client import GitHubEventsClient

logger = logging.getLogger(__name__)


def poll_events_loop(conn, github_token, backlog_check_interval=5):
    """Polls /events, but only while job_queue is drained.

    Enrichment is capped at ~1,000-1,250 repos/hour by the shared rate
    limiter; unthrottled polling can surface several times that many new
    repos/hour, so polling backs off until the workers catch up rather
    than growing an unbounded backlog.
    """
    client = GitHubEventsClient(github_token)
    waiting_on_backlog = False

    while True:
        if db.has_pending_jobs(conn):
            if not waiting_on_backlog:
                logger.info("job_queue has pending jobs; pausing /events polling until it drains")
                waiting_on_backlog = True
            time.sleep(backlog_check_interval)
            continue

        if waiting_on_backlog:
            logger.info("job_queue drained; resuming /events polling")
            waiting_on_backlog = False

        try:
            events, poll_interval = client.poll()
        except requests.RequestException:
            logger.exception("failed to poll /events")
            time.sleep(60)
            continue

        if events:
            inserted = db.insert_raw_events(conn, events)
            new_repos = db.upsert_repo_placeholders(conn, events)
            queued = db.enqueue_enrichment_jobs(conn, new_repos)
            logger.info(
                "polled %d events (%d new), %d new repos, %d jobs queued",
                len(events),
                inserted,
                len(new_repos),
                queued,
            )
        else:
            logger.debug("polled 0 events (304 not modified)")

        time.sleep(poll_interval)
