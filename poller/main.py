import logging
import os
import sys
import threading

from dotenv import load_dotenv

from . import db, handlers, queue_manager
from .github_client import build_session
from .event_poller import poll_events_loop
from .ratelimiter import RateLimiter

logger = logging.getLogger(__name__)

HANDLERS = {
    "repos": handlers.handle_repos_call,
    "users": handlers.handle_users_call,
    "commits": handlers.handle_commits_call,
    "contributors": handlers.handle_contributors_call,
    "tree": handlers.handle_tree_call,
}


def main():
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    github_token = os.environ["GITHUB_TOKEN"]
    database_url = os.environ["DATABASE_URL"]

    local_queues = queue_manager.build_local_queues()
    in_flight_ids = set()
    in_flight_lock = threading.Lock()
    rate_limiter = RateLimiter(min_interval=1.0)

    threads = [
        threading.Thread(
            target=poll_events_loop,
            args=(db.get_connection(database_url), github_token),
            daemon=True,
        ),
        threading.Thread(
            target=queue_manager.puller_loop,
            args=(db.get_connection(database_url), local_queues, in_flight_ids, in_flight_lock),
            daemon=True,
        ),
    ]

    for queue_name, handler in HANDLERS.items():
        threads.append(
            threading.Thread(
                target=queue_manager.worker_loop,
                args=(
                    queue_name,
                    local_queues[queue_name],
                    handler,
                    db.get_connection(database_url),
                    rate_limiter,
                    build_session(github_token),
                    in_flight_ids,
                    in_flight_lock,
                ),
                daemon=True,
            )
        )

    for t in threads:
        t.start()

    try:
        # A plain, no-timeout join() blocks the main thread in a way that
        # doesn't reliably return control to the interpreter to check for a
        # pending Ctrl+C -- since every thread here runs forever, that means
        # KeyboardInterrupt would never actually get raised. Polling with a
        # short timeout gives the interpreter a chance to notice the signal.
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        logger.info("Ctrl+C received, shutting down (in-flight jobs remain 'pending' and will resume on restart)")
        sys.exit(0)


if __name__ == "__main__":
    main()
