import psycopg2
import psycopg2.extras

ENRICHMENT_QUEUES = ("repos", "users", "commits", "contributors", "tree")


def get_connection(database_url):
    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    return conn


def insert_raw_events(conn, events):
    """Batch insert GitHub events into raw_events.

    Returns the number of rows actually inserted (duplicates, keyed on
    event_id, are silently skipped).
    """
    if not events:
        return 0

    rows = [
        (
            int(e["id"]),
            e["type"],
            e["actor"]["login"],
            e["actor"]["id"],
            e["repo"]["id"],
            e["repo"]["name"],
            psycopg2.extras.Json(e.get("payload") or {}),
            e["created_at"],
        )
        for e in events
    ]

    with conn.cursor() as cur:
        inserted = psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO raw_events
                (event_id, event_type, actor_login, actor_id, repo_id, repo_name, payload, github_created_at)
            VALUES %s
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            rows,
            fetch=True,
        )
        return len(inserted)


def upsert_repo_placeholders(conn, events):
    """Insert a placeholder repos row for each distinct repo_id in events.

    Whether a repo counts as "newly seen" is decided here, by the repos
    upsert itself (ON CONFLICT DO NOTHING) -- not by whether the source
    event was newly inserted into raw_events, since the same repo shows
    up in many events over time and should only be enriched once.

    Returns a list of (repo_id, owner_login, repo_name) for repos that
    were genuinely new.
    """
    if not events:
        return []

    seen = {}
    for e in events:
        repo_id = e["repo"]["id"]
        if repo_id in seen:
            continue
        owner_login, repo_name = e["repo"]["name"].split("/", 1)
        seen[repo_id] = (repo_id, owner_login, repo_name)

    with conn.cursor() as cur:
        new_repos = psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO repos (repo_id, owner_login, repo_name)
            VALUES %s
            ON CONFLICT (repo_id) DO NOTHING
            RETURNING repo_id, owner_login, repo_name
            """,
            list(seen.values()),
            fetch=True,
        )
        return new_repos


def enqueue_enrichment_jobs(conn, new_repos):
    """Fan out one enrichment job per queue for each newly-seen repo."""
    if not new_repos:
        return 0

    rows = [
        (queue_name, owner_login, repo_name, repo_id)
        for (repo_id, owner_login, repo_name) in new_repos
        for queue_name in ENRICHMENT_QUEUES
    ]

    with conn.cursor() as cur:
        inserted = psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO job_queue (queue_name, owner_login, repo_name, repo_id)
            VALUES %s
            ON CONFLICT (queue_name, repo_id) DO NOTHING
            RETURNING id
            """,
            rows,
            fetch=True,
        )
        return len(inserted)


def has_pending_jobs(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS(SELECT 1 FROM job_queue WHERE status = 'pending')")
        return cur.fetchone()[0]


def claim_pending_jobs(conn, queue_name, limit):
    """Read up to `limit` pending, due jobs for a queue. Does not mark them
    as claimed in Postgres -- see ARCHITECTURE.md on why there's no
    `processing` state; the caller tracks in-flight jobs in memory."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, owner_login, repo_name, repo_id, attempts, max_attempts
            FROM job_queue
            WHERE queue_name = %s AND status = 'pending' AND available_at <= now()
            ORDER BY id
            LIMIT %s
            """,
            (queue_name, limit),
        )
        return cur.fetchall()


def mark_job_done(conn, job_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE job_queue SET status = 'done', updated_at = now() WHERE id = %s",
            (job_id,),
        )


def reschedule_job(conn, job_id, attempts, error):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_queue
            SET attempts = %s,
                available_at = now() + interval '1 second' * power(2, %s),
                last_error = %s,
                updated_at = now()
            WHERE id = %s
            """,
            (attempts, attempts, str(error)[:2000], job_id),
        )


def mark_job_dead_letter(conn, job_id, attempts, error):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_queue
            SET status = 'dead_letter', attempts = %s, last_error = %s, updated_at = now()
            WHERE id = %s
            """,
            (attempts, str(error)[:2000], job_id),
        )


def get_repo(conn, repo_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM repos WHERE repo_id = %s", (repo_id,))
        return cur.fetchone()


def count_repos_by_owner(conn, owner_login):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM repos WHERE owner_login = %s", (owner_login,))
        return cur.fetchone()[0]


def upsert_repo_fields(conn, repo_id, fields, call_done_column):
    """Set the given columns on a repos row and mark its *_call_done flag.

    `fields` keys must always come from a fixed, code-controlled set of
    known column names (never from external/API-derived strings) -- they
    go directly into the SQL, unparameterized, to build the SET clause.
    """
    fields = dict(fields)
    fields[call_done_column] = True
    set_clause = ", ".join(f"{column} = %s" for column in fields)
    values = list(fields.values()) + [repo_id]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE repos SET {set_clause} WHERE repo_id = %s", values)
