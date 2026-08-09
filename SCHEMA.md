# Database Schema

PostgreSQL 14+. All tables live in a single database; no separate schema
namespace required at this scale.

## Table overview

| Table | Purpose |
|---|---|
| `job_queue` | Durable job queue backing all pipeline stages |
| `rate_limiter` | Unused in the single-process design — see note below |
| `raw_events` | Append-only log of every event pulled from `/events` |
| `repos` | One row per repository, incrementally enriched; the model's training/inference table |
| `api_call_log` | Optional per-call debug log |

> **Note on `rate_limiter`:** an earlier design used a Postgres row with
> `SELECT ... FOR UPDATE` to coordinate a rate limit across multiple
> worker *processes*. The project moved to a single-process, threaded
> design, where an in-memory `threading.Lock`-based limiter accomplishes
> the same thing without DB overhead (see `ARCHITECTURE.md`). This table
> is kept here for reference only — omit it unless the project moves back
> to a multi-process model.

---

## `job_queue`

```sql
CREATE TABLE job_queue (
    id             BIGSERIAL PRIMARY KEY,
    queue_name      TEXT NOT NULL,             -- 'repos' | 'users' | 'commits' | 'contributors' | 'tree'
    owner_login      TEXT NOT NULL,
    repo_name         TEXT NOT NULL,
    repo_id           BIGINT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',   -- 'pending' | 'done' | 'dead_letter'
    attempts         INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,
    available_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_job_queue_poll ON job_queue (queue_name, status, available_at);

-- Prevents duplicate enrichment jobs if fan-out is retried
CREATE UNIQUE INDEX uq_job_queue_dedup
ON job_queue (queue_name, repo_id);
```

**Notes:**

- Only three logical states are used: `pending`, `done`, `dead_letter`.
  There is no `processing` state — see `ARCHITECTURE.md` for why this is
  safe in a single-puller, idempotent-handler design.
- `owner_login`, `repo_name`, and `repo_id` are the only inputs every
  enrichment handler needs, and are the same three fields regardless of
  `queue_name` — so they're plain columns rather than a `payload` blob
  (unlike `raw_events.payload`, which genuinely varies by `event_type`).
- `available_at` is used both for initial scheduling and for exponential
  backoff on retry (`available_at = now() + interval '1 second' *
  power(2, attempts)`).
- The unique constraint on `(queue_name, repo_id)` makes re-inserting the
  same enrichment job a no-op (`ON CONFLICT DO NOTHING`) if fan-out is
  ever retried after a crash.

---

## `raw_events`

Append-only. Never updated after insert.

```sql
CREATE TABLE raw_events (
    event_id            BIGINT PRIMARY KEY,        -- GitHub's own event id
    event_type           TEXT NOT NULL,              -- e.g. 'PushEvent'
    actor_login           TEXT NOT NULL,
    actor_id              BIGINT NOT NULL,
    repo_id               BIGINT NOT NULL,
    repo_name             TEXT NOT NULL,              -- "owner/repo"
    payload               JSONB,                      -- raw event payload, kept for future feature mining
    github_created_at     TIMESTAMPTZ NOT NULL,
    ingested_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_events_repo_id ON raw_events(repo_id);
CREATE INDEX idx_raw_events_created_at ON raw_events(github_created_at);
```

---

## `repos`

The core table. One row per repository. Populated incrementally by five
independent enrichment workers, each responsible for a distinct set of
columns. Columns are grouped below by origin and by whether they are fed
to the classifier.

```sql
CREATE TABLE repos (
    repo_id                BIGINT PRIMARY KEY,
    owner_login              TEXT NOT NULL,
    repo_name                TEXT NOT NULL,

    -- ── Metadata (NOT fed to the model) ─────────────────────────────
    created_at                 TIMESTAMPTZ,
    pushed_at                  TIMESTAMPTZ,
    default_branch              TEXT,               -- from the repos call; the tree call depends on this
    account_created_at         TIMESTAMPTZ,
    fetched_at                  TIMESTAMPTZ,

    -- Manual labeling (ground truth)
    manual_label                TEXT,               -- e.g. 'legitimate' | 'suspicious' | null (unlabeled)
    manual_label_by              TEXT,               -- labeler identity
    manual_labeled_at             TIMESTAMPTZ,

    -- Model predictions (inference output — NOT ground truth)
    model_prediction              TEXT,
    model_confidence              REAL,               -- predict_proba() output, 0.0-1.0
    model_version                  TEXT,               -- e.g. 'gbt_2026-08-08'
    model_predicted_at              TIMESTAMPTZ,

    -- ── Feature columns (fed to the model) ──────────────────────────

    -- Account context  [source: GET /users/{owner}]
    account_age_days_at_repo_creation  INT,
    account_public_repos                 INT,
    account_followers                     INT,
    account_following                     INT,
    account_type                           TEXT,       -- 'User' | 'Organization'

    -- Repo activity / velocity  [source: GET /repos/{owner}/{repo}, commits, contributors]
    repo_age_days             INT,
    size_kb                    INT,
    commit_count_approx        BIGINT,     -- from commits?per_page=1 Link header
    commits_per_day            REAL,       -- derived: commit_count_approx / repo_age_days
    contributor_count_approx   INT,        -- from contributors?per_page=1&anon=true Link header
    is_fork                    BOOLEAN,
    archived                   BOOLEAN,
    has_issues_enabled         BOOLEAN,

    -- File structure  [source: GET /git/trees/{sha}?recursive=1 — paths/sizes only, never content]
    file_count                       INT,
    tree_truncated                    BOOLEAN,    -- true if GitHub truncated a large tree response
    extension_count_distinct          INT,
    top_extension                      TEXT,
    extension_homogeneity_ratio       REAL,       -- share of files sharing the most common extension
    filename_template_score            REAL,       -- 0-1, local regex/edit-distance score for generated-looking filenames
    avg_path_length                    REAL,
    primary_language                   TEXT,       -- from GET /repos/{owner}/{repo}

    -- Presentation / engagement  [source: GET /repos/{owner}/{repo}]
    description_present    BOOLEAN,
    description_length     INT,
    topics_count            INT,
    license_present         BOOLEAN,
    stars                    INT,
    forks                    INT,
    watchers                 INT,
    open_issues_count        INT,

    -- Cross-repo  [computed from this table, not fetched from the API]
    sibling_repo_count_same_account INT,

    -- ── Pipeline completion tracking ────────────────────────────────
    repos_call_done          BOOLEAN DEFAULT FALSE,
    users_call_done           BOOLEAN DEFAULT FALSE,
    commits_call_done         BOOLEAN DEFAULT FALSE,
    contributors_call_done     BOOLEAN DEFAULT FALSE,
    tree_call_done              BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_repos_owner ON repos(owner_login);
CREATE INDEX idx_repos_manual_label ON repos(manual_label);
```

### `*_call_done` semantics: "finished," not "succeeded"

A `*_call_done` flag means that queue's job reached a terminal state for
that repo -- either the call succeeded, or it permanently failed and the
pipeline gave up. What happens on permanent failure (job exhausts
`max_attempts` and is moved to `dead_letter`) differs by queue:

- **`repos`, `users`, `commits`** -- these confirm the repo/account still
  exists (`repos`, `users`) or are cheap enough to insist on (`commits`).
  A dead-lettered job here leaves `*_call_done = FALSE` permanently -- the
  repo never becomes eligible for labeling/training. Most often this is a
  404 because the repo or owner account was deleted/banned/made private
  between being seen in the event stream and enrichment running -- there's
  no repo left to verify against, so it's correctly excluded rather than
  trained on with missing identity data.
- **`contributors`, `tree`** -- enrichment on top of an already-confirmed
  repo. A dead-lettered job here still sets `*_call_done = TRUE`, leaving
  that call's feature columns `NULL`. The repo stays eligible for
  labeling/training with one feature group missing, rather than being
  excluded over (for example) a repo too large for the tree endpoint to
  enumerate. See `poller/queue_manager.py`'s `MARK_DONE_ON_DEAD_LETTER`.

### "Ready for labeling / inference" view

A repo is fully enriched once all five `*_call_done` flags are true (which,
per above, includes repos that gave up on `contributors` or `tree`). This
is the intended read surface for both the labeling app and the model
training/inference pipeline:

```sql
WHERE repos_call_done AND users_call_done AND commits_call_done
  AND contributors_call_done AND tree_call_done
```

with an additional `AND manual_label IS NULL` filter for the labeling
queue specifically.

### Label priority

The order repos are presented in the labeling app. Before a model exists,
repos are presented in FIFO order. Once a model exists, order by:

```sql
ORDER BY ABS(model_confidence - 0.5) ASC
```

i.e., repos the model is least certain about are surfaced first — this is
the active-learning loop: manual labeling effort is spent where it moves
the model the most, not on cases already obvious to it.

### Columns deliberately excluded (and why)

Several columns considered during design were cut to minimize API calls,
since each additional GitHub endpoint costs a full request against the
shared rate limit:

| Cut column | Reason |
|---|---|
| `language_count` (byte-per-language breakdown via `/languages`) | Redundant with `primary_language`, already free from `/repos` |
| `readme_present` (via `/readme`) | Strongly correlated with `description_present`; not independently informative enough to justify its own call |
| `issue_pr_count_approx` (via `/issues?state=all`) | Redundant with `open_issues_count`, already free from `/repos` |
| `url_count_in_files`, `ip_count_in_files`, `avg_file_entropy`, `flagged_url_count`, `flagged_ip_count` | Require reading file *contents*, which doesn't scale (some repos have thousands of files) and violates the metadata-only design principle. May return as an optional, expensive confirmation step for a short list of already-flagged repos — not part of the base pipeline. |
| `name_description_similarity_score` | Requires an embedding model dependency; deferred, not ruled out |

`contributor_count_approx` was initially cut on the assumption it was
implied by commit velocity, then restored — a repo with high commit
velocity and *low* contributor count (single-actor automation) is a
meaningfully different pattern from high velocity with *multiple*
contributors (coordinated multi-account activity), and only a direct
contributor count distinguishes the two.

---

## `api_call_log` (optional)

Not required for pipeline correctness; useful for debugging retry
behavior and diagnosing which endpoint is failing during an outage.

```sql
CREATE TABLE api_call_log (
    id             BIGSERIAL PRIMARY KEY,
    repo_id         BIGINT REFERENCES repos(repo_id),
    endpoint         TEXT NOT NULL,       -- 'repos' | 'users' | 'commits' | 'contributors' | 'tree'
    status_code       INT,
    attempt           INT NOT NULL DEFAULT 1,
    error_message      TEXT,
    called_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## API call budget

| # | Call | Populates |
|---|---|---|
| 1 | `GET /repos/{owner}/{repo}` | Most of the presentation/engagement group, `is_fork`, `archived`, `has_issues_enabled`, `primary_language`, timestamps |
| 2 | `GET /users/{owner}` | Account context group |
| 3 | `GET /repos/{owner}/{repo}/commits?per_page=1` (`Link` header) | `commit_count_approx` |
| 4 | `GET /repos/{owner}/{repo}/contributors?per_page=1&anon=true` (`Link` header) | `contributor_count_approx` |
| 5 | `GET /repos/{owner}/{repo}/git/trees/{sha}?recursive=1` | File structure group |

5 calls/repo, funneled through a shared 1 req/sec limiter →
~1,000–1,250 repos/hour throughput ceiling, well within the 5,000/hour
authenticated GitHub rate limit.