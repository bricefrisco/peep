# Peep

Peep is a local, event-driven pipeline that monitors GitHub's public
`/events` firehose, enriches newly-seen repositories with metadata, and
classifies them as **legitimate** or **suspicious** — flagging patterns
consistent with automated abuse (dead-drop / C2-style repos, mass
malicious-link hosting, bot-generated spam repos, and similar campaigns).

It's designed to run continuously on modest local hardware (developed
against a Raspberry Pi 5), with no external services beyond PostgreSQL and
the GitHub REST API.

## Why this exists

GitHub's public event stream surfaces a large number of repositories that
don't look like normal software projects — for example:

- Repos with commit counts in the hundreds of thousands to millions,
  authored by a single account, rewriting a small set of templated files
  over and over (consistent with a rotating dead-drop / C2 resolver
  pattern).
- Repos with generic, randomized names hosting large numbers of files
  containing links, with no real documentation, no engagement (stars,
  forks, issues), and a structurally templated appearance — consistent
  with mass link/malware distribution abusing GitHub's trusted domain.

Peep ingests the event stream, pulls cheap metadata (never file
contents) for each repository it sees, and builds a labeled dataset that a
locally-trained classifier can use to flag repositories matching these
patterns — without relying on any external LLM API, and without reading
file contents at scale.

## Design principles

- **Metadata only.** No repository file contents are ever fetched or
  stored. All features come from lightweight, count-based API responses
  (repo metadata, account metadata, commit/contributor counts via `Link`
  headers, and file *tree* listings — paths and sizes, never blobs).
- **Local-first.** No Docker, no external broker, no cloud dependency.
  PostgreSQL does double duty as both the data store and the job queue.
- **Rate-limit respectful.** All GitHub API calls are funneled through a
  single in-process rate limiter capped at 1 request/second, regardless of
  which stage of the pipeline is making the call.
- **Single process.** All ingestion, enrichment, and queueing logic runs
  as threads within one Python process. See
  [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the reasoning.
- **Human-in-the-loop labeling.** A lightweight local labeling web app is
  used to build the ground-truth dataset a classifier trains on. Model
  predictions and manual labels are tracked as distinct, separately
  auditable columns — model output is never silently treated as ground
  truth.

## How it works, in one paragraph

A poller thread pulls `/events` on a short interval and inserts one job per
event into a Postgres-backed job queue. A `raw_events` worker thread
consumes those jobs, writes each event to an append-only log table, and
fans out five enrichment jobs per newly-seen repository — one per GitHub
endpoint peep depends on. Five enrichment worker threads consume
those jobs (rate-limited to a shared 1 req/sec ceiling), writing results
into a single wide `repos` table. Once all five enrichment calls for a
repo have completed, it becomes eligible for manual labeling and,
eventually, model inference.

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full pipeline design
and [`SCHEMA.md`](./SCHEMA.md) for the database schema and the reasoning
behind each column.

## GitHub API endpoints used

| Endpoint | Purpose |
|---|---|
| `GET /events` | Source stream of public GitHub activity |
| `GET /repos/{owner}/{repo}` | Core repo metadata |
| `GET /users/{owner}` | Account metadata |
| `GET /repos/{owner}/{repo}/commits?per_page=1` | Commit count, via `Link` header |
| `GET /repos/{owner}/{repo}/contributors?per_page=1&anon=true` | Contributor count, via `Link` header |
| `GET /repos/{owner}/{repo}/git/trees/{sha}?recursive=1` | File tree (paths/sizes only) |

All calls require a GitHub **personal access token** (fine-grained,
public-repositories-read-only scope is sufficient) to get the authenticated
rate limit of 5,000 requests/hour rather than the unauthenticated 60/hour.

## Requirements

- Python 3.11+
- PostgreSQL 14+ (for `FOR UPDATE SKIP LOCKED` support and `JSONB`)
- A GitHub personal access token with public-repo read access
- `psycopg2`, `requests`, `python-dotenv` (see `requirements.txt`)

## Running the poller

```
pip install -r requirements.txt
# create the raw_events table — DDL lives in SCHEMA.md

cp poller/.env.example poller/.env
# fill in GITHUB_TOKEN and DATABASE_URL in poller/.env

python -m poller.main
```

This currently runs just the `/events` poller, which batch-writes newly
seen events into `raw_events` (see [`ARCHITECTURE.md`](./ARCHITECTURE.md)
for what's built vs. planned).

## Running the API

```
cp api/.env.example api/.env
# fill in DATABASE_URL in api/.env

python -m uvicorn api.main:app --reload
```

Interactive docs at `/docs`. Current endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /repos/next?labeler=<id>` | Atomically claim exactly one repo for `<id>` to label next (FIFO, or by model confidence once a model exists) |
| `POST /repos/{repo_id}/label` | Submit `{"label": "legitimate"|"suspicious", "labeled_by": "<id>"}`, releasing the claim |

See `SCHEMA.md`'s "Labeling claims" section for why `GET /repos/next`
needs `FOR UPDATE SKIP LOCKED` even though `job_queue` doesn't.

## Project status

Early build — pipeline architecture and schema finalized, initial
ingestion and labeling app in progress. No trained model yet; classifier
work begins once an initial labeled dataset exists.

## Roadmap

- [ ] Ingestion pipeline (poller + 6 worker threads)
- [ ] Labeling web app (Flask/FastAPI + minimal frontend)
- [ ] First trained classifier (logistic regression / GBT) on manually
      labeled data
- [ ] Model confidence feeding back into labeling queue prioritization
      (active learning loop)
- [ ] Optional: multiclass model on the flagged subset (e.g.
      infrastructure-abuse vs. link/malware-hosting) once enough flagged
      examples exist

## Non-goals

- **No file content analysis.** URL/IP extraction and reputation lookups
  from file contents were considered but explicitly deferred — this
  pipeline is metadata-only by design, both for footprint reasons and to
  keep GitHub API usage cheap and fast.
- **No user-level classification.** Early scoping considered
  account/user-level bot detection; the project narrowed to
  **repository-level** legitimacy classification.
- **Not a takedown or enforcement tool.** Peep identifies and scores
  patterns for research/monitoring purposes. It does not report, contact,
  or act against flagged repositories or accounts.

## License

MIT.