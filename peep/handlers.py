import logging
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

from . import db
from .github_client import API_ROOT

logger = logging.getLogger(__name__)

# Statuses GitHub returns for repos with no commits yet (freshly created,
# still-empty repos are common in the firehose) -- treat as zero, not a
# retryable failure.
EMPTY_REPO_STATUSES = (404, 409)


def _get(session, url, params=None):
    resp = session.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp


def _is_rate_limit_response(resp):
    """Distinguish a transient rate-limit 403 (primary quota exhausted, or
    GitHub's secondary/abuse-detection limiter) from a 403 that GitHub
    returns for a specific repo/resource and will return again on retry
    (e.g. a repo restricted by GitHub's own abuse/policy systems)."""
    if resp.headers.get("Retry-After") is not None:
        return True
    return resp.headers.get("X-RateLimit-Remaining") == "0"


def _parse_ts(value):
    if value is None:
        return None
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value  # already a tz-aware datetime, read back from Postgres


def _last_page_count(link_header):
    """Approximate a count from the `last` rel of a paginated response's
    Link header (per_page=1, so the last page number == the count)."""
    if not link_header:
        return None
    for part in link_header.split(","):
        segment = part.split(";")
        if len(segment) < 2 or 'rel="last"' not in segment[1]:
            continue
        qs = parse_qs(urlparse(segment[0].strip(" <>")).query)
        return int(qs.get("page", ["1"])[0])
    return None


def _approx_count(resp):
    count = _last_page_count(resp.headers.get("Link"))
    if count is not None:
        return count
    return len(resp.json())


def compute_derived_fields(merged):
    """Cross-call features -- computed once both required source columns
    are available, regardless of which handler ran second."""
    derived = {}

    created_at = merged.get("created_at")
    account_created_at = merged.get("account_created_at")
    if created_at is not None and account_created_at is not None:
        derived["account_age_days_at_repo_creation"] = (
            _parse_ts(created_at) - _parse_ts(account_created_at)
        ).days

    commit_count_approx = merged.get("commit_count_approx")
    repo_age_days = merged.get("repo_age_days")
    if commit_count_approx is not None and repo_age_days is not None:
        derived["commits_per_day"] = commit_count_approx / max(repo_age_days, 1)

    return derived


def handle_repos_call(session, conn, owner, repo, repo_row):
    data = _get(session, f"{API_ROOT}/repos/{owner}/{repo}").json()

    created_at = data.get("created_at")
    repo_age_days = None
    if created_at:
        repo_age_days = (datetime.now(timezone.utc) - _parse_ts(created_at)).days

    description = data.get("description") or ""
    sibling_count = max(db.count_repos_by_owner(conn, owner) - 1, 0)

    return {
        "created_at": created_at,
        "pushed_at": data.get("pushed_at"),
        "default_branch": data.get("default_branch"),
        "fetched_at": datetime.now(timezone.utc),
        "repo_age_days": repo_age_days,
        "size_kb": data.get("size"),
        "is_fork": data.get("fork"),
        "archived": data.get("archived"),
        "has_issues_enabled": data.get("has_issues"),
        "primary_language": data.get("language"),
        "description_present": bool(description),
        "description_length": len(description),
        "topics_count": len(data.get("topics") or []),
        "license_present": data.get("license") is not None,
        "stars": data.get("stargazers_count"),
        "forks": data.get("forks_count"),
        "watchers": data.get("watchers_count"),
        "open_issues_count": data.get("open_issues_count"),
        "sibling_repo_count_same_account": sibling_count,
    }


def handle_users_call(session, conn, owner, repo, repo_row):
    data = _get(session, f"{API_ROOT}/users/{owner}").json()
    return {
        "account_created_at": data.get("created_at"),
        "account_public_repos": data.get("public_repos"),
        "account_followers": data.get("followers"),
        "account_following": data.get("following"),
        "account_type": data.get("type"),
    }


def handle_commits_call(session, conn, owner, repo, repo_row):
    try:
        resp = _get(
            session,
            f"{API_ROOT}/repos/{owner}/{repo}/commits",
            params={"per_page": 1},
        )
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in EMPTY_REPO_STATUSES:
            return {"commit_count_approx": 0}
        raise
    return {"commit_count_approx": _approx_count(resp)}


def handle_contributors_call(session, conn, owner, repo, repo_row):
    try:
        resp = _get(
            session,
            f"{API_ROOT}/repos/{owner}/{repo}/contributors",
            params={"per_page": 1, "anon": "true"},
        )
    except Exception as exc:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if status in EMPTY_REPO_STATUSES:
            return {"contributor_count_approx": 0}
        if status == 403 and response is not None and not _is_rate_limit_response(response):
            logger.warning(
                "contributors call for %s/%s got a non-rate-limit 403 -- "
                "treating as permanently restricted, not retrying",
                owner, repo,
            )
            return {"contributor_count_approx": None}
        raise
    return {"contributor_count_approx": _approx_count(resp)}


def _extension(path):
    name = path.rsplit("/", 1)[-1]
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1].lower()


def handle_tree_call(session, conn, owner, repo, repo_row):
    default_branch = (repo_row or {}).get("default_branch")
    if not default_branch:
        raise RuntimeError("default_branch not yet known -- waiting on the repos call")

    try:
        resp = _get(
            session,
            f"{API_ROOT}/repos/{owner}/{repo}/git/trees/{default_branch}",
            params={"recursive": "1"},
        )
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in EMPTY_REPO_STATUSES:
            return {
                "file_count": 0,
                "tree_truncated": False,
                "extension_count_distinct": 0,
                "top_extension": None,
                "extension_homogeneity_ratio": None,
                "avg_path_length": None,
            }
        raise

    data = resp.json()
    blobs = [t for t in data.get("tree", []) if t.get("type") == "blob"]
    file_count = len(blobs)

    ext_counts = {}
    for blob in blobs:
        ext = _extension(blob["path"])
        ext_counts[ext] = ext_counts.get(ext, 0) + 1
    top_extension, top_count = (
        max(ext_counts.items(), key=lambda kv: kv[1]) if ext_counts else (None, 0)
    )

    return {
        "file_count": file_count,
        "tree_truncated": bool(data.get("truncated")),
        "extension_count_distinct": len(ext_counts),
        "top_extension": top_extension,
        "extension_homogeneity_ratio": (top_count / file_count) if file_count else None,
        "avg_path_length": (
            sum(len(b["path"]) for b in blobs) / file_count if file_count else None
        ),
        # filename_template_score intentionally left null here -- needs a
        # local regex/edit-distance scorer that hasn't been built yet.
    }
