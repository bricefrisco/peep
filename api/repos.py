from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

from .db import get_db

router = APIRouter()

# Mirrors SCHEMA.md's labeling-queue view -- served directly by the
# idx_repos_labeling_queue partial index.
LABELING_QUEUE_WHERE = """
    repos_call_done AND users_call_done AND commits_call_done
    AND contributors_call_done AND tree_call_done
    AND manual_label IS NULL
"""

# How long a claim (see GET /repos/next) reserves a repo for one labeler
# before it's treated as abandoned and released back into the pool.
CLAIM_TTL = "10 minutes"


@router.get("/repos/next", status_code=200)
def next_repo(labeler: str, response: Response, cur=Depends(get_db)):
    """Atomically claim exactly one repo for `labeler` to review next.

    Priority: FIFO (oldest-ready-first) until a model exists, then
    ORDER BY ABS(model_confidence - 0.5) ASC once one does -- see
    SCHEMA.md's "Label priority". FOR UPDATE SKIP LOCKED guarantees two
    concurrent callers never get handed the same repo; the claim expires
    after CLAIM_TTL so an abandoned repo (labeler closed the tab) falls
    back into the pool instead of being stuck forever.
    """
    cur.execute(
        f"""
        SELECT repo_id FROM repos
        WHERE {LABELING_QUEUE_WHERE}
          AND (claimed_by IS NULL OR claimed_at < now() - interval '{CLAIM_TTL}')
        ORDER BY fetched_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        """
    )
    row = cur.fetchone()
    if row is None:
        response.status_code = 204
        return None

    cur.execute(
        """
        UPDATE repos SET claimed_by = %s, claimed_at = now()
        WHERE repo_id = %s
        RETURNING *
        """,
        (labeler, row["repo_id"]),
    )
    return cur.fetchone()


class LabelSubmission(BaseModel):
    label: Literal["legitimate", "suspicious"]
    labeled_by: str


@router.post("/repos/{repo_id}/label")
def submit_label(repo_id: int, submission: LabelSubmission, cur=Depends(get_db)):
    """Record a manual label and release any claim on this repo.

    Scoped to `manual_label IS NULL` so a repo can never be labeled twice --
    defense in depth on top of the claim mechanism in GET /repos/next,
    which is what actually prevents two labelers from working the same
    repo concurrently in the first place.
    """
    cur.execute(
        """
        UPDATE repos
        SET manual_label = %s, manual_label_by = %s, manual_labeled_at = now(),
            claimed_by = NULL, claimed_at = NULL
        WHERE repo_id = %s AND manual_label IS NULL
        RETURNING repo_id
        """,
        (submission.label, submission.labeled_by, repo_id),
    )
    if cur.fetchone() is None:
        raise HTTPException(status_code=409, detail="repo not found, or already labeled")
    return {"repo_id": repo_id, "label": submission.label}
