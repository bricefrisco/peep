import requests

API_ROOT = "https://api.github.com"
EVENTS_URL = f"{API_ROOT}/events"
DEFAULT_POLL_INTERVAL = 60
PER_PAGE = 100


def build_session(token):
    """A requests.Session authenticated against the GitHub API.

    Sessions aren't thread-safe to share -- each thread making API calls
    (the poller, each enrichment worker) should get its own.
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    return session


class GitHubEventsClient:
    """Thin wrapper around GET /events with ETag-based conditional polling.

    GitHub returns 304 Not Modified (no body, doesn't count against the
    rate limit) when nothing's changed since the last poll, and tells us
    how long to wait before polling again via X-Poll-Interval.
    """

    def __init__(self, token, session=None):
        self.session = session or build_session(token)
        self._etag = None

    def poll(self):
        """Fetch the latest page of public events.

        Returns (events, poll_interval_seconds). `events` is [] on a 304.
        """
        headers = {"If-None-Match": self._etag} if self._etag else {}
        resp = self.session.get(
            EVENTS_URL, params={"per_page": PER_PAGE}, headers=headers, timeout=10
        )

        poll_interval = int(resp.headers.get("X-Poll-Interval", DEFAULT_POLL_INTERVAL))

        if resp.status_code == 304:
            return [], poll_interval

        resp.raise_for_status()
        self._etag = resp.headers.get("ETag")
        return resp.json(), poll_interval
