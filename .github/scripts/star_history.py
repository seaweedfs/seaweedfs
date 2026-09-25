#!/usr/bin/env python3
"""Render the repository's star history to note/star_history.svg.

Uses the GitHub REST stargazers endpoint with the starred-at accept header,
which caps at 40,000 entries (400 pages of 100). The chart is regenerated on
a schedule; if the repo grows past that cap the script stops at 40,000 and
logs a warning rather than under-reporting.
"""
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.dates import AutoDateLocator, DateFormatter  # noqa: E402

REPO = "seaweedfs/seaweedfs"
TOKEN = os.environ["GITHUB_TOKEN"]
OUT = os.environ.get("OUT", "note/star_history.svg")
PAGE_CAP = 400  # GitHub's hard limit on stargazer pagination
MAX_ATTEMPTS = 10


def fetch_page(url):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github.star+json",
                "Authorization": f"Bearer {TOKEN}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "seaweedfs-star-history",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            # Retry transient server errors and (secondary) rate limits.
            retryable = e.code >= 500 or e.code in (403, 429)
            if not retryable or attempt == MAX_ATTEMPTS:
                raise
            try:
                delay = int(e.headers.get("Retry-After"))
            except (TypeError, ValueError):
                delay = min(2 ** attempt, 60)
            print(
                f"::warning::GET {url} failed with HTTP {e.code} "
                f"(attempt {attempt}/{MAX_ATTEMPTS}); retrying in {delay}s"
            )
            time.sleep(delay + random.uniform(0, 1))
        except (urllib.error.URLError, OSError) as e:
            # OSError also covers bare http.client.HTTPException subclasses
            # (e.g. RemoteDisconnected), which urllib lets propagate
            # unwrapped instead of raising URLError.
            if attempt == MAX_ATTEMPTS:
                raise
            delay = min(2 ** attempt, 60)
            print(
                f"::warning::GET {url} failed with {e} "
                f"(attempt {attempt}/{MAX_ATTEMPTS}); retrying in {delay}s"
            )
            time.sleep(delay + random.uniform(0, 1))


def fetch_stargazers():
    stars = []
    page = 1
    while page <= PAGE_CAP:
        url = f"https://api.github.com/repos/{REPO}/stargazers?per_page=100&page={page}"
        batch = fetch_page(url)
        if not batch:
            break
        for u in batch:
            sa = u.get("starred_at")
            if sa:
                stars.append(datetime.fromisoformat(sa.replace("Z", "+00:00")))
        if len(batch) < 100:
            break
        page += 1
    if page > PAGE_CAP:
        print(
            f"::warning::Hit the {PAGE_CAP}-page stargazer pagination cap; "
            "chart reflects the first 40,000 stars only."
        )
    return stars


def render(stars, out):
    stars.sort()
    counts = list(range(1, len(stars) + 1))
    fig, ax = plt.subplots(figsize=(10, 6), dpi=130)
    ax.plot(stars, counts, color="#0969da", linewidth=1.6)
    ax.set_xlabel("Date")
    ax.set_ylabel("Stars")
    ax.set_title(f"{REPO} star history")
    ax.grid(True, linestyle="--", alpha=0.3)
    ax.xaxis.set_major_locator(AutoDateLocator())
    ax.xaxis.set_major_formatter(DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out, format="svg", transparent=False)
    plt.close(fig)


def main():
    stars = fetch_stargazers()
    if not stars:
        print("::error::No stargazers fetched; not updating the chart.")
        sys.exit(1)
    render(stars, OUT)
    print(f"Rendered {len(stars)} stars to {OUT}")


if __name__ == "__main__":
    main()
