#!/usr/bin/env python3
"""Render the repository's star history to note/star_history.svg.

Uses the GitHub REST stargazers endpoint with the starred-at accept header,
which caps at 40,000 entries (400 pages of 100). The chart is regenerated on
a schedule; if the repo grows past that cap the script stops at 40,000 and
logs a warning rather than under-reporting.
"""
import json
import os
import sys
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


def fetch_stargazers():
    stars = []
    page = 1
    while page <= PAGE_CAP:
        url = f"https://api.github.com/repos/{REPO}/stargazers?per_page=100&page={page}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github.star+json",
                "Authorization": f"Bearer {TOKEN}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "seaweedfs-star-history",
            },
        )
        with urllib.request.urlopen(req) as resp:
            batch = json.load(resp)
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
    fig, ax = plt.subplots(figsize=(10, 4), dpi=130)
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
