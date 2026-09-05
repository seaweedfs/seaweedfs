#!/usr/bin/env python3
"""Generate a star history SVG chart for a GitHub repo using the GitHub API.

Samples stargazers (with starred_at timestamps) and renders a taller
SVG chart so it looks good embedded in a README.

Usage:
    GITHUB_TOKEN=... python3 scripts/generate_star_history.py [owner/repo] [output.svg]

Defaults to seaweedfs/seaweedfs and note/star-history.svg.
"""
import json
import math
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from xml.sax.saxutils import escape

REPO = sys.argv[1] if len(sys.argv) > 1 else "seaweedfs/seaweedfs"
OUT = sys.argv[2] if len(sys.argv) > 2 else "note/star-history.svg"
TOKEN = os.environ.get("GITHUB_TOKEN", "")

# Chart dimensions (4:3 -> taller than the usual 2.5:1)
W, H = 800, 600
PAD_L, PAD_R, PAD_T, PAD_B = 60, 30, 40, 50

BG = "#ffffff"
AXIS = "#333333"
LINE = "#6b63ff"
GRID = "#e0e0e0"
TEXT = "#333333"


def gh_api(path):
    """Call GitHub API, returning parsed JSON."""
    url = f"https://api.github.com/{path}"
    req = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github.star+json",
        "Authorization": f"Bearer {TOKEN}",
    }) if TOKEN else urllib.request.Request(url, headers={
        "Accept": "application/vnd.github.star+json",
    })
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def fetch_stargazers():
    """Sample stargazers across pages to build a growth curve."""
    total = gh_api(f"repos/{REPO}")["stargazers_count"]
    per_page = 100
    total_pages = math.ceil(total / per_page)

    # Sample at most ~60 pages evenly across the full range
    max_pages = 60
    if total_pages <= max_pages:
        pages = list(range(1, total_pages + 1))
    else:
        step = total_pages / max_pages
        pages = sorted(set(int(round(i * step)) for i in range(max_pages + 1) if 0 < i * step <= total_pages))
        if 1 not in pages:
            pages.insert(0, 1)
        if total_pages not in pages:
            pages.append(total_pages)

    points = []  # (timestamp, star_count)
    for p in pages:
        for attempt in range(3):
            try:
                data = gh_api(f"repos/{REPO}/stargazers?per_page={per_page}&page={p}")
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  page {p}: failed after retries: {e}", file=sys.stderr)
                    data = None
                    break
                time.sleep(5)
        if not data:
            continue
        # star count at this page = (page-1)*per_page + 1 (first star on the page)
        count = (p - 1) * per_page + 1
        # use the first entry's timestamp on the page
        ts = data[0]["starred_at"]
        points.append((ts, count))
        print(f"  page {p}/{total_pages}: {ts} -> {count} stars", file=sys.stderr)
        time.sleep(0.3)  # be gentle

    # add final point = now, total stars
    points.append((datetime.now(timezone.utc).isoformat(), total))

    # deduplicate & sort by timestamp
    seen = {}
    for ts, cnt in points:
        # keep the max count for a given date
        k = ts[:10]
        seen[k] = max(seen.get(k, 0), cnt)
    sorted_pts = sorted(seen.items())
    return [(k, v) for k, v in sorted_pts]


def parse_date(s):
    return datetime.strptime(s[:10], "%Y-%m-%d")


def generate_svg(points):
    if len(points) < 2:
        return f'<svg width="{W}" height="{H}"><text>Not enough data</text></svg>'

    dates = [parse_date(p[0]) for p in points]
    counts = [p[1] for p in points]

    min_d = dates[0]
    max_d = dates[-1]
    min_c = 0
    max_c = max(counts) * 1.05

    d_range = (max_d - min_d).total_seconds()
    if d_range == 0:
        d_range = 1

    plot_x0 = PAD_L
    plot_y0 = PAD_T
    plot_w = W - PAD_L - PAD_R
    plot_h = H - PAD_T - PAD_B

    def to_x(d):
        return plot_x0 + (d - min_d).total_seconds() / d_range * plot_w

    def to_y(c):
        return plot_y0 + plot_h - (c - min_c) / (max_c - min_c) * plot_h

    # Build polyline points
    pts = " ".join(f"{to_x(d):.1f},{to_y(c):.1f}" for d, c in zip(dates, counts))

    # Y-axis ticks (5 steps)
    y_ticks = []
    for i in range(6):
        val = min_c + (max_c - min_c) * i / 5
        y = to_y(val)
        label = f"{int(val):,}"
        y_ticks.append((y, label, val))

    # X-axis ticks (yearly)
    x_ticks = []
    year = min_d.year
    while year <= max_d.year:
        d = datetime(year, 1, 1)
        if min_d <= d <= max_d:
            x = to_x(d)
            x_ticks.append((x, str(year)))
        year += 1

    # Grid lines (horizontal)
    grid_lines = "\n".join(
        f'    <line x1="{plot_x0}" y1="{y:.1f}" x2="{plot_x0 + plot_w}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>'
        for y, _, _ in y_ticks
    )

    # Y-axis labels
    y_labels = "\n".join(
        f'    <text x="{plot_x0 - 10}" y="{y + 4:.1f}" text-anchor="end" font-size="12" fill="{TEXT}">{escape(label)}</text>'
        for y, label, _ in y_ticks
    )

    # X-axis labels
    x_labels = "\n".join(
        f'    <text x="{x:.1f}" y="{plot_y0 + plot_h + 25}" text-anchor="middle" font-size="12" fill="{TEXT}">{escape(label)}</text>'
        for x, label in x_ticks
    )

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;">
  <rect width="{W}" height="{H}" fill="{BG}" rx="8"/>
  <text x="{W/2}" y="25" text-anchor="middle" font-size="16" font-weight="bold" fill="{TEXT}">Stargazers over time</text>
{grid_lines}
  <polyline points="{pts}" fill="none" stroke="{LINE}" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
  <line x1="{plot_x0}" y1="{plot_y0}" x2="{plot_x0}" y2="{plot_y0 + plot_h}" stroke="{AXIS}" stroke-width="1.5"/>
  <line x1="{plot_x0}" y1="{plot_y0 + plot_h}" x2="{plot_x0 + plot_w}" y2="{plot_y0 + plot_h}" stroke="{AXIS}" stroke-width="1.5"/>
{y_labels}
{x_labels}
</svg>'''
    return svg


def main():
    print(f"Fetching stargazers for {REPO}...", file=sys.stderr)
    points = fetch_stargazers()
    print(f"Got {len(points)} data points", file=sys.stderr)
    svg = generate_svg(points)
    os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
    with open(OUT, "w") as f:
        f.write(svg)
    print(f"Wrote {OUT} ({len(svg)} bytes)", file=sys.stderr)


if __name__ == "__main__":
    main()
