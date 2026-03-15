"""
Latency test: fires k requests per second in parallel against each API,
after loading data via /restore_random with custom n. Reports p50, p90, p95, p99, etc.

Usage:
  python test_apis_latency.py              # run all endpoints
  python test_apis_latency.py <name>       # run only endpoint matching <name>
"""

import asyncio
import os
import random
import sys
import statistics
import time
from collections.abc import Callable
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# Global constants (tune these for your run)
# ---------------------------------------------------------------------------
BASE_URL = "http://localhost:8000"
K_REQUESTS_PER_SECOND = 50
RESTORE_N = 1_000_000
TEST_DURATION_SECONDS = 10
MIN_SCORE = 1
MAX_SCORE = 5000

# Percentiles to report
PERCENTILES = (50, 90, 95, 99, 99.9)

# HTML report path (project root, single file for Python + Golang)
REPORT_HTML_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "latency_report.html")
MARKER_PYTHON_START = "<!-- PYTHON-SECTION -->"
MARKER_PYTHON_END = "<!-- END-PYTHON-SECTION -->"
MARKER_GOLANG_START = "<!-- GOLANG-SECTION -->"
MARKER_GOLANG_END = "<!-- END-GOLANG-SECTION -->"
MARKER_INMEM_START = "<!-- INMEM-SECTION -->"
MARKER_INMEM_END = "<!-- END-INMEM-SECTION -->"
MARKER_DIRECT_START = "<!-- DIRECT-SECTION -->"
MARKER_DIRECT_END = "<!-- END-DIRECT-SECTION -->"


def percentile(sorted_latencies_ms: list[float], p: float) -> float:
    """Compute percentile from sorted list of latencies in ms."""
    if not sorted_latencies_ms:
        return 0.0
    k = (len(sorted_latencies_ms) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_latencies_ms) else f
    return sorted_latencies_ms[f] + (k - f) * (sorted_latencies_ms[c] - sorted_latencies_ms[f])


def summarize_latencies(latencies_ms: list[float], name: str) -> dict[str, Any]:
    """Compute min, max, avg and percentiles. Latencies can be empty on failure."""
    if not latencies_ms:
        return {"endpoint": name, "count": 0, "error": "no successful samples"}

    sorted_ms = sorted(latencies_ms)
    n = len(sorted_ms)
    result: dict[str, Any] = {
        "endpoint": name,
        "count": n,
        "min_ms": round(sorted_ms[0], 2),
        "max_ms": round(sorted_ms[-1], 2),
        "avg_ms": round(statistics.mean(sorted_ms), 2),
    }
    if n >= 2:
        result["median_ms"] = round(statistics.median(sorted_ms), 2)
    for p in PERCENTILES:
        result[f"p{p}_ms"] = round(percentile(sorted_ms, p), 2)
    return result


async def run_restore_random(client: httpx.AsyncClient) -> None:
    """POST /restore_random?n=RESTORE_N and wait for success."""
    r = await client.post(f"{BASE_URL}/restore_random", params={"n": RESTORE_N})
    r.raise_for_status()
    print(f"restore_random done: {r.json()}")


async def fire_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    params: dict[str, Any] | None,
    latencies: list[float],
) -> None:
    """Execute one request and append latency in ms to latencies."""
    start = time.perf_counter()
    try:
        if method.upper() == "GET":
            resp = await client.get(url, params=params)
        else:
            resp = await client.post(url, params=params)
        resp.raise_for_status()
    except Exception:
        pass
    else:
        elapsed_ms = (time.perf_counter() - start) * 1000
        latencies.append(elapsed_ms)


async def run_load_test(
    client: httpx.AsyncClient,
    name: str,
    method: str,
    url_builder: Callable[[], tuple[str, dict[str, Any] | None]],
) -> list[float]:
    """
    Sustain K_REQUESTS_PER_SECOND for TEST_DURATION_SECONDS.
    url_builder() returns (url, params) for each request (e.g. random user_id).
    """
    latencies: list[float] = []
    interval = 1.0 / K_REQUESTS_PER_SECOND
    total_requests = int(K_REQUESTS_PER_SECOND * TEST_DURATION_SECONDS)
    tasks: list[asyncio.Task[None]] = []
    start_wall = time.perf_counter()

    for i in range(total_requests):
        now = time.perf_counter() - start_wall
        next_scheduled = i * interval
        if next_scheduled > now:
            await asyncio.sleep(next_scheduled - now)
        url, params = url_builder()
        t = asyncio.create_task(
            fire_request(client, method, url, params, latencies)
        )
        tasks.append(t)

    await asyncio.gather(*tasks)
    return latencies


def make_ping_builder() -> Callable[[], tuple[str, dict[str, Any] | None]]:
    def f() -> tuple[str, dict[str, Any] | None]:
        return (f"{BASE_URL}/ping", None)
    return f


def make_user_score_builder() -> Callable[[], tuple[str, dict[str, Any] | None]]:
    def f() -> tuple[str, dict[str, Any] | None]:
        user_id = str(random.randint(1, RESTORE_N))
        return (f"{BASE_URL}/user/{user_id}/score", None)
    return f


def make_user_rank_builder() -> Callable[[], tuple[str, dict[str, Any] | None]]:
    def f() -> tuple[str, dict[str, Any] | None]:
        user_id = str(random.randint(1, RESTORE_N))
        return (f"{BASE_URL}/user/{user_id}/rank", None)
    return f


def make_score_count_builder() -> Callable[[], tuple[str, dict[str, Any] | None]]:
    def f() -> tuple[str, dict[str, Any] | None]:
        score = random.randint(MIN_SCORE, MAX_SCORE)
        return (f"{BASE_URL}/score/{score}/count", None)
    return f


def make_leaderboard_builder() -> Callable[[], tuple[str, dict[str, Any] | None]]:
    def f() -> tuple[str, dict[str, Any] | None]:
        return (f"{BASE_URL}/leaderboard", None)
    return f


def make_leaderboard_all_builder() -> Callable[[], tuple[str, dict[str, Any] | None]]:
    def f() -> tuple[str, dict[str, Any] | None]:
        return (f"{BASE_URL}/leaderboard/all", None)
    return f


def make_leaderboard_rank_range_builder() -> Callable[[], tuple[str, dict[str, Any] | None]]:
    """GET /leaderboard?min_rank=&max_rank= (dynamic per request)."""
    def f() -> tuple[str, dict[str, Any] | None]:
        min_rank = random.randint(1, max(1, RESTORE_N))
        max_rank = random.randint(min_rank, RESTORE_N)
        return (f"{BASE_URL}/leaderboard", {"min_rank": min_rank, "max_rank": max_rank})
    return f


def make_leaderboard_score_range_builder() -> Callable[[], tuple[str, dict[str, Any] | None]]:
    """GET /leaderboard?min_score=&max_score= (dynamic per request)."""
    def f() -> tuple[str, dict[str, Any] | None]:
        min_score = random.randint(MIN_SCORE, MAX_SCORE)
        max_score = random.randint(min_score, MAX_SCORE)
        return (f"{BASE_URL}/leaderboard", {"min_score": min_score, "max_score": max_score})
    return f


def get_all_test_cases() -> list[tuple[str, str, Callable[[], tuple[str, dict[str, Any] | None]]]]:
    """Return (display_name, method, url_builder) for every endpoint."""
    return [
        ("GET /ping", "GET", make_ping_builder()),
        ("GET /user/{id}/score", "GET", make_user_score_builder()),
        ("GET /user/{id}/rank", "GET", make_user_rank_builder()),
        ("GET /score/{score}/count", "GET", make_score_count_builder()),
        ("GET /leaderboard", "GET", make_leaderboard_builder()),
        ("GET /leaderboard?min_rank=&max_rank= (dynamic)", "GET", make_leaderboard_rank_range_builder()),
        ("GET /leaderboard?min_score=&max_score= (dynamic)", "GET", make_leaderboard_score_range_builder()),
        ("GET /leaderboard/all", "GET", make_leaderboard_all_builder()),
    ]


def filter_test_cases_by_name(
    test_cases: list[tuple[str, str, Callable[[], tuple[str, dict[str, Any] | None]]]],
    name: str,
) -> list[tuple[str, str, Callable[[], tuple[str, dict[str, Any] | None]]]]:
    """Return test cases whose display name contains name (case-insensitive)."""
    name_lower = name.strip().lower()
    matched = [tc for tc in test_cases if name_lower in tc[0].lower()]
    return matched


async def main() -> None:
    all_cases = get_all_test_cases()
    if len(sys.argv) > 1:
        filter_arg = sys.argv[1]
        test_cases = filter_test_cases_by_name(all_cases, filter_arg)
        if not test_cases:
            print(f"No endpoint matching '{filter_arg}'.")
            print("Available endpoints:")
            for name, _, _ in all_cases:
                print(f"  {name}")
            sys.exit(1)
        print(f"Testing only: {[tc[0] for tc in test_cases]}")
    else:
        test_cases = all_cases

    print(f"BASE_URL={BASE_URL} K_RPS={K_REQUESTS_PER_SECOND} RESTORE_N={RESTORE_N} DURATION={TEST_DURATION_SECONDS}s")
    print("Calling /restore_random ...")
    async with httpx.AsyncClient(timeout=60.0) as client:
        await run_restore_random(client)
    print("Load testing each endpoint ...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        results: list[dict[str, Any]] = []
        for name, method, url_builder in test_cases:
            latencies = await run_load_test(client, name, method, url_builder)
            results.append(summarize_latencies(latencies, name))

    print("\n--- Latency summary (ms) ---")
    for r in results:
        if r.get("error"):
            print(f"  {r['endpoint']}: {r['error']}")
            continue
        parts = [f"  {r['endpoint']}: n={r['count']} min={r['min_ms']} max={r['max_ms']} avg={r['avg_ms']}"]
        for p in PERCENTILES:
            parts.append(f"p{p}={r[f'p{p}_ms']}")
        print(" ".join(parts))

    write_html_report(results)


def _read_golang_section() -> str:
    """Read existing report and return content between GOLANG markers, or placeholder."""
    try:
        with open(REPORT_HTML_PATH) as f:
            s = f.read()
    except FileNotFoundError:
        return '<p class="meta">Run: <code>cd go &amp;&amp; go run ./cmd/latency</code></p>'
    i = s.find(MARKER_GOLANG_START)
    j = s.find(MARKER_GOLANG_END)
    if i == -1 or j == -1 or j <= i:
        return '<p class="meta">Run: <code>cd go &amp;&amp; go run ./cmd/latency</code></p>'
    return s[i + len(MARKER_GOLANG_START) : j].strip() or '<p class="meta">Run: <code>cd go &amp;&amp; go run ./cmd/latency</code></p>'


def _read_inmem_section() -> str:
    """Read existing report and return content between INMEM markers, or placeholder."""
    try:
        with open(REPORT_HTML_PATH) as f:
            s = f.read()
    except FileNotFoundError:
        return '<p class="meta">Run: <code>cd go &amp;&amp; go run ./cmd/latency_inmem</code></p>'
    i = s.find(MARKER_INMEM_START)
    j = s.find(MARKER_INMEM_END)
    if i == -1 or j == -1 or j <= i:
        return '<p class="meta">Run: <code>cd go &amp;&amp; go run ./cmd/latency_inmem</code></p>'
    return s[i + len(MARKER_INMEM_START) : j].strip() or '<p class="meta">Run: <code>cd go &amp;&amp; go run ./cmd/latency_inmem</code></p>'


def _read_direct_section() -> str:
    """Read existing report and return content between DIRECT markers, or placeholder."""
    try:
        with open(REPORT_HTML_PATH) as f:
            s = f.read()
    except FileNotFoundError:
        return '<p class="meta">Run: <code>cd go-fenwick-based-in-memory &amp;&amp; go run ./cmd/latency_direct</code></p>'
    i = s.find(MARKER_DIRECT_START)
    j = s.find(MARKER_DIRECT_END)
    if i == -1 or j == -1 or j <= i:
        return '<p class="meta">Run: <code>cd go-fenwick-based-in-memory &amp;&amp; go run ./cmd/latency_direct</code></p>'
    return s[i + len(MARKER_DIRECT_START) : j].strip() or '<p class="meta">Run: <code>cd go-fenwick-based-in-memory &amp;&amp; go run ./cmd/latency_direct</code></p>'


def write_html_report(results: list[dict[str, Any]]) -> None:
    """Write latency results to shared HTML (Python section); preserve Golang section."""
    rows: list[str] = []
    for r in results:
        if r.get("error"):
            rows.append(
                f"    <tr><td>{_esc(r['endpoint'])}</td><td colspan=\"10\">{_esc(r['error'])}</td></tr>"
            )
            continue
        median = r.get("median_ms", r.get("p50_ms", "-"))
        rows.append(
            "    <tr>"
            f"<td>{_esc(r['endpoint'])}</td>"
            f"<td>{r['count']}</td>"
            f"<td>{r['min_ms']}</td>"
            f"<td>{r['max_ms']}</td>"
            f"<td>{r['avg_ms']}</td>"
            f"<td>{median}</td>"
            + "".join(f"<td>{r[f'p{p}_ms']}</td>" for p in PERCENTILES)
            + "</tr>"
        )

    python_body = f"""  <p class="meta">BASE_URL={_esc(BASE_URL)} | K_RPS={K_REQUESTS_PER_SECOND} | RESTORE_N={RESTORE_N} | DURATION={TEST_DURATION_SECONDS}s</p>
  <table>
    <thead>
      <tr>
        <th>Endpoint</th>
        <th>Count</th>
        <th>Min (ms)</th>
        <th>Max (ms)</th>
        <th>Avg (ms)</th>
        <th>Median (ms)</th>
        <th>p50 (ms)</th>
        <th>p90 (ms)</th>
        <th>p95 (ms)</th>
        <th>p99 (ms)</th>
        <th>p99.9 (ms)</th>
      </tr>
    </thead>
    <tbody>
{chr(10).join(rows)}
    </tbody>
  </table>"""
    golang_body = _read_golang_section()
    inmem_body = _read_inmem_section()
    direct_body = _read_direct_section()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>API Latency Report</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; background: #1a1a1a; color: #e0e0e0; }}
    h1 {{ color: #fff; margin-bottom: 0.5rem; }}
    h2 {{ color: #ccc; margin-top: 2rem; margin-bottom: 0.5rem; font-size: 1.2rem; }}
    .meta {{ color: #888; margin-bottom: 1rem; font-size: 0.9rem; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 1200px; }}
    th, td {{ border: 1px solid #444; padding: 0.6rem 0.8rem; text-align: left; }}
    th {{ background: #2d2d2d; color: #fff; font-weight: 600; }}
    tr:nth-child(even) {{ background: #252525; }}
    tr:hover {{ background: #333; }}
    td:nth-child(n+2) {{ text-align: right; font-variant-numeric: tabular-nums; }}
    td:first-child {{ max-width: 400px; word-break: break-all; }}
  </style>
</head>
<body>
  <h1>API Latency Report</h1>
  <h2>Python</h2>
{MARKER_PYTHON_START}
{python_body}
{MARKER_PYTHON_END}
  <h2>Golang</h2>
{MARKER_GOLANG_START}
{golang_body}
{MARKER_GOLANG_END}
  <h2>In-Memory (Fenwick)</h2>
{MARKER_INMEM_START}
{inmem_body}
{MARKER_INMEM_END}
  <h2>Direct (Fenwick)</h2>
{MARKER_DIRECT_START}
{direct_body}
{MARKER_DIRECT_END}
""" + _chart_and_comparison_footer()
    with open(REPORT_HTML_PATH, "w") as f:
        f.write(html)
    print(f"\nReport written to {REPORT_HTML_PATH}")


def _chart_and_comparison_footer() -> str:
    """Return HTML for Avg(ms) chart and % performance comparison table (client-side script)."""
    return r'''
  <h2 style="margin-top: 3rem;">Avg (ms) by endpoint – comparison</h2>
  <div id="chart-container" style="height: 420px; max-width: 1200px; margin: 1.5rem 0;">
    <canvas id="avg-chart"></canvas>
  </div>
  <h2 style="margin-top: 2rem;">Performance comparison (% improvement)</h2>
  <p class="meta">Positive % = faster than baseline. Baseline: Python (Redis).</p>
  <div id="comparison-table-container"></div>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <script>
(function() {
  var h2s = document.querySelectorAll("body h2");
  var sectionNames = ["Python", "Golang", "In-Memory (Fenwick)", "Direct (Fenwick)"];
  var tables = [];
  for (var i = 0; i < h2s.length; i++) {
    var name = h2s[i].textContent.trim();
    if (sectionNames.indexOf(name) === -1) continue;
    var el = h2s[i].nextElementSibling;
    while (el && el.tagName !== "TABLE") { el = el.nextElementSibling; }
    if (!el || el.tagName !== "TABLE") continue;
    tables.push({ name: name, table: el });
  }
  if (tables.length === 0) return;
  var dataBySection = [{}, {}, {}, {}];
  var sectionIndex = { "Python": 0, "Golang": 1, "In-Memory (Fenwick)": 2, "Direct (Fenwick)": 3 };
  tables.forEach(function(t) {
    var idx = sectionIndex[t.name];
    if (idx === undefined) return;
    var rows = t.table.querySelectorAll("tbody tr");
    rows.forEach(function(tr) {
      var tds = tr.querySelectorAll("td");
      if (tds.length < 5) return;
      var endpoint = tds[0].textContent.trim();
      var avg = parseFloat(tds[4].textContent.trim());
      if (isNaN(avg)) return;
      dataBySection[idx][endpoint] = avg;
    });
  });
  var allEndpoints = [];
  var seen = {};
  [0,1,2,3].forEach(function(i) { Object.keys(dataBySection[i] || {}).forEach(function(e) { if (!seen[e]) { seen[e] = 1; allEndpoints.push(e); } }); });
  var ctx = document.getElementById("avg-chart");
  if (!ctx) return;
  ctx = ctx.getContext("2d");
  var colors = ["#e74c3c", "#3498db", "#2ecc71", "#f39c12"];
  var datasets = [
    { label: "Python (Redis)", data: allEndpoints.map(function(e) { return dataBySection[0][e] != null ? dataBySection[0][e] : null; }), backgroundColor: colors[0] },
    { label: "Go (Redis)", data: allEndpoints.map(function(e) { return dataBySection[1][e] != null ? dataBySection[1][e] : null; }), backgroundColor: colors[1] },
    { label: "Go (Fenwick in-memory)", data: allEndpoints.map(function(e) { return dataBySection[2][e] != null ? dataBySection[2][e] : null; }), backgroundColor: colors[2] },
    { label: "Direct (Fenwick)", data: allEndpoints.map(function(e) { return dataBySection[3][e] != null ? dataBySection[3][e] : null; }), backgroundColor: colors[3] }
  ];
  new Chart(ctx, {
    type: "bar",
    data: { labels: allEndpoints, datasets: datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: { y: { beginAtZero: true, title: { display: true, text: "Avg (ms)" } } },
      plugins: { title: { display: true, text: "Average latency by endpoint" } }
    }
  });
  var py = dataBySection[0], go = dataBySection[1], fen = dataBySection[2], dir = dataBySection[3];
  var tableHtml = "<table><thead><tr><th>Endpoint</th><th>Python (Redis) ms</th><th>Go (Redis) ms</th><th>Go (Fenwick) ms</th><th>Direct (Fenwick) ms</th><th>Go-Redis vs Python</th><th>Go-Fenwick vs Python</th><th>Direct vs Python</th><th>Direct vs Go-Fenwick</th></tr></thead><tbody>";
  allEndpoints.forEach(function(ep) {
    var p = py[ep], g = go[ep], f = fen[ep], d = dir[ep];
    var cell = function(v) { return v != null ? v.toFixed(2) : "-"; };
    var pct = function(base, x) { if (base == null || x == null || base <= 0) return "-"; return ((base - x) / base * 100).toFixed(1) + "%"; };
    tableHtml += "<tr><td>" + ep + "</td><td>" + cell(p) + "</td><td>" + cell(g) + "</td><td>" + cell(f) + "</td><td>" + cell(d) + "</td><td>" + pct(p, g) + "</td><td>" + pct(p, f) + "</td><td>" + pct(p, d) + "</td><td>" + pct(f, d) + "</td></tr>";
  });
  tableHtml += "</tbody></table>";
  document.getElementById("comparison-table-container").innerHTML = tableHtml;
})();
  </script>
</body>
</html>
'''


def _esc(s: Any) -> str:
    """Escape for HTML."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


if __name__ == "__main__":
    asyncio.run(main())
