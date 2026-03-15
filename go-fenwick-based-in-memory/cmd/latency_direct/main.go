// Direct-call latency test for Fenwick store (no HTTP). Uses same RPS/duration as API tests.
// Writes results into the shared latency_report.html under section "Direct (Fenwick)".
//
// Usage:
//
//	go run ./cmd/latency_direct              # all endpoints
//	go run ./cmd/latency_direct ping        # only matching
package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"leaderboard-fenwick/store"
)

var (
	kRequestsPerSecond    = 50
	restoreN              = 1000000
	testDurationSeconds   = 10
	minScore              = store.MinScore
	maxScore              = store.MaxScore
	reportHTMLPath        = "latency_report.html"
)

const (
	markerPythonStart  = "<!-- PYTHON-SECTION -->"
	markerPythonEnd    = "<!-- END-PYTHON-SECTION -->"
	markerGolangStart  = "<!-- GOLANG-SECTION -->"
	markerGolangEnd    = "<!-- END-GOLANG-SECTION -->"
	markerInmemStart   = "<!-- INMEM-SECTION -->"
	markerInmemEnd     = "<!-- END-INMEM-SECTION -->"
	markerDirectStart  = "<!-- DIRECT-SECTION -->"
	markerDirectEnd    = "<!-- END-DIRECT-SECTION -->"
)

func init() {
	if v := os.Getenv("K_REQUESTS_PER_SECOND"); v != "" {
		if i, _ := strconv.Atoi(v); i > 0 {
			kRequestsPerSecond = i
		}
	}
	if v := os.Getenv("RESTORE_N"); v != "" {
		if i, _ := strconv.Atoi(v); i > 0 {
			restoreN = i
		}
	}
	if v := os.Getenv("TEST_DURATION_SECONDS"); v != "" {
		if i, _ := strconv.Atoi(v); i > 0 {
			testDurationSeconds = i
		}
	}
	if p := os.Getenv("REPORT_HTML_PATH"); p != "" {
		reportHTMLPath = p
	} else {
		reportHTMLPath = "../latency_report.html"
	}
}

type resultRow struct {
	Endpoint  string
	Count     int
	MinMs     float64
	MaxMs     float64
	AvgMs     float64
	MedianMs  float64
	P50       float64
	P90       float64
	P95       float64
	P99       float64
	P999      float64
	Error     string
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	k := (float64(len(sorted)) - 1) * (p / 100)
	f := int(k)
	if f < 0 {
		f = 0
	}
	c := f + 1
	if c >= len(sorted) {
		c = f
	}
	return sorted[f] + (k-float64(f))*(sorted[c]-sorted[f])
}

func summarize(name string, latencies []float64) resultRow {
	r := resultRow{Endpoint: name}
	if len(latencies) == 0 {
		r.Error = "no successful samples"
		return r
	}
	sorted := make([]float64, len(latencies))
	copy(sorted, latencies)
	sort.Float64s(sorted)
	r.Count = len(sorted)
	r.MinMs = sorted[0]
	r.MaxMs = sorted[len(sorted)-1]
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	r.AvgMs = sum / float64(len(sorted))
	r.MedianMs = percentile(sorted, 50)
	r.P50 = percentile(sorted, 50)
	r.P90 = percentile(sorted, 90)
	r.P95 = percentile(sorted, 95)
	r.P99 = percentile(sorted, 99)
	r.P999 = percentile(sorted, 99.9)
	return r
}

// directCall runs fn and returns latency in ms.
func directCall(fn func()) float64 {
	start := time.Now()
	fn()
	return time.Since(start).Seconds() * 1000
}

type directTestCase struct {
	Name string
	Run  func(s *store.Store, rng *rand.Rand) func()
}

func getAllDirectTests() []directTestCase {
	return []directTestCase{
		{"GET /ping", func(s *store.Store, rng *rand.Rand) func() {
			return func() { s.Ping() }
		}},
		{"GET /user/{id}/score", func(s *store.Store, rng *rand.Rand) func() {
			uid := strconv.Itoa(1 + rng.Intn(restoreN))
			return func() { s.GetUserScore(uid) }
		}},
		{"GET /user/{id}/rank", func(s *store.Store, rng *rand.Rand) func() {
			uid := strconv.Itoa(1 + rng.Intn(restoreN))
			return func() { s.GetUserRank(uid) }
		}},
		{"GET /score/{score}/count", func(s *store.Store, rng *rand.Rand) func() {
			sc := minScore + rng.Intn(maxScore-minScore+1)
			return func() { s.GetScoreCount(sc) }
		}},
		{"GET /leaderboard", func(s *store.Store, rng *rand.Rand) func() {
			return func() { s.GetLeaderboard(1, store.LeaderboardTop, nil, nil) }
		}},
		{"GET /leaderboard?min_rank=&max_rank= (dynamic)", func(s *store.Store, rng *rand.Rand) func() {
			minR := 1 + rng.Intn(restoreN)
			maxR := minR + rng.Intn(restoreN-minR+1)
			if maxR > restoreN {
				maxR = restoreN
			}
			return func() { s.GetLeaderboard(minR, maxR, nil, nil) }
		}},
		{"GET /leaderboard?min_score=&max_score= (dynamic)", func(s *store.Store, rng *rand.Rand) func() {
			minS := minScore + rng.Intn(maxScore-minScore+1)
			maxS := minS + rng.Intn(maxScore-minS+1)
			if maxS > maxScore {
				maxS = maxScore
			}
			return func() { s.GetLeaderboard(1, restoreN, &minS, &maxS) }
		}},
		{"GET /leaderboard/all", func(s *store.Store, rng *rand.Rand) func() {
			return func() { s.GetLeaderboardAll() }
		}},
	}
}

func runDirectLoadTest(s *store.Store, rng *rand.Rand, tc directTestCase) []float64 {
	interval := time.Second / time.Duration(kRequestsPerSecond)
	totalRequests := kRequestsPerSecond * testDurationSeconds
	latencies := make([]float64, 0, totalRequests)
	start := time.Now()
	for i := 0; i < totalRequests; i++ {
		next := start.Add(interval * time.Duration(i))
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}
		fn := tc.Run(s, rng)
		latencies = append(latencies, directCall(fn))
	}
	return latencies
}

func htmlEsc(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

func readSection(content, startMarker, endMarker, fallback string) string {
	i := strings.Index(content, startMarker)
	j := strings.Index(content, endMarker)
	if i == -1 || j == -1 || j <= i {
		return fallback
	}
	inner := strings.TrimSpace(content[i+len(startMarker) : j])
	if inner == "" {
		return fallback
	}
	return inner
}

const chartFooter = `
  <h2 style="margin-top: 3rem;">Avg (ms) by endpoint – comparison</h2>
  <div id="chart-container" style="height: 420px; max-width: 1200px; margin: 1.5rem 0;"><canvas id="avg-chart"></canvas></div>
  <h2 style="margin-top: 2rem;">Performance comparison (% improvement)</h2>
  <p class="meta">Positive % = faster than baseline. Baseline: Python (Redis).</p>
  <div id="comparison-table-container"></div>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <script>
(function(){var h2s=document.querySelectorAll("body h2");var sectionNames=["Python","Golang","In-Memory (Fenwick)","Direct (Fenwick)"];var tables=[];for(var i=0;i<h2s.length;i++){var name=h2s[i].textContent.trim();if(sectionNames.indexOf(name)===-1)continue;var el=h2s[i].nextElementSibling;while(el&&el.tagName!=="TABLE")el=el.nextElementSibling;if(!el||el.tagName!=="TABLE")continue;tables.push({name:name,table:el});}var dataBySection=[{},{},{},{}];var sectionIndex={"Python":0,"Golang":1,"In-Memory (Fenwick)":2,"Direct (Fenwick)":3};tables.forEach(function(t){var idx=sectionIndex[t.name];if(idx===undefined)return;var rows=t.table.querySelectorAll("tbody tr");rows.forEach(function(tr){var tds=tr.querySelectorAll("td");if(tds.length<5)return;var endpoint=tds[0].textContent.trim();var avg=parseFloat(tds[4].textContent.trim());if(isNaN(avg))return;dataBySection[idx][endpoint]=avg;});});var allEndpoints=[];var seen={};[0,1,2,3].forEach(function(i){Object.keys(dataBySection[i]||{}).forEach(function(e){if(!seen[e]){seen[e]=1;allEndpoints.push(e);}});});var ctx=document.getElementById("avg-chart");if(!ctx)return;ctx=ctx.getContext("2d");var colors=["#e74c3c","#3498db","#2ecc71","#f39c12"];var sets=[{label:"Python (Redis)",d:0},{label:"Go (Redis)",d:1},{label:"Go (Fenwick in-memory)",d:2},{label:"Direct (Fenwick)",d:3}];var datasets=sets.map(function(s,i){return{label:s.label,data:allEndpoints.map(function(e){return dataBySection[s.d][e]!=null?dataBySection[s.d][e]:null;}),backgroundColor:colors[i]};});new Chart(ctx,{type:"bar",data:{labels:allEndpoints,datasets:datasets},options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,title:{display:true,text:"Avg (ms)"}}},plugins:{title:{display:true,text:"Average latency by endpoint"}}}});var py=dataBySection[0],go=dataBySection[1],fen=dataBySection[2],dir=dataBySection[3];var tableHtml="<table><thead><tr><th>Endpoint</th><th>Python (Redis) ms</th><th>Go (Redis) ms</th><th>Go (Fenwick) ms</th><th>Direct (Fenwick) ms</th><th>Go-Redis vs Python</th><th>Go-Fenwick vs Python</th><th>Direct vs Python</th><th>Direct vs Go-Fenwick</th></tr></thead><tbody>";allEndpoints.forEach(function(ep){var p=py[ep],g=go[ep],f=fen[ep],d=dir[ep];var cell=function(v){return v!=null?v.toFixed(2):"-";};var pct=function(base,x){if(base==null||x==null||base<=0)return"-";return ((base-x)/base*100).toFixed(1)+"%";};tableHtml+="<tr><td>"+ep+"</td><td>"+cell(p)+"</td><td>"+cell(g)+"</td><td>"+cell(f)+"</td><td>"+cell(d)+"</td><td>"+pct(p,g)+"</td><td>"+pct(p,f)+"</td><td>"+pct(p,d)+"</td><td>"+pct(f,d)+"</td></tr>";});tableHtml+="</tbody></table>";document.getElementById("comparison-table-container").innerHTML=tableHtml;})();
  </script>
</body>
</html>
`

func writeHTMLReport(results []resultRow) {
	var directBody bytes.Buffer
	directBody.WriteString("  <p class=\"meta\">Direct calls (no HTTP) | K_RPS=" + strconv.Itoa(kRequestsPerSecond) + " | RESTORE_N=" + strconv.Itoa(restoreN) + " | DURATION=" + strconv.Itoa(testDurationSeconds) + "s</p>\n")
	directBody.WriteString("  <table>\n    <thead>\n      <tr>\n        <th>Endpoint</th>\n        <th>Count</th>\n        <th>Min (ms)</th>\n        <th>Max (ms)</th>\n        <th>Avg (ms)</th>\n        <th>Median (ms)</th>\n        <th>p50 (ms)</th>\n        <th>p90 (ms)</th>\n        <th>p95 (ms)</th>\n        <th>p99 (ms)</th>\n        <th>p99.9 (ms)</th>\n      </tr>\n    </thead>\n    <tbody>\n")
	for _, r := range results {
		if r.Error != "" {
			fmt.Fprintf(&directBody, "    <tr><td>%s</td><td colspan=\"10\">%s</td></tr>\n", htmlEsc(r.Endpoint), htmlEsc(r.Error))
			continue
		}
		median := r.MedianMs
		if median == 0 {
			median = r.P50
		}
		fmt.Fprintf(&directBody, "    <tr><td>%s</td><td>%d</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td></tr>\n",
			htmlEsc(r.Endpoint), r.Count, r.MinMs, r.MaxMs, r.AvgMs, median, r.P50, r.P90, r.P95, r.P99, r.P999)
	}
	directBody.WriteString("    </tbody>\n  </table>")

	existing, _ := os.ReadFile(reportHTMLPath)
	existingStr := string(existing)

	py := readSection(existingStr, markerPythonStart, markerPythonEnd, `<p class="meta">Run Python latency test</p>`)
	goSec := readSection(existingStr, markerGolangStart, markerGolangEnd, `<p class="meta">Run Go latency test</p>`)
	inmem := readSection(existingStr, markerInmemStart, markerInmemEnd, `<p class="meta">Run Go in-memory latency test</p>`)

	var buf bytes.Buffer
	buf.WriteString(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>API Latency Report</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem; background: #1a1a1a; color: #e0e0e0; }
    h1 { color: #fff; margin-bottom: 0.5rem; }
    h2 { color: #ccc; margin-top: 2rem; margin-bottom: 0.5rem; font-size: 1.2rem; }
    .meta { color: #888; margin-bottom: 1rem; font-size: 0.9rem; }
    table { border-collapse: collapse; width: 100%; max-width: 1200px; }
    th, td { border: 1px solid #444; padding: 0.6rem 0.8rem; text-align: left; }
    th { background: #2d2d2d; color: #fff; font-weight: 600; }
    tr:nth-child(even) { background: #252525; }
    tr:hover { background: #333; }
    td:nth-child(n+2) { text-align: right; font-variant-numeric: tabular-nums; }
    td:first-child { max-width: 400px; word-break: break-all; }
  </style>
</head>
<body>
  <h1>API Latency Report</h1>
  <h2>Python</h2>
` + markerPythonStart + "\n" + py + "\n" + markerPythonEnd + `
  <h2>Golang</h2>
` + markerGolangStart + "\n" + goSec + "\n" + markerGolangEnd + `
  <h2>In-Memory (Fenwick)</h2>
` + markerInmemStart + "\n" + inmem + "\n" + markerInmemEnd + `
  <h2>Direct (Fenwick)</h2>
` + markerDirectStart + "\n" + directBody.String() + "\n" + markerDirectEnd + chartFooter)

	if err := os.WriteFile(reportHTMLPath, buf.Bytes(), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		return
	}
	fmt.Printf("\nReport written to %s\n", reportHTMLPath)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	s := store.New()
	rng := rand.New(rand.NewSource(42))
	fmt.Printf("Direct store: RestoreRandom(n=%d) ...\n", restoreN)
	s.RestoreRandom(restoreN, rng)
	fmt.Println("Load testing (direct calls) ...")

	allCases := getAllDirectTests()
	var testCases []directTestCase
	if len(os.Args) > 1 {
		filter := strings.ToLower(strings.TrimSpace(os.Args[1]))
		for _, tc := range allCases {
			if strings.Contains(strings.ToLower(tc.Name), filter) {
				testCases = append(testCases, tc)
			}
		}
		if len(testCases) == 0 {
			fmt.Printf("No endpoint matching %q\n", os.Args[1])
			os.Exit(1)
		}
	} else {
		testCases = allCases
	}

	fmt.Printf("K_RPS=%d RESTORE_N=%d DURATION=%ds\n", kRequestsPerSecond, restoreN, testDurationSeconds)

	rng2 := rand.New(rand.NewSource(99))
	var results []resultRow
	for _, tc := range testCases {
		latencies := runDirectLoadTest(s, rng2, tc)
		results = append(results, summarize(tc.Name, latencies))
	}

	fmt.Println("\n--- Latency summary (ms) ---")
	for _, r := range results {
		if r.Error != "" {
			fmt.Printf("  %s: %s\n", r.Endpoint, r.Error)
			continue
		}
		fmt.Printf("  %s: n=%d min=%.2f max=%.2f avg=%.2f p50=%.2f p99=%.2f\n",
			r.Endpoint, r.Count, r.MinMs, r.MaxMs, r.AvgMs, r.P50, r.P99)
	}
	writeHTMLReport(results)
}
