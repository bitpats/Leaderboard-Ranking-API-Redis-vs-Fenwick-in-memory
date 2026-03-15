// Latency test for in-memory Fenwick leaderboard (on :8081 by default).
// Same shape as the Redis-backed tester but targets the in-memory service.
//
// Usage:
//
//	go run ./cmd/latency_inmem              # run all endpoints
//	go run ./cmd/latency_inmem <name>       # run only endpoint matching <name>
package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Global constants
var (
	baseURL            = "http://localhost:8081"
	kRequestsPerSecond = 50
	restoreN           = 1000000
	testDurationSeconds = 10
	minScore           = 1
	maxScore           = 5000
	reportHTMLPath     = "latency_report.html" // set by init to project root
)

const (
	markerPythonStart = "<!-- PYTHON-SECTION -->"
	markerPythonEnd   = "<!-- END-PYTHON-SECTION -->"
	markerGolangStart = "<!-- GOLANG-SECTION -->"
	markerGolangEnd   = "<!-- END-GOLANG-SECTION -->"
	markerInmemStart  = "<!-- INMEM-SECTION -->"
	markerInmemEnd    = "<!-- END-INMEM-SECTION -->"
	markerDirectStart = "<!-- DIRECT-SECTION -->"
	markerDirectEnd   = "<!-- END-DIRECT-SECTION -->"
)

const chartAndComparisonFooter = `
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

func init() {
	if u := os.Getenv("BASE_URL"); u != "" {
		baseURL = strings.TrimRight(u, "/")
	}
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
	} else if _, err := os.Stat("go.mod"); err == nil {
		reportHTMLPath = "../latency_report.html"
	}
}

type resultRow struct {
	Endpoint string
	Count    int
	MinMs    float64
	MaxMs    float64
	AvgMs    float64
	MedianMs float64
	P50      float64
	P90      float64
	P95      float64
	P99      float64
	P999     float64
	Error    string
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
	r.MinMs = round2(sorted[0])
	r.MaxMs = round2(sorted[len(sorted)-1])
	var sum float64
	for _, v := range sorted {
		sum += v
	}
	r.AvgMs = round2(sum / float64(len(sorted)))
	r.MedianMs = round2(percentile(sorted, 50))
	r.P50 = round2(percentile(sorted, 50))
	r.P90 = round2(percentile(sorted, 90))
	r.P95 = round2(percentile(sorted, 95))
	r.P99 = round2(percentile(sorted, 99))
	r.P999 = round2(percentile(sorted, 99.9))
	return r
}

func round2(x float64) float64 { return float64(int(x*100+0.5)) / 100 }

func runRestoreRandom(client *http.Client) error {
	u := baseURL + "/restore_random?n=" + strconv.Itoa(restoreN)
	resp, err := client.Post(u, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("restore_random: %s", resp.Status)
	}
	fmt.Printf("restore_random (inmem) done (n=%d)\n", restoreN)
	return nil
}

type reqBuilder func() (method, path string, params url.Values)

func fireRequest(client *http.Client, method, path string, params url.Values, latencies *[]float64, mu *sync.Mutex) {
	start := time.Now()
	fullURL := baseURL + path
	if len(params) > 0 {
		fullURL += "?" + params.Encode()
	}
	req, err := http.NewRequest(method, fullURL, nil)
	if err != nil {
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		elapsed := time.Since(start).Seconds() * 1000
		mu.Lock()
		*latencies = append(*latencies, elapsed)
		mu.Unlock()
	}
}

func runLoadTest(client *http.Client, name, method string, build reqBuilder) []float64 {
	var latencies []float64
	var mu sync.Mutex
	interval := time.Second / time.Duration(kRequestsPerSecond)
	totalRequests := kRequestsPerSecond * testDurationSeconds
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < totalRequests; i++ {
		next := start.Add(interval * time.Duration(i))
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}
		m, p, params := build()
		_ = name
		_ = method
		wg.Add(1)
		go func() {
			defer wg.Done()
			fireRequest(client, m, p, params, &latencies, &mu)
		}()
	}
	wg.Wait()
	return latencies
}

func makePing() reqBuilder {
	return func() (string, string, url.Values) {
		return "GET", "/ping", nil
	}
}
func makeUserScore() reqBuilder {
	return func() (string, string, url.Values) {
		uid := strconv.Itoa(1 + rand.Intn(restoreN))
		return "GET", "/user/" + uid + "/score", nil
	}
}
func makeUserRank() reqBuilder {
	return func() (string, string, url.Values) {
		uid := strconv.Itoa(1 + rand.Intn(restoreN))
		return "GET", "/user/" + uid + "/rank", nil
	}
}
func makeScoreCount() reqBuilder {
	return func() (string, string, url.Values) {
		s := minScore + rand.Intn(maxScore-minScore+1)
		return "GET", "/score/" + strconv.Itoa(s) + "/count", nil
	}
}
func makeLeaderboard() reqBuilder {
	return func() (string, string, url.Values) {
		return "GET", "/leaderboard", nil
	}
}
func makeLeaderboardRankRange() reqBuilder {
	return func() (string, string, url.Values) {
		minR := 1 + rand.Intn(restoreN)
		maxR := minR + rand.Intn(restoreN-minR+1)
		if maxR > restoreN {
			maxR = restoreN
		}
		v := url.Values{}
		v.Set("min_rank", strconv.Itoa(minR))
		v.Set("max_rank", strconv.Itoa(maxR))
		return "GET", "/leaderboard", v
	}
}
func makeLeaderboardScoreRange() reqBuilder {
	return func() (string, string, url.Values) {
		minS := minScore + rand.Intn(maxScore-minScore+1)
		maxS := minS + rand.Intn(maxScore-minS+1)
		if maxS > maxScore {
			maxS = maxScore
		}
		v := url.Values{}
		v.Set("min_score", strconv.Itoa(minS))
		v.Set("max_score", strconv.Itoa(maxS))
		return "GET", "/leaderboard", v
	}
}
func makeLeaderboardAll() reqBuilder {
	return func() (string, string, url.Values) {
		return "GET", "/leaderboard/all", nil
	}
}

type testCase struct {
	Name   string
	Method string
	Build  reqBuilder
}

func getAllTestCases() []testCase {
	return []testCase{
		{"GET /ping", "GET", makePing()},
		{"GET /user/{id}/score", "GET", makeUserScore()},
		{"GET /user/{id}/rank", "GET", makeUserRank()},
		{"GET /score/{score}/count", "GET", makeScoreCount()},
		{"GET /leaderboard", "GET", makeLeaderboard()},
		{"GET /leaderboard?min_rank=&max_rank= (dynamic)", "GET", makeLeaderboardRankRange()},
		{"GET /leaderboard?min_score=&max_score= (dynamic)", "GET", makeLeaderboardScoreRange()},
		{"GET /leaderboard/all", "GET", makeLeaderboardAll()},
	}
}

func filterByName(cases []testCase, name string) []testCase {
	name = strings.ToLower(strings.TrimSpace(name))
	var out []testCase
	for _, tc := range cases {
		if strings.Contains(strings.ToLower(tc.Name), name) {
			out = append(out, tc)
		}
	}
	return out
}

func htmlEsc(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

func readSectionBetweenMarkers(content, startMarker, endMarker, fallback string) string {
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

func writeHTMLReport(results []resultRow) {
	var golangBody bytes.Buffer
	golangBody.WriteString("  <p class=\"meta\">BASE_URL=" + htmlEsc(baseURL) + " | K_RPS=" + strconv.Itoa(kRequestsPerSecond) + " | RESTORE_N=" + strconv.Itoa(restoreN) + " | DURATION=" + strconv.Itoa(testDurationSeconds) + "s</p>\n")
	golangBody.WriteString("  <table>\n    <thead>\n      <tr>\n        <th>Endpoint</th>\n        <th>Count</th>\n        <th>Min (ms)</th>\n        <th>Max (ms)</th>\n        <th>Avg (ms)</th>\n        <th>Median (ms)</th>\n        <th>p50 (ms)</th>\n        <th>p90 (ms)</th>\n        <th>p95 (ms)</th>\n        <th>p99 (ms)</th>\n        <th>p99.9 (ms)</th>\n      </tr>\n    </thead>\n    <tbody>\n")
	for _, r := range results {
		if r.Error != "" {
			fmt.Fprintf(&golangBody, "    <tr><td>%s</td><td colspan=\"10\">%s</td></tr>\n", htmlEsc(r.Endpoint), htmlEsc(r.Error))
			continue
		}
		median := r.MedianMs
		if median == 0 {
			median = r.P50
		}
		fmt.Fprintf(&golangBody, "    <tr><td>%s</td><td>%d</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td></tr>\n",
			htmlEsc(r.Endpoint), r.Count, r.MinMs, r.MaxMs, r.AvgMs, median, r.P50, r.P90, r.P95, r.P99, r.P999)
	}
	golangBody.WriteString("    </tbody>\n  </table>")

	// read existing file to preserve Python & Golang sections
	existing, _ := os.ReadFile(reportHTMLPath)
	existingStr := string(existing)

	pythonBody := readSectionBetweenMarkers(existingStr, markerPythonStart, markerPythonEnd, `<p class="meta">Run: <code>cd python &amp;&amp; python test_apis_latency.py</code></p>`)
	golangBodyExisting := readSectionBetweenMarkers(existingStr, markerGolangStart, markerGolangEnd, `<p class="meta">Run: <code>cd go &amp;&amp; go run ./cmd/latency</code></p>`)
	if golangBodyExisting == "" {
		golangBodyExisting = `<p class="meta">Run: <code>cd go &amp;&amp; go run ./cmd/latency</code></p>`
	}
	directBody := readSectionBetweenMarkers(existingStr, markerDirectStart, markerDirectEnd, `<p class="meta">Run: <code>cd go-fenwick-based-in-memory &amp;&amp; go run ./cmd/latency_direct</code></p>`)
	if directBody == "" {
		directBody = `<p class="meta">Run: <code>cd go-fenwick-based-in-memory &amp;&amp; go run ./cmd/latency_direct</code></p>`
	}
	inmemBody := golangBody.String()

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
` + markerPythonStart + "\n" + pythonBody + "\n" + markerPythonEnd + `
  <h2>Golang</h2>
` + markerGolangStart + "\n" + golangBodyExisting + "\n" + markerGolangEnd + `
  <h2>In-Memory (Fenwick)</h2>
` + markerInmemStart + "\n" + inmemBody + "\n" + markerInmemEnd + `
  <h2>Direct (Fenwick)</h2>
` + markerDirectStart + "\n" + directBody + "\n" + markerDirectEnd + chartAndComparisonFooter)

	if err := os.WriteFile(reportHTMLPath, buf.Bytes(), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		return
	}
	fmt.Printf("\nReport written to %s\n", reportHTMLPath)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	allCases := getAllTestCases()
	var testCases []testCase
	if len(os.Args) > 1 {
		filterArg := os.Args[1]
		testCases = filterByName(allCases, filterArg)
		if len(testCases) == 0 {
			fmt.Printf("No endpoint matching '%s'.\n", filterArg)
			fmt.Println("Available endpoints:")
			for _, tc := range allCases {
				fmt.Println(" ", tc.Name)
			}
			os.Exit(1)
		}
		fmt.Printf("Testing only: %v\n", func() []string {
			var names []string
			for _, tc := range testCases {
				names = append(names, tc.Name)
			}
			return names
		}())
	} else {
		testCases = allCases
	}

	fmt.Printf("BASE_URL=%s K_RPS=%d RESTORE_N=%d DURATION=%ds\n", baseURL, kRequestsPerSecond, restoreN, testDurationSeconds)
	client := &http.Client{Timeout: 60 * time.Second}
	fmt.Println("Calling /restore_random ...")
	if err := runRestoreRandom(client); err != nil {
		fmt.Fprintf(os.Stderr, "restore_random: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Load testing each endpoint ...")
	client = &http.Client{Timeout: 30 * time.Second}

	var results []resultRow
	for _, tc := range testCases {
		latencies := runLoadTest(client, tc.Name, tc.Method, tc.Build)
		results = append(results, summarize(tc.Name, latencies))
	}

	fmt.Println("\n--- Latency summary (ms) ---")
	for _, r := range results {
		if r.Error != "" {
			fmt.Printf("  %s: %s\n", r.Endpoint, r.Error)
			continue
		}
		fmt.Printf("  %s: n=%d min=%.2f max=%.2f avg=%.2f p50=%.2f p90=%.2f p95=%.2f p99=%.2f p99.9=%.2f\n",
			r.Endpoint, r.Count, r.MinMs, r.MaxMs, r.AvgMs, r.P50, r.P90, r.P95, r.P99, r.P999)
	}
	writeHTMLReport(results)
}

