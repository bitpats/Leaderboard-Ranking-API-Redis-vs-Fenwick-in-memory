# Latency report generation (writes to latency_report.html at project root)
# Use "make report" to bring up Docker, run all 4 tests (Python, Go Redis, Go In-Memory, Direct), then bring down.

.PHONY: report report-only report-python report-go report-inmem report-direct help docker-up docker-wait docker-down

help:
	@echo "Report generation (output: latency_report.html)"
	@echo "  make report          - Docker compose up, run all 4 latency tests, Docker compose down"
	@echo "  make report-only     - Run all 4 tests only (requires: docker compose up already)"
	@echo "  make report-python   - Run Python latency test only (API on :8000)"
	@echo "  make report-go       - Run Golang latency test only (API on :8080)"
	@echo "  make report-inmem    - Run In-Memory (Fenwick) API latency test only (API on :8081)"
	@echo "  make report-direct   - Run Direct (Fenwick) latency test only (no API; in-process calls)"

# Full flow: start stack, run all reports (3 API-based + direct), then stop stack
report: docker-up docker-wait report-only docker-down
	@echo "Done. Open latency_report.html"

# Run all four report generators (call when services are already up)
report-only: report-python report-go report-inmem report-direct
	@echo "Reports written to latency_report.html"

docker-up:
	@echo "Starting Redis + Python API + Go Redis API + Go In-Memory API..."
	docker compose up -d --build

docker-wait:
	@echo "Waiting for services to be ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		curl -sf http://localhost:8000/ping >/dev/null 2>&1 && \
		curl -sf http://localhost:8080/ping >/dev/null 2>&1 && \
		curl -sf http://localhost:8081/ping >/dev/null 2>&1 && break; \
		echo "  attempt $$i/10..."; sleep 3; \
	done
	@curl -sf http://localhost:8000/ping >/dev/null 2>&1 && curl -sf http://localhost:8080/ping >/dev/null 2>&1 && curl -sf http://localhost:8081/ping >/dev/null 2>&1 || (echo "Services not ready; check docker compose."; exit 1)
	@sleep 2

docker-down:
	@echo "Stopping services..."
	docker compose down

report-python:
	@echo "Running Python latency test..."
	cd python && python test_apis_latency.py

report-go:
	@echo "Running Golang latency test..."
	cd go && go run ./cmd/latency

report-inmem:
	@echo "Running In-Memory (Fenwick) latency test..."
	cd go && go run ./cmd/latency_inmem

report-direct:
	@echo "Running Direct (Fenwick) latency test (no HTTP)..."
	cd go-fenwick-based-in-memory && go run ./cmd/latency_direct
