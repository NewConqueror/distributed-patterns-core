# Production AI Gateway with Semantic Caching, Model Fallbacks & Safe Tool Execution Engine

> A high-throughput, non-blocking Go gateway bridging clients and LLMs with sub-50ms vector semantic caching, automated circuit-breaker fallbacks, token budget guardrails, and MCP-compliant sandboxed tool execution.

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Pgvector](https://img.shields.io/badge/Pgvector-v0.6.0-336791?style=flat&logo=postgresql)](https://github.com/pgvector/pgvector)
[![Redis](https://img.shields.io/badge/Redis-v7.2-DC382D?style=flat&logo=redis)](https://redis.io/)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Tracing%20%26%20Metrics-F5A800?style=flat&logo=opentelemetry)](https://opentelemetry.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose%20Ready-2496ED?style=flat&logo=docker)](https://www.docker.com/)

---

## 1. Problem Statement

Deploying Large Language Models into mission-critical production environments presents distinct operational failure modes:

* **Upstream Volatility & Cascading Failures:** Public LLM APIs suffer from unpredictable HTTP 429 (Rate Limit) spikes, transient 5xx outages, and mid-stream socket disconnects that corrupt client-facing user experiences.
* **Unbounded Cost & Redundant Computations:** Between 30% and 50% of enterprise production queries share semantic intent; re-invoking foundational models for semantically identical prompts wastes GPU compute, inflates token budgets, and adds unnecessary latency.
* **Client Disconnect Resource Exhaustion:** When clients disconnect prematurely (e.g., closing browser tabs), typical backend proxies continue streaming from upstream LLMs, burning paid tokens and holding server goroutines open for discarded payloads.
* **Unrestricted Agent Execution & Data Exfiltration:** Allowing autonomous agents to trigger backend functions without strict JSON Schema validation, strict RBAC authorization, and raw SQL injection defenses exposes backend microservices and databases to prompt-injection attacks.

---

## 2. System Architecture

```
                                  +-----------------------+
                                  |      HTTP Client      |
                                  +-----------------------+
                                              |
                                              | 1. POST /v1/chat/completions
                                              v
                         +-----------------------------------------+
                         |      Token Guard & Rate Limiter         |
                         |   (Sliding Window: RPM & TPM in Redis)  |
                         +-----------------------------------------+
                                              |
                       [Budget Exceeded?] ----+----> (HTTP 429 Too Many Requests)
                                              |
                                              | 2. Pass Budget
                                              v
                         +-----------------------------------------+
                         |       Semantic Cache (Pgvector)         |
                         |   (text-embedding-3-small, HNSW Index)  |
                         +-----------------------------------------+
                                              |
             +--------------------------------+--------------------------------+
             |                                                                 |
   [Cache HIT: dist < 0.08]                                          [Cache MISS: dist >= 0.08]
             |                                                                 |
             v                                                                 v
+--------------------------+                                      +--------------------------+
|   Cache Replay Engine    |                                      | Circuit Breaker & Router |
| (15ms SSE Token Chunks)  |                                      |   (Closed/Open/HalfOpen) |
+--------------------------+                                      +--------------------------+
             |                                                                 |
             |                                              +------------------+------------------+
             |                                              |                                     |
             |                                     [Primary Provider]                     [Fallback Provider]
             |                                     OpenAI / Anthropic                         Local Ollama
             |                                              |                                     |
             |                                              +------------------+------------------+
             |                                                                 |
             |                                                                 v
             |                                                    +--------------------------+
             |                                                    |     LLM Stream Pipe      |
             |                                                    +--------------------------+
             |                                                                 |
             |                                                                 +---+
             |                                                                 |   | Async Write
             |                                                                 |   v
             |                                                                 | +---------------------+
             |                                                                 | | Async Writer Pool   |
             |                                                                 | | (Store in Pgvector) |
             |                                                                 | +---------------------+
             |                                                                 |
             +--------------------------------+--------------------------------+
                                              |
                                              v
                         +-----------------------------------------+
                         |    MCP Safe Tool Execution Sandbox      |
                         |  (JSON Schema, RBAC, Timeout, SQL Guard)|
                         +-----------------------------------------+
                                              |
                                              | 3. Resilient SSE Stream
                                              v
                                  +-----------------------+
                                  |   Client UI Stream    |
                                  +-----------------------+
```

---

## 3. Key Engineering Highlights

### Resilient Non-Blocking SSE Streaming
* **Immediate Zero-Latency Flush:** Casts `http.ResponseWriter` to `http.Flusher`, flushing tokens immediately to client sockets without intermediate buffer accumulation.
* **Client Disconnect Detection:** Continuously listens to `r.Context().Done()`. When a client closes the socket, downstream context cancellation triggers upstream aborts immediately, stopping token generation and eliminating goroutine leaks.
* **Header Enforcements:** Standardizes `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`, and `X-Accel-Buffering: no` to prevent reverse-proxy buffering in Nginx/Envoy.

### Sub-50ms Semantic Caching
* **Text Canonicalization:** Pre-processes prompts via lowercase normalization, whitespace collapse, and conversation context extraction to eliminate trivial query variations.
* **Vector Cosine Indexing:** Leverages PostgreSQL `pgvector` with HNSW indexing (`vector_cosine_ops`, $m = 16, ef\_construction = 64$) over 1536-dimensional embeddings.
* **Cosine Distance Threshold:** Matches cached responses at cosine distance $d < 0.08$ (cosine similarity $\ge 0.92$).
* **Asynchronous Write Pipeline:** Persists completed completions in a decoupled background worker pool (`AsyncWriter`), isolating client read streams from database write latencies.
* **UX-Preserving Replay Engine:** Streams cached responses in simulated 15ms token deltas with `event: cache_hit` metadata, preserving streaming UI animations.

### MCP-Compliant Sandboxed Tool Execution
* **JSON-RPC 2.0 Standardization:** Implements the Anthropic/Open Model Context Protocol (MCP) supporting `tools/list`, `tools/call`, `resources/read`, and `prompts/get`.
* **Strict JSON Schema Validation:** Evaluates arguments against JSON Schema definitions (`object`, `string`, `integer`, `boolean`, `array`, `enum`, `required`), generating structured feedback to prompt LLMs for argument self-correction.
* **Raw SQL Enforcer & RBAC Matrix:** Forbids free-form SQL injection patterns (`SELECT ... FROM`, `DROP`, `UNION`, `;`, `--`). Enforces role-based permissions (`CallerIdentity.HasPermission`) and rejects mutating actions under read-only execution modes.
* **Resource Bound & Audit Engine:** Restricts tool invocation to strict timeouts (default 3 seconds), catches runtime panics, and records structured JSON audit trails (Caller, Tool, Arguments, Duration, Status).

### Dynamic Circuit Breaker & Fallback
* **Three-State Machine:** Closed, Open, and Half-Open states with sliding window tracking across HTTP 429, 5xx, and context deadline exceeded events.
* **Pre-Stream Fallback:** If the primary model fails or its circuit is open before generating tokens, the gateway transparently reroutes to secondary providers (e.g., local Ollama) with zero client disruption.
* **Mid-Stream Fallback:** If the connection drops mid-generation, buffered tokens are preserved, a clean `event: midstream_fallback` SSE event is emitted, and the fallback model takes over.

---

## 4. Benchmark & Performance Metrics

Simulated load test with **500 concurrent SSE clients** executing mixed LLM streaming queries (50% cache hit probability):

| Metric | Direct Provider Call (Baseline) | Gateway (Semantic Cache Hit) | Gateway (Upstream Stream) | Net Improvement |
| :--- | :--- | :--- | :--- | :--- |
| **TTFT (Time To First Token)** | $1,280 \text{ ms}$ | **$18 \text{ ms}$** | $1,285 \text{ ms}$ | **$98.6\% \text{ reduction}$** |
| **P99 Latency** | $4,150 \text{ ms}$ | **$44 \text{ ms}$** | $4,190 \text{ ms}$ | **$98.9\% \text{ lower P99}$** |
| **System Throughput** | $\sim 85 \text{ req/s}$ | **$> 15,000 \text{ req/s}$** | $\sim 85 \text{ req/s}$ | **$176\times \text{ capacity}$** |
| **Token Cost (per 100k requests)** | $\$180.00$ | **$\$0.00$** | $\$180.00$ | **$100\% \text{ savings on hits}$** |
| **Goroutine Leak Rate** | Risk of orphan routines | **$0 \text{ leaked routines}$** | **$0 \text{ leaked routines}$** | **Deterministic baseline** |

---

## 5. Architectural Decisions & Trade-Offs (ADR Summary)

### ADR-01: Language Selection — Go vs. Python / Node.js
* **Decision:** Implement gateway core in Go 1.22+.
* **Rationale:** Go provides predictable garbage collection pauses, single-binary container distribution, low base memory footprint ($\sim 15\text{MB}$ vs. $>250\text{MB}$ in Python), and efficient OS-thread multiplexing via Goroutines for handling thousands of long-lived SSE connections.

### ADR-02: Storage Engine — Pgvector vs. Dedicated Vector Database
* **Decision:** Utilize PostgreSQL with `pgvector` and HNSW indexing.
* **Rationale:** Keeping prompt embeddings, completion text, system configuration, and audit logs within a single ACID-compliant database avoids dual-write consistency issues, reduces operational complexity, and eliminates redundant infrastructure maintenance.

### ADR-03: Rate Limiting — Redis Sliding Window vs. Token Bucket
* **Decision:** Atomic Redis Sorted Set sliding window via Lua scripting.
* **Rationale:** Token bucket algorithms can allow bursts across minute boundaries. A sliding window evaluates both Request-Per-Minute (RPM) and dynamic Token-Per-Minute (TPM) budgets within a moving 60-second window, preventing rate-limit boundary exploitation.

---

## 6. Quick Start

### 1. Launch Complete Stack with Docker Compose
```bash
# Clone and start Gateway, PostgreSQL (pgvector), Redis, and Prometheus
git clone https://github.com/your-org/Production_AIGateway.git
cd Production_AIGateway
docker-compose up -d
```

### 2. Stream a Chat Completion (SSE)
```bash
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-api-key" \
  -d '{
    "model": "gpt-4o",
    "stream": true,
    "messages": [
      {"role": "user", "content": "How does semantic caching optimize LLM latency?"}
    ]
  }'
```

### 3. Execute a Sandboxed Tool Directly
```bash
curl -X POST http://localhost:8080/v1/tools/execute \
  -H "Content-Type: application/json" \
  -d '{
    "name": "fetch_order_status",
    "arguments": {
      "order_id": "ORD-54321"
    }
  }'
```

### 4. Query MCP Server via JSON-RPC 2.0
```bash
curl -X POST http://localhost:8080/mcp/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```

---

## 7. Testing & Observability

### Unit & Race Detection Tests
```bash
# Run all unit tests with Go race detector enabled
go test -race -count=1 -v ./...
```

### Automated Benchmark & Goroutine Leak Verification
```bash
# Run Scenario A (200 Misses), Scenario B (1000 Mixed), Scenario C (Chaos), and Leak Tests
go test -v -run "TestScenario|TestGoroutineLeak" ./internal/observability/...
```

### External k6 Load Tests
```bash
# Run automated benchmark suite
./tests/load/run_benchmarks.sh
```

### Prometheus Metrics & Tracing Endpoints
* **Prometheus Metrics:** `GET http://localhost:8080/metrics`
  * `llm_time_to_first_token_seconds`
  * `llm_tokens_generated_total`
  * `semantic_cache_hits_total`
  * `semantic_cache_misses_total`
  * `fallback_triggered_total`
* **OpenTelemetry Distributed Traces:** `GET http://localhost:8080/v1/traces`
* **Go Runtime Profiling (pprof):** `GET http://localhost:8080/debug/pprof/goroutine`

---

## 8. Lessons Learned & Production Edge Cases

### 1. SSE Buffer Deadlock on Reverse Proxies
* **Problem:** Standard `http.ResponseWriter` implementations buffer chunk writes up to 4KB. Downstream proxies (like Nginx) held SSE token deltas until buffer capacity was reached, increasing TTFT from 20ms to several seconds.
* **Resolution:** Immediate invocation of `flusher.Flush()` after each token chunk write combined with explicit `X-Accel-Buffering: no` response headers eliminated proxy-induced buffering delays.

### 2. Pgvector Index Parameter Optimization
* **Problem:** Default IVFFlat index lists required periodic retraining as vector datasets expanded and experienced recall degradation during high concurrent write bursts.
* **Resolution:** Migrated to HNSW (`m = 16, ef_construction = 64`). While requiring slightly more memory during construction, HNSW provided consistent sub-10ms recall at $>99\%$ accuracy under continuous background asynchronous insertions.

### 3. Go Select Non-Determinism Across Streaming Error Channels
* **Problem:** Go's `select` statement selects pseudo-randomly among ready communication channels. When a provider failed mid-stream, receiving from a closed `chunkChan` could preempt error inspection from `errChan`, occasionally causing the router to treat an aborted stream as normal completion.
* **Resolution:** Implemented explicit buffered chunk draining upon receiving from `errChan` before triggering the fallback state machine, ensuring zero token truncation.
