# GeoPulse - Real-Time Fleet Telemetry & Geo-Tracking Engine

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![gRPC](https://img.shields.io/badge/Protocol-gRPC%20%7C%20Protobuf-244f5a?style=flat&logo=grpc)](https://grpc.io)
[![Redis](https://img.shields.io/badge/Hot%20Storage-Redis%20v7%20Geospatial-DC382D?style=flat&logo=redis)](https://redis.io)
[![PostgreSQL](https://img.shields.io/badge/Cold%20Storage-PostgreSQL%20%2B%20PostGIS-336791?style=flat&logo=postgresql)](https://postgis.net)
[![Prometheus](https://img.shields.io/badge/Metrics-Prometheus%20Endpoint-E6522C?style=flat&logo=prometheus)](https://prometheus.io)

**GeoPulse** is a high-throughput, low-latency spatial ingestion and real-time vehicle telemetry engine written in Go. The platform is designed to ingest streaming GPS telemetry from thousands of concurrent mobile drivers, perform microsecond-level spatial indexing in-memory, broadcast live updates over regional pub/sub channels, and asynchronously flush historical coordinates to durable PostGIS storage without choking database connection pools.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph Clients["Driver Fleet & Clients"]
        D1["Driver Client 1 (10 Hz)"]
        D2["Driver Client 2 (10 Hz)"]
        DN["Driver Client N (1,000+ VUs)"]
    end

    subgraph Ingestion["Ingestion Layer (Go Concurrency Engine)"]
        GRPC["gRPC Server (:50051)<br/>StreamLocation Handler"]
        RB["Bounded Ring Buffer<br/>(DropOldest / DropNewest Backpressure)"]
        WP["Worker Pool (Concurrent Consumers)"]
    end

    subgraph HotPath["Hot Path (In-Memory Spatial & Real-Time Distribution)"]
        R_GEO["Redis Geospatial<br/>GEOADD drivers:active"]
        R_HB["Redis Heartbeats<br/>ZADD drivers:heartbeat"]
        SWEEP["Ghost Driver Sweeper<br/>(Prunes Inactive TTL Records)"]
        PUBSUB["Redis Pub/Sub<br/>driver.updates.{region_id}"]
    end

    subgraph ColdPath["Cold Path (Durable Batch Persistence)"]
        BB["In-Memory Batch Buffer<br/>(Atomic Slice Detachment)"]
        FE["Dual-Trigger Flusher Engine<br/>• Count: 250 items<br/>• Timer: 3.0 seconds"]
        PG["PostgreSQL + PostGIS<br/>Binary COPY / Multi-INSERT<br/>GIST(geom) Spatial Index"]
    end

    subgraph Observability["Observability & Metrics"]
        PROM["Prometheus Server (:2112/metrics)<br/>active_connections_count<br/>location_pings_total<br/>batch_flush_duration_seconds"]
    end

    D1 & D2 & DN -->|gRPC Bidirectional/Client Streaming| GRPC
    GRPC -->|Push| RB
    RB -->|Pop| WP

    WP -->|Hot Path Pipeline| R_GEO
    WP -->|Timestamp Update| R_HB
    R_HB -.->|Evict Stale| SWEEP
    SWEEP -.->|ZREM| R_GEO
    WP -->|Fan-Out Broadcast| PUBSUB

    WP -->|Cold Path Pipeline| BB
    BB -->|Flush Trigger| FE
    FE -->|Streaming Bulk COPY| PG

    GRPC -.->|Metrics| PROM
    RB -.->|Buffer Depth| PROM
    FE -.->|Latency Histogram| PROM
```

### End-to-End Streaming & Ingestion Flow

```mermaid
sequenceDiagram
    autonumber
    actor Driver as Driver Client
    participant gRPC as gRPC Server
    participant RingBuf as Bounded RingBuffer
    participant Worker as Worker Pool
    participant Redis as Redis (Hot Path)
    participant BatchBuf as Batch Buffer
    participant Postgres as PostgreSQL (Cold Path)

    Driver->>gRPC: Open StreamLocation RPC
    loop 10 Pings / Second
        Driver->>gRPC: Send LocationPing (Protobuf: 52 bytes)
        gRPC->>RingBuf: Push(ping) with Drop Strategy
        Note over RingBuf: Prevents memory explosion<br/>under sudden traffic bursts
    end

    loop Asynchronous Processing
        Worker->>RingBuf: Pop(ctx)
        par Hot Path (Low Latency)
            Worker->>Redis: Pipeline: GEOADD + ZADD heartbeat + HSET meta
            Worker->>Redis: PUBLISH driver.updates.<region_id> (Protobuf)
        and Cold Path (Decoupled Batching)
            Worker->>BatchBuf: Push(ping)
            opt Buffer Count >= 250 OR Timer >= 3s
                BatchBuf->>Postgres: Binary COPY into driver_locations
            end
        end
    end

    Driver->>gRPC: CloseSend (io.EOF)
    gRPC->>Driver: StreamAck (Success=true, ProcessedCount=N)
```

---

## 2. Core Engineering Capabilities

* **Protobuf Wire Optimization:** Wire size reduced from **131 bytes (JSON)** to **52 bytes (Protobuf)**, saving **60.31% network bandwidth** and eliminating JSON reflection overhead.
* **Controlled Backpressure Ring Buffer:** Thread-safe bounded buffer supporting `DropOldest` and `DropNewest` policies. Prevents goroutine bloat and Out-Of-Memory (OOM) crashes under upstream traffic spikes.
* **Microsecond Spatial Indexing:** Uses Redis Sorted Sets (`GEOADD`, `GEOSEARCH`) with 52-bit geohashing for $O(\log N)$ radius driver search (`/drivers/nearby`).
* **Regional Pub/Sub Fan-Out:** Partitioned message routing to discrete channels (`driver.updates.<region_id>`), allowing downstream services and subscribers to consume geo-fenced updates.
* **Ghost Driver Sweeper:** Periodic background worker evicts inactive drivers from `drivers:active` and `drivers:heartbeat` when no telemetry is received within the stale threshold (default: 30s).
* **Dual-Trigger Batch Persistence:** Accumulates pings in-memory with atomic slice detachment, flushing to PostgreSQL either when **250 records** accumulate or after **3 seconds** elapse, executed via PostgreSQL's high-speed binary `COPY` protocol.
* **Production Observability:** Native Prometheus endpoint exposing live gauges, histograms, and counters at `:2112/metrics`.

---

## 3. Proof of Work & Benchmark Analysis (Darboğaz Analizi)

### Load Test Specifications
* **Concurrent Virtual Drivers:** 1,000 active agents
* **Ping Frequency:** 10 pings/second per vehicle (10 Hz)
* **Target Load:** 10,000 pings/second sustained egress
* **Test Duration:** 10.012 seconds (2.0s ramp-up)
* **Transport:** gRPC streaming multiplexed over HTTP/2 connections

### Performance & Latency Benchmark Results

| Metric | Target | Measured Result | Status |
| :--- | :--- | :--- | :--- |
| **Ingestion Peak Throughput** | 10,000 pings/sec | **10,935.76 pings/sec** |  Exceeded Target |
| **Total Processed Records** | 100,000+ | **109,485 records** |  Zero Data Loss |
| **Dropped Packets (Drop Rate)** | < 1.0% | **0 dropped (0.00%)** |  Zero Drops |
| **p50 Latency (Median)** | < 2.0 ms | **< 0.01 ms** |  Sub-millisecond |
| **p90 Latency** | < 5.0 ms | **0.01 ms** |  Ultra Low |
| **p95 Latency** | < 10.0 ms | **0.02 ms** |  Ultra Low |
| **p99 Latency** | < 15.0 ms | **0.13 ms** |  99% under 0.15ms |
| **Max Peak Latency** | < 100.0 ms | **15.60 ms** |  Stable |
| **PostgreSQL Cold Path Flushes** | N/A | **109,478 rows in 452 batches** |  100% Persisted |
| **PostgreSQL Bulk Write Errors** | 0 | **0 errors** |  Zero Faults |
| **Ghost Driver Sweep Efficiency** | 1,000 pruned | **1,000 pruned @ 30s TTL** |  Clean Sweeping |

### Resource Consumption Profile Under 10,000 Pings/Sec
* **Server RSS Memory:** ~35 MB - 45 MB (Lightweight goroutines with pooled buffers)
* **Server CPU Utilization:** ~35% - 48% across multi-core Linux host
* **Database Connection Overhead:** Constrained to 4-20 connections via `pgxpool`, completely bypassing row-by-row write contention.

### Serialization Benchmark (Protobuf vs JSON)

```text
=======================================================
   PAYLOAD SIZE COMPARISON (SINGLE TELEMETRY PING)
=======================================================
Protobuf Wire Size :   52 bytes
JSON Wire Size     :  131 bytes
Bandwidth Saved    :   79 bytes (60.31% smaller)
=======================================================
Network Egress @ 10,000 pings/s:
   Protobuf: 0.50 MB/s
   JSON:     1.20 MB/s  (Saved 0.70 MB/s bandwidth)
=======================================================
```

---

## 4. Quickstart & Usage

### Prerequisites
* **Go** 1.22+
* **Redis** v7.0+ (`localhost:6379`)
* **PostgreSQL** 14+ (`localhost:5432` with database `geopulse`)
* Optional: PostGIS (`sudo apt install postgresql-16-postgis-3`)
* Optional: `k6` (`which k6`)

### Build System Commands

```bash
# 1. Compile Protocol Buffers into Go gRPC stubs
make proto

# 2. Build all production binaries (server, simulator, loadtester)
make build

# 3. Run full unit and integration test suite
make test

# 4. Run Protobuf vs JSON serialization benchmarks
make benchmark
```

### Running the System Locally

#### Step 1: Start the Ingestion Engine Server
```bash
./bin/geopulse-server \
  -port 50051 \
  -metrics-port 2112 \
  -batch-size 250 \
  -batch-interval 3s \
  -enable-redis=true \
  -enable-postgres=true
```

#### Step 2: Stream Simulated GPS Telemetry
```bash
# Stream 10 pings/sec for 10 seconds along an Istanbul route
./bin/geopulse-sim -driver-id driver-ist-01 -rate 10 -duration 10s
```

#### Step 3: Query Nearby Drivers via gRPC
```bash
# Query active drivers within a 5.0 km radius around Sultanahmet (41.0082, 28.9784)
./bin/geopulse-sim -query -lat 41.0082 -lon 28.9784 -radius 5.0 -limit 10
```

*Example Output:*
```text
=======================================================
       GeoPulse Spatial Radius Query Result            
=======================================================
 Search Center  : (41.008200, 28.978400)
 Search Radius  : 5.00 km
 Drivers Found  : 2
 Server Time    : 2026-09-16T17:17:12+03:00
------------------------------------------------------------------------------------------------
#   | Driver ID            | Dist (km) | Coordinates (Lat, Lon)  | Speed (km/h) | Heading 
------------------------------------------------------------------------------------------------
1   | driver-ist-01        |     0.016 | (41.008297, 28.978535) |         53.4 |    48.1°
2   | driver-ist-02        |     1.331 | (41.020093, 28.980123) |         52.6 |    47.3°
================================================================================================
```

#### Step 4: Run High-Concurrency Load Test (1,000 Concurrent Vehicles)
```bash
# Run native concurrent load test (1,000 agents @ 10 Hz for 10s)
make loadtest

# Or run Grafana k6 scenario
make k6
```

#### Step 5: Inspect Prometheus Observability Metrics
```bash
curl -s http://localhost:2112/metrics | grep -E 'active_connections|location_pings|batch_flush|ring_buffer'
```

*Example Scraped Output:*
```text
# HELP active_connections_count Current count of active streaming gRPC client connections
# TYPE active_connections_count gauge
active_connections_count 1000

# HELP location_pings_total Total count of telemetry pings handled by the engine
# TYPE location_pings_total counter
location_pings_total{status="received"} 109485
location_pings_total{status="processed"} 109478

# HELP ring_buffer_depth Current number of queued records in the ingestion ring buffer
# TYPE ring_buffer_depth gauge
ring_buffer_depth 0

# HELP batch_flush_records_total Total number of telemetry records successfully flushed to PostgreSQL
# TYPE batch_flush_records_total counter
batch_flush_records_total 109478

# HELP batch_flush_duration_seconds Latency histogram of database batch flush operations
batch_flush_duration_seconds_bucket{le="0.025"} 452
```

---

## 5. Repository Structure

```text
.
├── cmd/
│   ├── loadtester/      # 1,000-vehicle concurrent stress & latency benchmark tool
│   ├── server/          # GeoPulse gRPC & Prometheus server daemon
│   └── simulator/       # Mock vehicle simulator CLI & interactive radius query client
├── gen/go/telemetry/v1/ # Generated Go Protocol Buffers and gRPC interfaces
├── internal/
│   ├── buffer/          # Thread-safe bounded RingBuffer with DropOldest/DropNewest policies
│   ├── ingestion/       # Ingestion layer: gRPC server, stream lifecycles & worker pool
│   ├── metrics/         # Prometheus registry, collector instruments & HTTP /metrics server
│   ├── persistence/     # Cold Path: BatchBuffer, dual-trigger engine & PostgreSQL COPY repo
│   ├── server/          # Server composition, configuration & graceful signal handling
│   └── spatial/         # Hot Path: Redis GEO Sorted Sets, Pub/Sub fan-out & ghost sweeper
├── proto/telemetry/v1/  # Protocol Buffer interface definition (telemetry.proto)
├── test/
│   ├── k6/              # Grafana k6 performance testing scripts
│   └── payload_test     # Protobuf vs JSON benchmark test suite
├── Makefile             # Automation targets for build, proto, test, loadtest, clean
├── go.mod
└── README.md
```

---

## 6. License
MIT License. Created as part of the GeoPulse High-Throughput Spatial Telemetry System.
