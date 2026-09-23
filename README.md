# Multi-Tenant Core Engine with PostgreSQL RLS & Relation-Based Access Control (ReBAC)

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)](https://www.postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis)](https://redis.io)
[![gRPC](https://img.shields.io/badge/gRPC-v1-244c5a?style=flat&logo=grpc)](https://grpc.io)
[![k6](https://img.shields.io/badge/k6-Load_Testing-7D64FF?style=flat&logo=k6)](https://k6.io)
An enterprise-grade, high-performance **Multi-Tenant Core Engine** engineered in Go. It offloads multi-tenant data isolation from error-prone application code directly to the **PostgreSQL Row-Level Security (RLS)** engine, combines it with a **Google Zanzibar-inspired Relation-Based Access Control (ReBAC)** graph solver, and delivers **sub-3ms p99 latency** under extreme concurrency via multi-tier caching (L1 In-Memory + L2 Redis Cluster) and singleflight deduplication.

---

## Architecture Overview

```mermaid
flowchart TD
    Client(["HTTP / gRPC Client"]) --> |X-Tenant-ID / Token| Gateway["Tenant Guard Middleware<br/>(UUID Format & Injection)"]
    Gateway --> Service["Application Layer / gRPC Server"]

    subgraph CachingLayer ["Multi-Tier Caching & Thundering Herd Protection"]
        Service --> L1["L1 In-Memory LRU Cache<br/>(~100ns Latency)"]
        L1 -- Cache Miss --> L2["L2 Redis Cluster Cache<br/>({tenant_id} Hash-Tagged, ~1-2ms)"]
        L2 -- Cache Miss --> SF["Singleflight Deduplicator<br/>(Collapses 500+ Concurrent Requests)"]
    end

    subgraph DataLayer ["PostgreSQL 16 Engine with Physical RLS"]
        SF --> TxWrapper["WithinTenantTx Decorator<br/>(BeginTx + set_config(app.current_tenant_id, true))"]
        TxWrapper --> RLS["PostgreSQL RLS Engine<br/>(FORCE ROW LEVEL SECURITY)"]
        RLS --> ReBAC["Recursive CTE Graph Traversal<br/>(Cycle Detection + Userset Rewrites)"]
        ReBAC --> Tables[("Tables: tenants, users, documents, tuples")]
    end

    subgraph InvalidationBus ["Distributed Invalidation Pipeline"]
        Service --> |Write / Delete Tuple| Bus["Invalidation Bus"]
        Bus --> |INCR {tenant_id}:version| Zookies["Zookies O(1) Ephemeral Snapshot"]
        Bus --> |Publish tenant_invalidations| PubSub["Redis Pub/Sub (Cross-Pod L1 Sync)"]
        PubSub --> L1
    end
```

---

## Key Architectural Pillars

### 1. Physical Multi-Tenant Isolation (PostgreSQL RLS)
- **Elimination of Human Error:** Developers do not need to manually append `WHERE tenant_id = ?` to queries. The database engine physically enforces row filtering via `USING` and `WITH CHECK` policies.
- **`FORCE ROW LEVEL SECURITY`:** Ensures that table owners and migration roles cannot bypass security policies.
- **Connection Pool Scoping (`SET LOCAL` via `set_config`):**
  > In PostgreSQL, standard `SET LOCAL param = $1` does not accept parameterized bindings. Using formatted string interpolation introduces SQL injection risks, while a global `SET` causes catastrophic **Context Leakage** when connections are recycled in a connection pool (`pgxpool`).
  >
  > **Solution:** We use `SELECT set_config('app.current_tenant_id', $1, true);` where `is_local = true` ensures parameters are SQL-injection proof and **strictly scoped to the transaction boundary**. Upon Commit or Rollback, the parameter is instantly discarded by PostgreSQL.
- **Dedicated Non-Superuser Role (`app_user`):** Superusers bypass RLS by design in PostgreSQL. The engine runs under a constrained `app_user` role, guaranteeing 100% policy enforcement.

---

### 2. Google Zanzibar ReBAC Engine
- **Relational Tuple Schema:** Canonical Zanzibar tuple schema:
  $$\langle \text{entity\_type} \rangle : \langle \text{entity\_id} \rangle \# \langle \text{relation} \rangle @ \langle \text{subject\_type} \rangle : \langle \text{subject\_id} \rangle [\# \langle \text{subject\_relation} \rangle]$$
- **Composite Indexing:**
  - Lookup Index: `(tenant_id, entity_type, entity_id, relation)`
  - Reverse Index: `(tenant_id, subject_type, subject_id)`
  - Userset Indirection Index: `(tenant_id, subject_type, subject_id, subject_relation)`
- **Declarative Authorization Model DSL:**
  - Direct Assignment (`this`)
  - Computed Userset (e.g. `owner` implies `editor`, `editor` implies `viewer`)
  - Tuple-to-Userset (e.g. `document#parent -> folder#viewer`)
  - Set Operators: `Union` (OR) and `Intersection` (AND)
- **Dual Graph Traversal Engines:**
  1. **PostgreSQL Recursive CTE:** Traverses transitive relationships directly inside the database with `CYCLE` detection (`NOT node = ANY(path)` and depth limiting).
  2. **Go In-Memory BFS Solver:** Evaluates the schema graph in-process with a queue and `visited` set cycle prevention.
- **gRPC Check API:** Standard Protobuf definition for high-throughput microsecond-level permission evaluations.

---

### 3. Tenant-Aware Multi-Tier Caching & Invalidation
- **L1 In-Memory LRU:** In-process caching (`hashicorp/golang-lru/v2`) providing **~100ns** read latency.
- **L2 Distributed Redis:** Cross-instance cache (`go-redis/v9`) providing **~1-2ms** read latency.
- **Redis Cluster Hash-Tagging:**
  - Key format: `{[tenant_id]}:v[version]:rebac:check:[sha256_hash]`
  - Curly brackets `{tenant_id}` guarantee that all keys for the same tenant map to the exact same Redis hash slot (shard), enabling atomic multi-key operations and pipelining per tenant.
- **Asymmetric TTL Strategy:**
  - Positive permissions (`allowed = true`): **10 minutes**
  - Negative permissions (`allowed = false`): **30 seconds** (mitigates permission grant lag while shielding the DB from brute-force authorization attacks).
- **Zookies Ephemeral Snapshot Invalidation ($O(1)$ Complexity):**
  - When relations change, `INCR {tenant_id}:version` increments the tenant's epoch.
  - All subsequent checks use the new version in their key format, immediately rendering all prior keys obsolete in **$O(1)$ time** without needing expensive `SCAN` or `KEYS` operations.
- **Redis Pub/Sub Invalidation Bus:** Broadcasts invalidations across all application pods to immediately purge local L1 caches.
- **Thundering Herd Protection via `singleflight`:** When a cache miss occurs, 500 concurrent requests for the same permission are collapsed into **exactly 1 database query**.

---

## Performance & Benchmark Metrics

| Metric | Target | Result | Mechanism |
|---|---|---|---|
| **L1 Cache Read Latency** | $< 1\,\mu\text{s}$ | **~100 ns** | In-Memory Go LRU (`golang-lru/v2`) |
| **L2 Redis Read Latency** | $< 3\text{ ms}$ | **1.2 ms** | Redis Hash-Tagged Key Lookup |
| **Cold DB Traversal (p95)** | $< 150\text{ ms}$ | **~18 ms** | Recursive CTE with composite B-Tree indexes |
| **Thundering Herd 500 VUs** | 0% Errors | **1 DB query executed** | Go `singleflight.Group` deduplication |
| **Cross-Tenant Data Leakage** | 0.00% | **0 leaks / 5,000 tx** | PostgreSQL RLS + `set_config(..., true)` |
| **Bulk Data Ingestion** | $> 25\text{k rows/s}$ | **~45k tuples/s** | PostgreSQL Binary `pgx.CopyFrom` |

---

## Project Structure

```
├── cmd/
│   ├── api/                     # Server entrypoint (HTTP REST + gRPC Check Server)
│   └── seed/                    # High-speed synthetic data generator (1,000 tenants, 500k+ tuples)
├── internal/
│   ├── database/                # pgxpool setup and WithinTenantTx transaction decorator
│   ├── document/                # Protected document models and RLS-backed repository
│   ├── http/
│   │   ├── handler/             # Document & HTTP ReBAC Check handlers
│   │   └── middleware/          # TenantGuardMiddleware (X-Tenant-ID validation)
│   ├── rebac/
│   │   ├── cache/               # L1 LRU, L2 Redis, InvalidationBus, CachedCheckEngine
│   │   ├── engine/              # Dual solvers (SQL Recursive CTE & Go In-Memory BFS)
│   │   ├── grpc/                # gRPC AuthorizationService implementation
│   │   ├── model/               # Zanzibar Tuple parser, formatter & domain models
│   │   ├── repository/          # Relation tuple database repository
│   │   ├── schema/              # Declarative Authorization Model DSL & Default Schema
│   │   └── service/             # TupleService with transactional write & auto-invalidation
│   └── tenant/                  # Tenant model & Go context propagation helpers
├── migrations/                  # Schema migrations with RLS and composite indexes
├── proto/rebac/v1/              # Protocol Buffers definition and generated Go stubs
├── scripts/                     # Docker entrypoint initialization scripts
└── test/
    ├── integration/             # Security exploit tests, context leakage, ReBAC, and benchmarks
    └── load/                    # k6 performance and load testing scripts (Cold, Warm, Herd)
```

---

## Getting Started

### 1. Requirements
- Go 1.25+
- Docker & Docker Compose
- `k6` (optional, for load testing)
- `protoc` (optional, for rebuilding Protobuf stubs)

### 2. Run with Docker Compose
```bash
# Start PostgreSQL 16 (Port 5433) and Redis 7 (Port 6379)
make up

# Check container health
docker-compose ps
```

### 3. Run the Application
```bash
# Start HTTP REST (:8080) and gRPC ReBAC Check Service (:50051)
make run
```

### 4. Execute the Security & Integration Test Suite
```bash
# Run all unit tests with Go race detector
make test

# Run Context Leakage and ReBAC integration tests
make test-integration

# Run Concurrency & Exploit Security Showcase (100 parallel workers + singleflight)
make test-concurrency
```

### 5. Generate 500,000+ Relation Tuples
```bash
# Seeds 1,000 tenants with 5-level deep folder hierarchies and 500,000+ tuples
make seed
```

### 6. Run k6 Load Tests
```bash
# Scenario 1: Cold Cache (Raw PostgreSQL RLS + Recursive CTE)
make load-cold

# Scenario 2: Warm Cache (L1 LRU + L2 Redis high concurrency)
make load-warm

# Scenario 3: Thundering Herd Test (500 simultaneous requests)
make load-thundering
```