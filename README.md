# 🚀 Go High-Concurrency Reverse Proxy & Load Balancer (v2)

A production-grade, highly-concurrent HTTP reverse proxy and layer-7 load balancer built with Go standard library primitives. Demonstrates core Go strengths: lightweight goroutines, channel-based coordination, atomic operations, token-bucket rate limiting, active background health checkers, **Circuit Breakers**, and **Dynamic Topology Hot-Reloading**.

---

## ✨ Key Features & Go Paradigms Showcased

- **Multiple Balancing Algorithms:**
  - **Round-Robin:** Atomic index distribution (`sync/atomic`) ensuring zero lock contention on hot paths.
  - **Least-Connections:** Dynamic routing based on real-time in-flight request tracking across backends.
  - **IP-Hash:** Consistent client-to-backend affinity based on FNV-32a hashing.
- **🛡️ Circuit Breaker Pattern (`pkg/circuit`):**
  - Tracks consecutive errors per backend (`Closed` -> `Open` -> `Half-Open`).
  - Automatically trips to protect overloaded upstreams from cascading failures, allowing probe requests during cooldown.
- **⚡ Dynamic Runtime Topology API (`pkg/api`):**
  - Add (`POST /api/backends`), remove (`DELETE /api/backends`), and gracefully drain (`POST /api/backends/drain`) backends on the fly without restarting the process.
- **Active Background Health Checks:**
  - Background goroutine running on a `time.Ticker` verifying upstream reachability via TCP socket probing with `context.WithTimeout`.
- **Per-IP Token Bucket Rate Limiter:**
  - Thread-safe token refill rate limiter with periodic background garbage collection for inactive client states.
- **Real-Time Observability & Metrics:**
  - `/lb-status` JSON endpoint reporting live request counts, error counts, and per-backend connection & circuit breaker statistics.
- **Graceful Shutdown:**
  - Traps `SIGINT`/`SIGTERM` via `os/signal` to complete in-flight transactions before closing listeners.
- **Built-in Mock Backends (`--mock`):**
  - Instant zero-dependency evaluation mode that launches 3 mock upstream servers automatically.

---

## 🏗️ Architecture

```
                                  +-----------------------+
                                  |   Incoming Clients    |
                                  +-----------+-----------+
                                              |
                                              v
                              +-------------------------------+
                              |    IP Token Bucket Limiter    |
                              +---------------+---------------+
                                              |
                                              v
                              +-------------------------------+
                              |       Server Pool Router      |
                              |  (Round-Robin / Least-Conns)  |
                              +---------------+---------------+
                                  /           |           \
                                 /            |            \
                                v             v             v
                       +-------------+ +-------------+ +-------------+
                       |  Backend 1  | |  Backend 2  | |  Backend 3  |
                       | [Breaker:OK]| | [Breaker:OK]| | [Breaker:OK]|
                       |  (:9001)    | |  (:9002)    | |  (:9003)    |
                       +------+------+ +------+------+ +------+------+
                              ^               ^               ^
                              |               |               |
                              +---------------+---------------+
                                      TCP Health Checker
                                   (Runs in Background)
```

---

## 🚀 Quick Start

### 1. Run the Load Balancer

```bash
go run main.go --port=8080 --algo=round-robin --mock=true
```

### 2. Dynamically Register a New Backend at Runtime

```bash
curl -X POST http://localhost:8080/api/backends \
  -H "Content-Type: application/json" \
  -d '{"url":"http://127.0.0.1:9004"}'
```

### 3. Gracefully Drain a Backend

```bash
curl -X POST http://localhost:8080/api/backends/drain \
  -H "Content-Type: application/json" \
  -d '{"url":"http://127.0.0.1:9001"}'
```

### 4. Query Real-Time Metrics & Breaker States

```bash
curl http://localhost:8080/api/backends
```

**Sample Output:**
```json
[
  {
    "url": "http://127.0.0.1:9001",
    "alive": false,
    "draining": true,
    "breaker_state": "CLOSED",
    "active_connections": 0,
    "total_served": 418
  },
  {
    "url": "http://127.0.0.1:9002",
    "alive": true,
    "draining": false,
    "breaker_state": "CLOSED",
    "active_connections": 1,
    "total_served": 416
  }
]
```

---

## 🧪 Running Tests

```bash
go test -v ./...
go test -race ./...
```

---

## 📄 License
MIT License.
