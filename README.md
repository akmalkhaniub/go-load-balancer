# 🚀 Go High-Concurrency Reverse Proxy & Load Balancer

A production-grade, highly-concurrent HTTP reverse proxy and layer-7 load balancer built with Go standard library primitives. Demonstrates core Go strengths: lightweight goroutines, channel-based coordination, atomic operations, token-bucket rate limiting, active background health checkers, and dynamic upstream routing.

---

## ✨ Key Features & Go Paradigms Showcased

- **Multiple Balancing Algorithms:**
  - **Round-Robin:** Atomic index distribution (`sync/atomic`) ensuring zero lock contention on hot paths.
  - **Least-Connections:** Dynamic routing based on real-time in-flight request tracking across backends.
  - **IP-Hash:** Consistent client-to-backend affinity based on FNV-32a hashing.
- **Active Background Health Checks:**
  - Background goroutine running on a `time.Ticker` verifying upstream reachability via TCP socket probing with `context.WithTimeout`.
  - Automatic fault isolation: dead backends are automatically bypassed and restored seamlessly upon recovery.
- **Per-IP Token Bucket Rate Limiter:**
  - Thread-safe token refill rate limiter with periodic background garbage collection for inactive client states.
- **Real-Time Observability & Metrics:**
  - `/lb-status` JSON endpoint reporting live request counts, error counts, and per-backend connection statistics.
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

### 1. Run the Load Balancer (with built-in mock backends)

```bash
go run main.go --port=8080 --algo=round-robin --mock=true
```

### 2. Send Requests

```bash
curl http://localhost:8080/
```

**Sample Output:**
```json
{
  "message": "Hello from mock upstream backend!",
  "server_port": 9001,
  "path": "/",
  "time": "2026-08-30T02:50:00Z"
}
```

Repeated requests will cycle between ports `9001`, `9002`, and `9003`.

### 3. Check Live Stats & Metrics

```bash
curl http://localhost:8080/lb-status
```

**Sample Metrics Output:**
```json
{
  "uptime_seconds": 45.2,
  "total_requests": 1250,
  "total_errors": 0,
  "rate_limited": 0,
  "backends": [
    { "url": "http://127.0.0.1:9001", "alive": true, "active_connections": 2, "total_served": 418 },
    { "url": "http://127.0.0.1:9002", "alive": true, "active_connections": 1, "total_served": 416 },
    { "url": "http://127.0.0.1:9003", "alive": true, "active_connections": 0, "total_served": 416 }
  ]
}
```

---

## ⚙️ Configuration Flags

| Flag | Default | Description |
|---|---|---|
| `--port` | `8080` | Port for the reverse proxy listener |
| `--backends` | `http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003` | Comma-separated list of upstream URLs |
| `--algo` | `round-robin` | Balancing strategy (`round-robin`, `least-connections`, `ip-hash`) |
| `--health-interval` | `5s` | Frequency of TCP health checks |
| `--rate-limit` | `50.0` | Maximum allowed requests per second per IP (0 to disable) |
| `--rate-burst` | `100.0` | Burst capacity for token bucket limiter |
| `--mock` | `true` | Launch local mock servers for testing |

---

## 🧪 Running Tests & Concurrency Benchmarks

```bash
# Run unit & concurrency tests
go test -v ./...

# Run with race detector
go test -race ./...
```

---

## 📄 License
MIT License.
