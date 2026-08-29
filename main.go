package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/akmalkhaniub/go-load-balancer/pkg/backend"
	"github.com/akmalkhaniub/go-load-balancer/pkg/balancer"
	"github.com/akmalkhaniub/go-load-balancer/pkg/metrics"
	"github.com/akmalkhaniub/go-load-balancer/pkg/ratelimit"
)

func startMockBackends(ports []int) {
	for _, port := range ports {
		p := port
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"message":"Hello from mock upstream backend!","server_port":%d,"path":"%s","time":"%s"}`+"\n",
				p, r.URL.Path, time.Now().Format(time.RFC3339Nano))
		})

		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", p),
			Handler: mux,
		}

		go func() {
			slog.Info("Starting mock backend server", "port", p)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("Mock server error", "port", p, "err", err)
			}
		}()
	}
}

func main() {
	port := flag.Int("port", 8080, "Port for the load balancer to listen on")
	backendsFlag := flag.String("backends", "http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003", "Comma-separated list of backend URLs")
	algoFlag := flag.String("algo", "round-robin", "Balancing algorithm (round-robin, least-connections, ip-hash)")
	checkInterval := flag.Duration("health-interval", 5*time.Second, "Health check interval")
	rateLimit := flag.Float64("rate-limit", 50.0, "Requests per second per IP (0 to disable)")
	rateBurst := flag.Float64("rate-burst", 100.0, "Rate limit token bucket burst capacity")
	startMocks := flag.Bool("mock", true, "Automatically start mock backends on ports 9001, 9002, 9003 for testing")
	flag.Parse()

	// Configure structured logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("Initializing Go Load Balancer",
		"port", *port,
		"algorithm", *algoFlag,
		"backends", *backendsFlag,
	)

	// Optionally start mock upstream backends
	if *startMocks {
		startMockBackends([]int{9001, 9002, 9003})
		time.Sleep(100 * time.Millisecond) // Allow listeners to bind
	}

	algo := balancer.Algorithm(*algoFlag)
	pool := balancer.NewServerPool(algo)

	backendList := strings.Split(*backendsFlag, ",")
	for _, rawURL := range backendList {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" {
			continue
		}
		b, err := backend.NewBackend(rawURL)
		if err != nil {
			slog.Error("Failed to initialize backend", "url", rawURL, "err", err)
			os.Exit(1)
		}
		pool.AddBackend(b)
		slog.Info("Registered backend", "url", b.URL.String())
	}

	// Start background health checking
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.StartHealthCheck(ctx, *checkInterval)

	// Setup metrics collector
	metricCollector := metrics.NewMetricsCollector(pool.GetBackends)

	// Build main HTTP router
	mainMux := http.NewServeMux()
	mainMux.HandleFunc("/lb-status", metricCollector.Handler())
	mainMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}` + "\n"))
	})
	mainMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		metricCollector.IncrRequests()
		pool.ServeHTTP(w, r)
	})

	var rootHandler http.Handler = mainMux
	if *rateLimit > 0 {
		limiter := ratelimit.NewIPRateLimiter(*rateLimit, *rateBurst)
		rootHandler = limiter.Middleware(mainMux)
		slog.Info("Rate limiting active", "rps", *rateLimit, "burst", *rateBurst)
	}

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      rootHandler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Channel to listen for OS interrupt signals
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		slog.Info("Load Balancer is actively listening", "addr", fmt.Sprintf("http://localhost:%d", *port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server error", "err", err)
		}
	}()

	<-stopChan
	slog.Info("Shutting down Load Balancer gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Forced shutdown", "err", err)
	}
	slog.Info("Server stopped successfully")
}
