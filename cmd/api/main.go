package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/maciejdbrznsk/pkstt-backend/internal/api"
	"github.com/maciejdbrznsk/pkstt-backend/internal/config"
	"github.com/maciejdbrznsk/pkstt-backend/internal/db"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsrt"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsstatic"
	"github.com/maciejdbrznsk/pkstt-backend/internal/scheduler"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	log := newLogger(cfg.LogLevel)
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		log.Error("load timezone failed", "timezone", cfg.Timezone, "error", err)
		os.Exit(1)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := connectPoolWithRetry(rootCtx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := db.Migrate(rootCtx, pool); err != nil {
		log.Error("database migration failed", "error", err)
		os.Exit(1)
	}
	log.Info("database migrations applied")

	rtStore := gtfsrt.NewStore()
	importer := gtfsstatic.NewImporter(pool, cfg.UserAgent, log)
	poller := gtfsrt.NewPoller(rtStore, cfg.UserAgent, log)
	runner := scheduler.NewRunner(cfg, importer, poller, rtStore, log)

	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		runner.Start(rootCtx)
	}()

	limiter := api.NewRateLimiter(rootCtx, cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.TrustedProxy)
	router := api.NewRouter(cfg, pool, rtStore, limiter, loc, log)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	srvErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
		close(srvErr)
	}()

	select {
	case <-rootCtx.Done():
		log.Info("shutdown signal received")
	case err := <-srvErr:
		if err != nil {
			log.Error("http server error", "error", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http server shutdown error", "error", err)
	}

	select {
	case <-schedulerDone:
	case <-time.After(10 * time.Second):
		log.Warn("background scheduler timed out during shutdown")
	}

	log.Info("server stopped cleanly")
}

func newLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch levelStr {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func connectPoolWithRetry(ctx context.Context, dsn string, log *slog.Logger) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = 20
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	var lastErr error
	for attempt := 1; attempt <= 15; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			pingErr := pool.Ping(pingCtx)
			cancel()
			if pingErr == nil {
				return pool, nil
			}
			lastErr = pingErr
			pool.Close()
		} else {
			lastErr = err
		}
		log.Warn("waiting for postgres", "attempt", attempt, "error", lastErr)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, lastErr
}
