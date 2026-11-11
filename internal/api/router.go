package api

import (
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/maciejdbrznsk/pkstt-backend/internal/config"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsrt"
)

// NewRouter builds the HTTP router with security headers, CORS, logging, and per-IP rate limiting.
func NewRouter(cfg config.Config, pool *pgxpool.Pool, rtStore *gtfsrt.Store, limiter *RateLimiter, loc *time.Location, log *slog.Logger) http.Handler {
	h := &Handler{
		pool:    pool,
		rtStore: rtStore,
		loc:     loc,
		log:     log,
		cfg:     cfg,
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(securityHeadersMiddleware)
	r.Use(corsMiddleware(cfg.CORSAllowedOrigins))
	r.Use(requestLoggerMiddleware(log, cfg.TrustedProxy))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.Healthz)

	r.Route("/v1", func(v1 chi.Router) {
		v1.Use(limiter.Middleware)

		v1.Get("/meta", h.Meta)
		v1.Get("/stop-groups", h.ListStopGroups)
		v1.Get("/stop-groups/{groupID}", h.GetStopGroup)
		v1.Get("/stop-groups/{groupID}/departures", h.GetStopGroupDepartures)
		v1.Get("/stops/{stopID}/departures", h.GetStopDepartures)
		v1.Get("/routes", h.ListRoutes)
		v1.Get("/routes/{routeID}", h.GetRoute)
		v1.Get("/trips/{tripID}", h.GetTrip)
		v1.Get("/shapes/{shapeID}", h.GetShape)
		v1.Get("/vehicles", h.ListVehicles)
		v1.Get("/alerts", h.ListAlerts)
		v1.Get("/fares", h.ListFares)
	})

	return r
}

func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		next.ServeHTTP(w, r)
	})
}

func corsMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	allowAll := slices.Contains(allowedOrigins, "*")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if allowAll {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else if origin != "" && slices.Contains(allowedOrigins, origin) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, If-None-Match")
			w.Header().Set("Access-Control-Max-Age", "3600")

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func requestLoggerMiddleware(log *slog.Logger, trustedProxy string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			if r.URL.Path == "/healthz" && ww.Status() < 400 {
				return
			}
			log.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"query", r.URL.RawQuery,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration_ms", time.Since(start).Milliseconds(),
				"ip", ExtractClientIP(r, trustedProxy),
				"req_id", middleware.GetReqID(r.Context()),
			)
		})
	}
}
