package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings for the backend server, importers, and security middleware.
type Config struct {
	HTTPAddr           string
	DatabaseURL        string
	ZTMStaticURL       string
	PKSStaticURL       string
	ZTMRTURL           string
	PKSRTURL           string
	StaticSyncInterval time.Duration
	RTPollInterval     time.Duration
	UserAgent          string
	TrustedProxy       string // "private" (default), "none", "all"
	RateLimitRPS       float64
	RateLimitBurst     int
	CORSAllowedOrigins []string
	Timezone           string
	LogLevel           string
}

// Load reads configuration from environment variables with safe defaults.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:           envOr("HTTP_ADDR", ":8080"),
		DatabaseURL:        strings.TrimSpace(os.Getenv("DATABASE_URL")),
		ZTMStaticURL:       envOr("ZTM_STATIC_URL", "https://kmrzeszow.live/gtfs/latest.zip"),
		PKSStaticURL:       envOr("PKS_STATIC_URL", "https://kmrzeszow.live/gtfs-pks/latest.zip"),
		ZTMRTURL:           envOr("ZTM_RT_URL", "https://kmrzeszow.live/gtfs/rt/gtfsrt.pb"),
		PKSRTURL:           envOr("PKS_RT_URL", "https://kmrzeszow.live/gtfs-pks/rt/gtfsrt.pb"),
		StaticSyncInterval: envDuration("STATIC_SYNC_INTERVAL", 6*time.Hour),
		RTPollInterval:     envDuration("RT_POLL_INTERVAL", 10*time.Second),
		UserAgent:          envOr("HTTP_USER_AGENT", "pkstt-backend/1.0 (+https://github.com/maciejdbrznsk/pkstt-backend)"),
		TrustedProxy:       strings.ToLower(envOr("TRUSTED_PROXY", "private")),
		RateLimitRPS:       envFloat("RATE_LIMIT_RPS", 15),
		RateLimitBurst:     envInt("RATE_LIMIT_BURST", 40),
		CORSAllowedOrigins: splitCSV(envOr("CORS_ALLOWED_ORIGINS", "*")),
		Timezone:           envOr("TIMEZONE", "Europe/Warsaw"),
		LogLevel:           strings.ToLower(envOr("LOG_LEVEL", "info")),
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.DatabaseURL == "" {
		return errors.New("DATABASE_URL must be set")
	}
	if c.StaticSyncInterval < time.Minute {
		return fmt.Errorf("STATIC_SYNC_INTERVAL must be >= 1m, got %s", c.StaticSyncInterval)
	}
	if c.RTPollInterval < 2*time.Second {
		return fmt.Errorf("RT_POLL_INTERVAL must be >= 2s, got %s", c.RTPollInterval)
	}
	switch c.TrustedProxy {
	case "private", "none", "all":
	default:
		return fmt.Errorf("TRUSTED_PROXY must be one of private, none, all; got %q", c.TrustedProxy)
	}
	if c.RateLimitRPS <= 0 || c.RateLimitBurst < 1 {
		return errors.New("RATE_LIMIT_RPS must be > 0 and RATE_LIMIT_BURST >= 1")
	}
	if _, err := time.LoadLocation(c.Timezone); err != nil {
		return fmt.Errorf("invalid TIMEZONE %q: %w", c.Timezone, err)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func envFloat(key string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return []string{"*"}
	}
	return out
}
