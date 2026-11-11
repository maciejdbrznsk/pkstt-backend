package api

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxTrackedIPs = 100_000
	ipEntryTTL    = 5 * time.Minute
)

type visitorBucket struct {
	tokens   float64
	lastSeen time.Time
}

// RateLimiter enforces per-IP token-bucket rate limits with bounded memory and periodic eviction.
type RateLimiter struct {
	mu           sync.Mutex
	visitors     map[string]*visitorBucket
	rps          float64
	burst        float64
	trustedProxy string
}

// NewRateLimiter creates a per-IP token-bucket limiter and starts background cleanup until ctx cancels.
func NewRateLimiter(ctx context.Context, rps float64, burst int, trustedProxy string) *RateLimiter {
	rl := &RateLimiter{
		visitors:     make(map[string]*visitorBucket),
		rps:          rps,
		burst:        float64(burst),
		trustedProxy: trustedProxy,
	}
	go rl.cleanupLoop(ctx)
	return rl
}

func (rl *RateLimiter) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			rl.mu.Lock()
			for ip, v := range rl.visitors {
				if now.Sub(v.lastSeen) > ipEntryTTL {
					delete(rl.visitors, ip)
				}
			}
			rl.mu.Unlock()
		}
	}
}

// Allow consumes 1 token for ip and returns whether allowed, remaining tokens, and retry seconds.
func (rl *RateLimiter) Allow(ip string, now time.Time) (allowed bool, remaining int, retryAfterSec int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.visitors[ip]
	if !ok {
		if len(rl.visitors) >= maxTrackedIPs {
			// Evict oldest subset or reset map defensively against IP spray floods.
			for k, v := range rl.visitors {
				if now.Sub(v.lastSeen) > 30*time.Second {
					delete(rl.visitors, k)
				}
			}
			if len(rl.visitors) >= maxTrackedIPs {
				clear(rl.visitors)
			}
		}
		b = &visitorBucket{tokens: rl.burst, lastSeen: now}
		rl.visitors[ip] = b
	}

	elapsed := now.Sub(b.lastSeen).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * rl.rps
		if b.tokens > rl.burst {
			b.tokens = rl.burst
		}
	}
	b.lastSeen = now

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true, int(b.tokens), 0
	}

	needed := (1.0 - b.tokens) / rl.rps
	retry := int(needed) + 1
	return false, 0, retry
}

// Middleware rejects abusive request bursts with HTTP 429 and Retry-After headers.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := ExtractClientIP(r, rl.trustedProxy)
		allowed, rem, retry := rl.Allow(ip, time.Now())

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(int(rl.burst)))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(rem))

		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(retry))
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded; slow down")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ExtractClientIP extracts the client IP safely, only trusting CF-Connecting-IP / X-Forwarded-For from trusted proxies.
func ExtractClientIP(r *http.Request, trustedProxy string) string {
	remoteIP := parseRemoteIP(r.RemoteAddr)
	if trustedProxy == "none" || !isProxyTrusted(remoteIP, trustedProxy) {
		return remoteIP
	}

	if cfIP := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); cfIP != "" {
		if addr, err := netip.ParseAddr(cfIP); err == nil {
			return addr.Unmap().String()
		}
	}

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if addr, err := netip.ParseAddr(strings.TrimSpace(first)); err == nil {
			return addr.Unmap().String()
		}
	}

	return remoteIP
}

func parseRemoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	if addr, err := netip.ParseAddr(strings.TrimSpace(host)); err == nil {
		return addr.Unmap().String()
	}
	return "unknown"
}

func isProxyTrusted(remoteIP, mode string) bool {
	if mode == "all" {
		return true
	}
	addr, err := netip.ParseAddr(remoteIP)
	if err != nil {
		return false
	}
	addr = addr.Unmap()
	return addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast()
}
