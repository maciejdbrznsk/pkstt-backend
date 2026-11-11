package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/maciejdbrznsk/pkstt-backend/internal/config"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsrt"
	"google.golang.org/protobuf/proto"
)

func TestExtractClientIP(t *testing.T) {
	mkReq := func(remoteAddr, cfIP, xff string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/v1/vehicles", nil)
		req.RemoteAddr = remoteAddr
		if cfIP != "" {
			req.Header.Set("CF-Connecting-IP", cfIP)
		}
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}
		return req
	}

	// Trusted Docker/loopback proxy (cloudflared) -> trusts CF-Connecting-IP.
	if got := ExtractClientIP(mkReq("172.18.0.5:41234", "203.0.113.10", "198.51.100.2"), "private"); got != "203.0.113.10" {
		t.Fatalf("expected CF-Connecting-IP from private proxy, got %q", got)
	}

	// Untrusted public peer attempting header spoofing -> ignores CF-Connecting-IP.
	if got := ExtractClientIP(mkReq("198.51.100.99:5555", "203.0.113.10", ""), "private"); got != "198.51.100.99" {
		t.Fatalf("expected direct peer IP when proxy untrusted, got %q", got)
	}

	// mode="none" always ignores proxy headers even from loopback.
	if got := ExtractClientIP(mkReq("127.0.0.1:5555", "203.0.113.10", ""), "none"); got != "127.0.0.1" {
		t.Fatalf("expected loopback IP when mode=none, got %q", got)
	}
}

func TestRateLimiterAllowAndHTTP429(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rl := NewRateLimiter(ctx, 1, 2, "none")
	now := time.Now()
	if ok, _, _ := rl.Allow("1.2.3.4", now); !ok {
		t.Fatal("first request should succeed")
	}
	if ok, _, _ := rl.Allow("1.2.3.4", now); !ok {
		t.Fatal("second request in burst should succeed")
	}
	if ok, _, retry := rl.Allow("1.2.3.4", now); ok || retry < 1 {
		t.Fatalf("third request should be rate-limited, got ok=%v retry=%d", ok, retry)
	}
}

func TestVehiclesEndpointETagAndSecurityHeaders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rtStore := gtfsrt.NewStore()
	now := time.Now().UTC()
	ts := uint64(now.Unix())
	lat := float32(50.04)
	lon := float32(22.00)
	rtStore.ApplyFeed("pks", &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Timestamp:           &ts,
		},
		Entity: []*gtfs.FeedEntity{
			{
				Id: proto.String("v1"),
				Vehicle: &gtfs.VehiclePosition{
					Vehicle:  &gtfs.VehicleDescriptor{Id: proto.String("R9020")},
					Position: &gtfs.Position{Latitude: &lat, Longitude: &lon},
				},
			},
		},
	}, "", "", now)

	cfg := config.Config{
		TrustedProxy:       "private",
		RateLimitRPS:       10,
		RateLimitBurst:     10,
		CORSAllowedOrigins: []string{"*"},
		Timezone:           "Europe/Warsaw",
	}
	limiter := NewRateLimiter(ctx, cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.TrustedProxy)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := NewRouter(cfg, nil, rtStore, limiter, time.UTC, logger)

	req1 := httptest.NewRequest(http.MethodGet, "/v1/vehicles", nil)
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec1.Code, rec1.Body.String())
	}
	if rec1.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("missing security header X-Content-Type-Options")
	}
	etag := rec1.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header on /v1/vehicles")
	}

	req2 := httptest.NewRequest(http.MethodGet, "/v1/vehicles", nil)
	req2.Header.Set("If-None-Match", etag)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified on matching ETag, got %d", rec2.Code)
	}

	// Bad canonical ID validation
	reqBad := httptest.NewRequest(http.MethodGet, "/v1/stops/invalid-id/departures", nil)
	recBad := httptest.NewRecorder()
	router.ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for malformed stopID, got %d", recBad.Code)
	}
}
