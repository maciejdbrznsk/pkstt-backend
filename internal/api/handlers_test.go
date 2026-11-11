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

func TestBuildBaysMergesSharedPlatformAndPreservesDistinctNumbers(t *testing.T) {
	platforms := []platformDTO{
		{
			ID:           "ztm:65",
			Operator:     "ztm",
			Name:         "Marszałkowska 01",
			PlatformCode: "01",
			Lat:          50.04100,
			Lon:          22.00100,
			SampleDirections: []sampleDirectionDTO{
				{Operator: "ztm", RouteID: "ztm:18", RouteShortName: "18", Headsign: "Łukasiewicza", TripCount: 40},
			},
		},
		{
			ID:           "pks:199",
			Operator:     "pks",
			Name:         "Marszałkowska 01",
			PlatformCode: "01",
			Lat:          50.04108,
			Lon:          22.00105,
			SampleDirections: []sampleDirectionDTO{
				{Operator: "pks", RouteID: "pks:202", RouteShortName: "202", Headsign: "Tyczyn", TripCount: 15},
			},
		},
		{
			ID:           "ztm:66",
			Operator:     "ztm",
			Name:         "Marszałkowska 02",
			PlatformCode: "02",
			Lat:          50.04120,
			Lon:          22.00130,
			SampleDirections: []sampleDirectionDTO{
				{Operator: "ztm", RouteID: "ztm:18", RouteShortName: "18", Headsign: "Obr. Poczty Gdańskiej", TripCount: 38},
			},
		},
		{
			ID:           "pks:205",
			Operator:     "pks",
			Name:         "Marszałkowska 03",
			PlatformCode: "03",
			Lat:          50.04145,
			Lon:          22.00150,
			SampleDirections: []sampleDirectionDTO{
				{Operator: "pks", RouteID: "pks:202", RouteShortName: "202", Headsign: "Rzeszów DA", TripCount: 22},
			},
		},
	}

	bays := buildBays("Marszałkowska", platforms)
	if len(bays) != 3 {
		t.Fatalf("expected 3 bays (01 merged + 02 + 03), got %d: %+v", len(bays), bays)
	}
	if bays[0].BayID != "01" || len(bays[0].StopIDs) != 2 || len(bays[0].Operators) != 2 {
		t.Fatalf("expected merged bay 01 with 2 stop_ids and 2 operators, got %+v", bays[0])
	}
	if bays[1].BayID != "02" || bays[2].BayID != "03" {
		t.Fatalf("expected bays 02 and 03 distinct, got %+v, %+v", bays[1], bays[2])
	}
}

func TestBuildRouteTimetableDaysAndLegend(t *testing.T) {
	deps := []rawTimetableDep{
		{TripID: "ztm:t1", ServiceID: "PS", Headsign: "Lwowska Szpital", DestStopName: "Lwowska Szpital", DepartureSecs: 6*3600 + 15*60, WheelchairAccessible: true},
		{TripID: "ztm:t2", ServiceID: "PS", Headsign: "Lwowska Szpital", DestStopName: "Lwowska Szpital", DepartureSecs: 7*3600 + 30*60, WheelchairAccessible: true},
		{TripID: "ztm:t3", ServiceID: "PS", Headsign: "Baligrodzka", DestStopName: "Baligrodzka", DepartureSecs: 24*3600 + 10*60},
		{TripID: "ztm:t4", ServiceID: "PW", Headsign: "Lwowska Szpital", DestStopName: "Lwowska Szpital", DepartureSecs: 8 * 3600},
		{TripID: "ztm:t5", ServiceID: "SS", Headsign: "Lwowska Szpital", DestStopName: "Lwowska Szpital", DepartureSecs: 9*3600 + 5*60},
	}

	days, legend := buildRouteTimetableDays(deps, map[string]bool{"PS": true})
	if len(days) != 3 {
		t.Fatalf("expected 3 service day types (PS, PW, SS), got %d", len(days))
	}
	if days[0].ServiceID != "PS" || !days[0].ActiveToday || days[1].ServiceID != "PW" || days[2].ServiceID != "SS" {
		t.Fatalf("unexpected day ordering or active flags: %+v", days)
	}
	if len(days[0].Hours) != 3 {
		t.Fatalf("expected 3 hour rows for PS, got %d", len(days[0].Hours))
	}
	postMidnight := days[0].Hours[2]
	if postMidnight.Hour != 24 || postMidnight.DisplayHour != 0 || postMidnight.Departures[0].Minute != 10 {
		t.Fatalf("unexpected post-midnight row: %+v", postMidnight)
	}
	if postMidnight.Departures[0].VariantCode != "A" {
		t.Fatalf("expected secondary variant code A, got %q", postMidnight.Departures[0].VariantCode)
	}
	if days[0].Hours[0].Departures[0].VariantCode != "" {
		t.Fatalf("expected primary variant to have empty code, got %q", days[0].Hours[0].Departures[0].VariantCode)
	}
	if len(legend) != 1 || legend[0].Code != "A" {
		t.Fatalf("expected 1 legend entry for code A, got %+v", legend)
	}
}

func TestParseCanonicalIDHandlesEncodedColon(t *testing.T) {
	cases := []struct {
		in      string
		wantF   string
		wantID  string
		wantOK  bool
	}{
		{"ztm:18", "ztm", "18", true},
		{"ztm%3A18", "ztm", "18", true},
		{"pks%3A202", "pks", "202", true},
		{"ztm:225838", "ztm", "225838", true},
		{"invalid", "", "", false},
		{"other:18", "", "", false},
	}
	for _, tc := range cases {
		f, id, ok := parseCanonicalID(tc.in)
		if f != tc.wantF || id != tc.wantID || ok != tc.wantOK {
			t.Fatalf("parseCanonicalID(%q) = (%q,%q,%v), want (%q,%q,%v)", tc.in, f, id, ok, tc.wantF, tc.wantID, tc.wantOK)
		}
	}
}
