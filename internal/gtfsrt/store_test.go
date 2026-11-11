package gtfsrt

import (
	"testing"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsstatic"
	"google.golang.org/protobuf/proto"
)

func TestStoreEnrichmentAndDelayPropagation(t *testing.T) {
	store := NewStore()
	store.SetTripMeta(map[string]gtfsstatic.TripMeta{
		"ztm:226412": {
			Feed:           "ztm",
			TripID:         "226412",
			RouteID:        "19",
			RouteShortName: "19",
			RouteColor:     "D32A00",
			RouteTextColor: "FFFFFF",
			Headsign:       "Miłocińska",
			DirectionID:    0,
			Brigade:        "19/4",
		},
	})

	now := time.Date(2026, 11, 11, 18, 0, 0, 0, time.UTC)
	ts := uint64(now.Unix())
	seq20 := uint32(20)
	delay135 := int32(135)
	lat := float32(50.0492)
	lon := float32(22.0005)

	msg := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Timestamp:           &ts,
		},
		Entity: []*gtfs.FeedEntity{
			{
				Id: proto.String("tu-1"),
				TripUpdate: &gtfs.TripUpdate{
					Trip:    &gtfs.TripDescriptor{TripId: proto.String("226412")},
					Vehicle: &gtfs.VehicleDescriptor{Id: proto.String("100")},
					StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{
						{
							StopSequence: &seq20,
							Arrival:      &gtfs.TripUpdate_StopTimeEvent{Delay: &delay135},
							Departure:    &gtfs.TripUpdate_StopTimeEvent{Delay: &delay135},
						},
					},
				},
			},
			{
				Id: proto.String("v-1"),
				Vehicle: &gtfs.VehiclePosition{
					Trip:                &gtfs.TripDescriptor{TripId: proto.String("226412")},
					Vehicle:             &gtfs.VehicleDescriptor{Id: proto.String("100"), Label: proto.String("100"), LicensePlate: proto.String("RZ0100V")},
					Position:            &gtfs.Position{Latitude: &lat, Longitude: &lon},
					CurrentStopSequence: &seq20,
					Timestamp:           &ts,
				},
			},
		},
	}

	store.ApplyFeed("ztm", msg, `"etag1"`, "Wed, 11 Nov 2026 18:00:00 GMT", now)

	vehicles, stale := store.Vehicles("ztm", "19", nil, now.Add(30*time.Second))
	if stale || len(vehicles) != 1 {
		t.Fatalf("expected 1 fresh vehicle on route 19, got %d (stale=%v)", len(vehicles), stale)
	}
	v := vehicles[0]
	if v.RouteShortName != "19" || v.Brigade != "19/4" || v.Headsign != "Miłocińska" {
		t.Fatalf("vehicle metadata enrichment failed: %+v", v)
	}
	if v.DelaySeconds == nil || *v.DelaySeconds != 135 {
		t.Fatalf("expected vehicle delay 135, got %v", v.DelaySeconds)
	}

	// Delay propagates forward to stop_sequence 22, not backward to stop_sequence 18.
	sched := now.Add(5 * time.Minute)
	est22 := store.EstimateDeparture("ztm", "226412", 22, "65", sched, now.Add(30*time.Second))
	if !est22.IsRealtime || est22.DelaySeconds == nil || *est22.DelaySeconds != 135 {
		t.Fatalf("expected +135s propagated delay at seq 22, got %+v", est22)
	}
	if !est22.EstimatedTime.Equal(sched.Add(135 * time.Second)) {
		t.Fatalf("estimated time mismatch: %v", est22.EstimatedTime)
	}

	est18 := store.EstimateDeparture("ztm", "226412", 18, "60", sched, now.Add(30*time.Second))
	if est18.DelaySeconds != nil {
		t.Fatalf("expected no backward delay propagation to seq 18, got %d", *est18.DelaySeconds)
	}

	// Stale feed after maxRTFeedAge should not serve old RT data.
	estStale := store.EstimateDeparture("ztm", "226412", 22, "65", sched, now.Add(15*time.Minute))
	if estStale.IsRealtime {
		t.Fatalf("expected stale feed to fall back to scheduled, got %+v", estStale)
	}
}
