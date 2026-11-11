package gtfsrt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsstatic"
)

const maxRTFeedAge = 10 * time.Minute

// Vehicle represents a live vehicle position enriched with static route/trip metadata.
type Vehicle struct {
	ID                  string    `json:"id"`
	Operator            string    `json:"operator"`
	VehicleID           string    `json:"vehicle_id"`
	Label               string    `json:"label,omitempty"`
	LicensePlate        string    `json:"license_plate,omitempty"`
	TripID              string    `json:"trip_id,omitempty"`
	RouteID             string    `json:"route_id,omitempty"`
	RouteShortName      string    `json:"route_short_name,omitempty"`
	RouteColor          string    `json:"route_color,omitempty"`
	RouteTextColor      string    `json:"route_text_color,omitempty"`
	Headsign            string    `json:"headsign,omitempty"`
	Brigade             string    `json:"brigade,omitempty"`
	DirectionID         int       `json:"direction_id"`
	Lat                 float64   `json:"lat"`
	Lon                 float64   `json:"lon"`
	Bearing             *float32  `json:"bearing,omitempty"`
	Speed               *float32  `json:"speed,omitempty"`
	CurrentStopSequence *uint32   `json:"current_stop_sequence,omitempty"`
	StopID              string    `json:"stop_id,omitempty"`
	CurrentStatus       string    `json:"current_status,omitempty"`
	DelaySeconds        *int      `json:"delay_seconds,omitempty"`
	Timestamp           time.Time `json:"timestamp"`
}

// StopTimeUpdate mirrors a GTFS-RT StopTimeUpdate for delay propagation.
type StopTimeUpdate struct {
	StopSequence   int32
	StopID         string
	ArrivalDelay   *int32
	ArrivalTime    *int64
	DepartureDelay *int32
	DepartureTime  *int64
	Skipped        bool
}

// TripUpdate holds real-time schedule adjustments for a single trip.
type TripUpdate struct {
	Canceled    bool
	TripDelay   *int
	VehicleID   string
	StopUpdates []StopTimeUpdate
	Timestamp   time.Time
}

// Alert represents an active GTFS-RT Service Alert.
type Alert struct {
	ID          string           `json:"id"`
	Operator    string           `json:"operator"`
	Header      string           `json:"header"`
	Description string           `json:"description,omitempty"`
	URL         string           `json:"url,omitempty"`
	Cause       string           `json:"cause,omitempty"`
	Effect      string           `json:"effect,omitempty"`
	ActiveFrom  *time.Time       `json:"active_from,omitempty"`
	ActiveTo    *time.Time       `json:"active_to,omitempty"`
	Entities    []InformedEntity `json:"entities,omitempty"`
}

// InformedEntity identifies a route, stop, or trip affected by an alert.
type InformedEntity struct {
	RouteID string `json:"route_id,omitempty"`
	StopID  string `json:"stop_id,omitempty"`
	TripID  string `json:"trip_id,omitempty"`
}

// FeedStatus captures freshness and diagnostic metrics for one operator's RT feed.
type FeedStatus struct {
	Feed            string     `json:"feed"`
	LastCheckedAt   *time.Time `json:"last_checked_at,omitempty"`
	LastUpdatedAt   *time.Time `json:"last_updated_at,omitempty"`
	FeedTimestamp   *time.Time `json:"feed_timestamp,omitempty"`
	LastError       string     `json:"last_error,omitempty"`
	VehicleCount    int        `json:"vehicle_count"`
	TripUpdateCount int        `json:"trip_update_count"`
	AlertCount      int        `json:"alert_count"`
	Stale           bool       `json:"stale"`
}

type feedSnapshot struct {
	vehicles       []Vehicle
	vehiclesByTrip map[string]Vehicle    // keyed by raw trip_id
	tripUpdates    map[string]TripUpdate // keyed by raw trip_id
	alerts         []Alert
	etag           string
	lastMod        string
	hash           string
	checkedAt      time.Time
	updatedAt      time.Time
	feedTS         time.Time
	lastError      string
}

// Store keeps thread-safe in-memory GTFS-RT snapshots for ZTM and PKS, plus static TripMeta for enrichment.
type Store struct {
	mu       sync.RWMutex
	feeds    map[string]*feedSnapshot
	tripMeta map[string]gtfsstatic.TripMeta
	version  string
}

// NewStore creates an empty in-memory GTFS-RT store.
func NewStore() *Store {
	return &Store{
		feeds:    make(map[string]*feedSnapshot),
		tripMeta: make(map[string]gtfsstatic.TripMeta),
		version:  "empty",
	}
}

// SetTripMeta atomically replaces the static trip metadata lookup map used to enrich RT entities.
func (s *Store) SetTripMeta(meta map[string]gtfsstatic.TripMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tripMeta = meta
}

// GetConditionalHeaders returns the last seen ETag and Last-Modified headers for a feed.
func (s *Store) GetConditionalHeaders(feed string) (etag, lastMod string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if snap, ok := s.feeds[feed]; ok {
		return snap.etag, snap.lastMod
	}
	return "", ""
}

// MarkChecked updates last checked timestamp without replacing entities (e.g., on 304 Not Modified).
func (s *Store) MarkChecked(feed string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.feeds[feed]
	if !ok {
		snap = &feedSnapshot{}
		s.feeds[feed] = snap
	}
	snap.checkedAt = now
	snap.lastError = ""
}

// MarkError records a polling error while retaining the last good snapshot.
func (s *Store) MarkError(feed string, now time.Time, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.feeds[feed]
	if !ok {
		snap = &feedSnapshot{}
		s.feeds[feed] = snap
	}
	snap.checkedAt = now
	snap.lastError = err.Error()
}

// ApplyFeed decodes a GTFS-RT FeedMessage and replaces the snapshot for the given operator feed.
func (s *Store) ApplyFeed(feed string, msg *gtfs.FeedMessage, etag, lastMod string, now time.Time) {
	s.mu.RLock()
	meta := s.tripMeta
	s.mu.RUnlock()

	feedTS := now
	if ts := msg.GetHeader().GetTimestamp(); ts > 0 {
		feedTS = time.Unix(int64(ts), 0).UTC()
	}

	tripUpdates := make(map[string]TripUpdate)
	var rawVehicles []*gtfs.VehiclePosition
	var alerts []Alert

	for _, entity := range msg.Entity {
		if tu := entity.GetTripUpdate(); tu != nil {
			tid := strings.TrimSpace(tu.GetTrip().GetTripId())
			if tid == "" {
				continue
			}
			parsed := parseTripUpdate(tu, feedTS)
			tripUpdates[tid] = parsed
		}
		if v := entity.GetVehicle(); v != nil && v.GetPosition() != nil {
			rawVehicles = append(rawVehicles, v)
		}
		if al := entity.GetAlert(); al != nil {
			alerts = append(alerts, parseAlert(feed, entity.GetId(), al))
		}
	}

	vehicles := make([]Vehicle, 0, len(rawVehicles))
	vehiclesByTrip := make(map[string]Vehicle, len(rawVehicles))
	for _, v := range rawVehicles {
		built := buildVehicle(feed, v, tripUpdates, meta, feedTS)
		vehicles = append(vehicles, built)
		if rawTID := strings.TrimSpace(v.GetTrip().GetTripId()); rawTID != "" {
			vehiclesByTrip[rawTID] = built
		}
	}
	sort.Slice(vehicles, func(i, j int) bool { return vehicles[i].ID < vehicles[j].ID })

	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s:%d:%d:%d:%d", feed, feedTS.Unix(), len(vehicles), len(tripUpdates), len(alerts))
	snapHash := hex.EncodeToString(h.Sum(nil))[:16]

	s.mu.Lock()
	defer s.mu.Unlock()
	s.feeds[feed] = &feedSnapshot{
		vehicles:       vehicles,
		vehiclesByTrip: vehiclesByTrip,
		tripUpdates:    tripUpdates,
		alerts:         alerts,
		etag:           etag,
		lastMod:        lastMod,
		hash:           snapHash,
		checkedAt:      now,
		updatedAt:      now,
		feedTS:         feedTS,
		lastError:      "",
	}
	s.recomputeVersionLocked()
}

func (s *Store) recomputeVersionLocked() {
	ztmHash := ""
	pksHash := ""
	if z, ok := s.feeds["ztm"]; ok {
		ztmHash = z.hash
	}
	if p, ok := s.feeds["pks"]; ok {
		pksHash = p.hash
	}
	s.version = ztmHash + "-" + pksHash
}

// Version returns an ETag-friendly hash representing the current combined RT state.
func (s *Store) Version() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// Vehicles returns active vehicles matching optional operator, route, and bounding box filters.
func (s *Store) Vehicles(operatorFilter, routeFilter string, bbox *[4]float64, now time.Time) ([]Vehicle, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []Vehicle
	anyStale := false
	for feed, snap := range s.feeds {
		if operatorFilter != "" && feed != operatorFilter {
			continue
		}
		if now.Sub(snap.updatedAt) > maxRTFeedAge {
			anyStale = true
			continue
		}
		for _, v := range snap.vehicles {
			if routeFilter != "" && !strings.EqualFold(v.RouteShortName, routeFilter) && v.RouteID != routeFilter {
				continue
			}
			if bbox != nil {
				if v.Lat < bbox[0] || v.Lon < bbox[1] || v.Lat > bbox[2] || v.Lon > bbox[3] {
					continue
				}
			}
			out = append(out, v)
		}
	}
	if out == nil {
		out = []Vehicle{}
	}
	return out, anyStale
}

// Alerts returns active service alerts filtered optionally by operator.
func (s *Store) Alerts(operatorFilter string, now time.Time) []Alert {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []Alert
	for feed, snap := range s.feeds {
		if operatorFilter != "" && feed != operatorFilter {
			continue
		}
		if now.Sub(snap.updatedAt) > maxRTFeedAge {
			continue
		}
		for _, al := range snap.alerts {
			if isAlertActiveAt(al, now) {
				out = append(out, al)
			}
		}
	}
	if out == nil {
		out = []Alert{}
	}
	return out
}

// Status returns per-feed freshness and entity count metrics.
func (s *Store) Status(now time.Time) []FeedStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	feeds := []string{"ztm", "pks"}
	out := make([]FeedStatus, 0, len(feeds))
	for _, f := range feeds {
		snap, ok := s.feeds[f]
		if !ok {
			out = append(out, FeedStatus{Feed: f, Stale: true})
			continue
		}
		st := FeedStatus{
			Feed:            f,
			LastError:       snap.lastError,
			VehicleCount:    len(snap.vehicles),
			TripUpdateCount: len(snap.tripUpdates),
			AlertCount:      len(snap.alerts),
			Stale:           snap.updatedAt.IsZero() || now.Sub(snap.updatedAt) > maxRTFeedAge,
		}
		if !snap.checkedAt.IsZero() {
			t := snap.checkedAt
			st.LastCheckedAt = &t
		}
		if !snap.updatedAt.IsZero() {
			t := snap.updatedAt
			st.LastUpdatedAt = &t
		}
		if !snap.feedTS.IsZero() {
			t := snap.feedTS
			st.FeedTimestamp = &t
		}
		out = append(out, st)
	}
	return out
}

// DepartureEstimate contains live delay/cancellation info for a scheduled stop call.
type DepartureEstimate struct {
	EstimatedTime   time.Time
	DelaySeconds    *int
	IsRealtime      bool
	HasLiveLocation bool
	Canceled        bool
	Skipped         bool
	VehicleID       string
}

// VehicleByTrip returns the active live GPS vehicle for a canonical (feed, rawTripID), if available and fresh.
func (s *Store) VehicleByTrip(feed, rawTripID string, now time.Time) *Vehicle {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.feeds[feed]
	if !ok || now.Sub(snap.updatedAt) > maxRTFeedAge {
		return nil
	}
	v, ok := snap.vehiclesByTrip[rawTripID]
	if !ok {
		return nil
	}
	cp := v
	return &cp
}

// EstimateDeparture applies GTFS-RT TripUpdate delay propagation for a scheduled stop arrival/departure.
func (s *Store) EstimateDeparture(feed, rawTripID string, stopSeq int, rawStopID string, scheduled time.Time, now time.Time) DepartureEstimate {
	res := DepartureEstimate{EstimatedTime: scheduled}

	s.mu.RLock()
	snap, ok := s.feeds[feed]
	if !ok || now.Sub(snap.updatedAt) > maxRTFeedAge {
		s.mu.RUnlock()
		return res
	}
	veh, hasVeh := snap.vehiclesByTrip[rawTripID]
	tu, hasTU := snap.tripUpdates[rawTripID]
	s.mu.RUnlock()

	if hasVeh {
		res.HasLiveLocation = true
		res.VehicleID = veh.ID
	}

	if !hasTU {
		return res
	}

	res.IsRealtime = true
	if res.VehicleID == "" && tu.VehicleID != "" {
		res.VehicleID = feed + ":" + tu.VehicleID
	}
	if tu.Canceled {
		res.Canceled = true
		return res
	}

	// 1. Exact stop_sequence or stop_id match.
	seq32 := int32(stopSeq)
	for _, stu := range tu.StopUpdates {
		if (stu.StopSequence > 0 && stu.StopSequence == seq32) || (stu.StopID != "" && stu.StopID == rawStopID) {
			if stu.Skipped {
				res.Skipped = true
				return res
			}
			return applyStopUpdate(res, stu, scheduled)
		}
	}

	// 2. Propagate delay from the latest preceding StopTimeUpdate (stop_sequence <= stopSeq).
	var latest *StopTimeUpdate
	for i := range tu.StopUpdates {
		stu := &tu.StopUpdates[i]
		if stu.StopSequence > 0 && stu.StopSequence <= seq32 {
			latest = stu
		}
	}
	if latest != nil {
		return applyStopDelayOnly(res, *latest, scheduled)
	}

	// 3. Fallback to trip-level delay if provided.
	if tu.TripDelay != nil {
		d := *tu.TripDelay
		res.DelaySeconds = &d
		res.EstimatedTime = scheduled.Add(time.Duration(d) * time.Second)
	}
	return res
}

func applyStopUpdate(res DepartureEstimate, stu StopTimeUpdate, scheduled time.Time) DepartureEstimate {
	if stu.DepartureTime != nil {
		est := time.Unix(*stu.DepartureTime, 0).In(scheduled.Location())
		d := int(est.Sub(scheduled).Seconds())
		res.EstimatedTime = est
		res.DelaySeconds = &d
		return res
	}
	if stu.ArrivalTime != nil {
		est := time.Unix(*stu.ArrivalTime, 0).In(scheduled.Location())
		d := int(est.Sub(scheduled).Seconds())
		res.EstimatedTime = est
		res.DelaySeconds = &d
		return res
	}
	return applyStopDelayOnly(res, stu, scheduled)
}

func applyStopDelayOnly(res DepartureEstimate, stu StopTimeUpdate, scheduled time.Time) DepartureEstimate {
	if stu.DepartureDelay != nil {
		d := int(*stu.DepartureDelay)
		res.DelaySeconds = &d
		res.EstimatedTime = scheduled.Add(time.Duration(d) * time.Second)
		return res
	}
	if stu.ArrivalDelay != nil {
		d := int(*stu.ArrivalDelay)
		res.DelaySeconds = &d
		res.EstimatedTime = scheduled.Add(time.Duration(d) * time.Second)
	}
	return res
}

func parseTripUpdate(tu *gtfs.TripUpdate, feedTS time.Time) TripUpdate {
	ts := feedTS
	if tu.GetTimestamp() > 0 {
		ts = time.Unix(int64(tu.GetTimestamp()), 0).UTC()
	}

	out := TripUpdate{
		Canceled:  tu.GetTrip().GetScheduleRelationship() == gtfs.TripDescriptor_CANCELED,
		VehicleID: strings.TrimSpace(tu.GetVehicle().GetId()),
		Timestamp: ts,
	}
	if tu.Delay != nil {
		d := int(tu.GetDelay())
		out.TripDelay = &d
	}

	stus := make([]StopTimeUpdate, 0, len(tu.StopTimeUpdate))
	for _, raw := range tu.StopTimeUpdate {
		stu := StopTimeUpdate{
			StopSequence: int32(raw.GetStopSequence()),
			StopID:       strings.TrimSpace(raw.GetStopId()),
			Skipped:      raw.GetScheduleRelationship() == gtfs.TripUpdate_StopTimeUpdate_SKIPPED,
		}
		if arr := raw.GetArrival(); arr != nil {
			if arr.Delay != nil {
				v := arr.GetDelay()
				stu.ArrivalDelay = &v
			}
			if arr.Time != nil {
				v := arr.GetTime()
				stu.ArrivalTime = &v
			}
		}
		if dep := raw.GetDeparture(); dep != nil {
			if dep.Delay != nil {
				v := dep.GetDelay()
				stu.DepartureDelay = &v
			}
			if dep.Time != nil {
				v := dep.GetTime()
				stu.DepartureTime = &v
			}
		}
		stus = append(stus, stu)
	}
	sort.Slice(stus, func(i, j int) bool { return stus[i].StopSequence < stus[j].StopSequence })
	out.StopUpdates = stus
	return out
}

func buildVehicle(feed string, v *gtfs.VehiclePosition, tus map[string]TripUpdate, meta map[string]gtfsstatic.TripMeta, feedTS time.Time) Vehicle {
	rawVID := strings.TrimSpace(v.GetVehicle().GetId())
	if rawVID == "" {
		rawVID = strings.TrimSpace(v.GetVehicle().GetLabel())
	}
	rawTripID := strings.TrimSpace(v.GetTrip().GetTripId())
	rawRouteID := strings.TrimSpace(v.GetTrip().GetRouteId())

	pos := v.GetPosition()
	ts := feedTS
	if v.GetTimestamp() > 0 {
		ts = time.Unix(int64(v.GetTimestamp()), 0).UTC()
	}

	veh := Vehicle{
		ID:           feed + ":" + rawVID,
		Operator:     feed,
		VehicleID:    rawVID,
		Label:        strings.TrimSpace(v.GetVehicle().GetLabel()),
		LicensePlate: strings.TrimSpace(v.GetVehicle().GetLicensePlate()),
		DirectionID:  int(v.GetTrip().GetDirectionId()),
		Lat:          float64(pos.GetLatitude()),
		Lon:          float64(pos.GetLongitude()),
		Timestamp:    ts,
	}

	if rawTripID != "" {
		veh.TripID = feed + ":" + rawTripID
	}
	if rawRouteID != "" {
		veh.RouteID = feed + ":" + rawRouteID
		veh.RouteShortName = rawRouteID
	}

	if tm, ok := meta[feed+":"+rawTripID]; ok {
		veh.RouteID = feed + ":" + tm.RouteID
		veh.RouteShortName = tm.RouteShortName
		veh.RouteColor = tm.RouteColor
		veh.RouteTextColor = tm.RouteTextColor
		veh.Headsign = tm.Headsign
		veh.Brigade = tm.Brigade
		veh.DirectionID = tm.DirectionID
	}

	if pos.Bearing != nil {
		b := pos.GetBearing()
		veh.Bearing = &b
	}
	if pos.Speed != nil {
		sp := pos.GetSpeed()
		veh.Speed = &sp
	}
	if v.CurrentStopSequence != nil {
		seq := v.GetCurrentStopSequence()
		veh.CurrentStopSequence = &seq
	}
	if sid := strings.TrimSpace(v.GetStopId()); sid != "" {
		veh.StopID = feed + ":" + sid
	}
	if v.CurrentStatus != nil {
		veh.CurrentStatus = v.GetCurrentStatus().String()
	}

	if tu, ok := tus[rawTripID]; ok {
		if d := latestDelayFromTripUpdate(tu, v.GetCurrentStopSequence()); d != nil {
			veh.DelaySeconds = d
		}
	}

	return veh
}

func latestDelayFromTripUpdate(tu TripUpdate, currentSeq uint32) *int {
	seq32 := int32(currentSeq)
	for i := len(tu.StopUpdates) - 1; i >= 0; i-- {
		stu := tu.StopUpdates[i]
		if currentSeq == 0 || stu.StopSequence <= seq32 {
			if stu.DepartureDelay != nil {
				d := int(*stu.DepartureDelay)
				return &d
			}
			if stu.ArrivalDelay != nil {
				d := int(*stu.ArrivalDelay)
				return &d
			}
		}
	}
	if len(tu.StopUpdates) > 0 {
		stu := tu.StopUpdates[0]
		if stu.DepartureDelay != nil {
			d := int(*stu.DepartureDelay)
			return &d
		}
		if stu.ArrivalDelay != nil {
			d := int(*stu.ArrivalDelay)
			return &d
		}
	}
	return tu.TripDelay
}

func parseAlert(feed, rawID string, al *gtfs.Alert) Alert {
	out := Alert{
		ID:          feed + ":" + rawID,
		Operator:    feed,
		Header:      pickTranslation(al.GetHeaderText()),
		Description: pickTranslation(al.GetDescriptionText()),
		URL:         pickTranslation(al.GetUrl()),
	}
	if al.Cause != nil {
		out.Cause = al.GetCause().String()
	}
	if al.Effect != nil {
		out.Effect = al.GetEffect().String()
	}
	if len(al.ActivePeriod) > 0 {
		p := al.ActivePeriod[0]
		if p.Start != nil && p.GetStart() > 0 {
			t := time.Unix(int64(p.GetStart()), 0).UTC()
			out.ActiveFrom = &t
		}
		if p.End != nil && p.GetEnd() > 0 {
			t := time.Unix(int64(p.GetEnd()), 0).UTC()
			out.ActiveTo = &t
		}
	}
	for _, ie := range al.InformedEntity {
		e := InformedEntity{}
		if rid := strings.TrimSpace(ie.GetRouteId()); rid != "" {
			e.RouteID = feed + ":" + rid
		}
		if sid := strings.TrimSpace(ie.GetStopId()); sid != "" {
			e.StopID = feed + ":" + sid
		}
		if tid := strings.TrimSpace(ie.GetTrip().GetTripId()); tid != "" {
			e.TripID = feed + ":" + tid
		}
		if e.RouteID != "" || e.StopID != "" || e.TripID != "" {
			out.Entities = append(out.Entities, e)
		}
	}
	return out
}

func pickTranslation(ts *gtfs.TranslatedString) string {
	if ts == nil || len(ts.Translation) == 0 {
		return ""
	}
	for _, tr := range ts.Translation {
		if strings.EqualFold(tr.GetLanguage(), "pl") && tr.GetText() != "" {
			return tr.GetText()
		}
	}
	return ts.Translation[0].GetText()
}

func isAlertActiveAt(al Alert, now time.Time) bool {
	if al.ActiveFrom != nil && now.Before(*al.ActiveFrom) {
		return false
	}
	if al.ActiveTo != nil && now.After(*al.ActiveTo) {
		return false
	}
	return true
}
