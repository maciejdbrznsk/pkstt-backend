package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/maciejdbrznsk/pkstt-backend/internal/config"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsrt"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsstatic"
)

// Handler serves read-only transit API endpoints over PostgreSQL static GTFS and in-memory GTFS-RT.
type Handler struct {
	pool    *pgxpool.Pool
	rtStore *gtfsrt.Store
	loc     *time.Location
	log     *slog.Logger
	cfg     config.Config
}

type sampleDirectionDTO struct {
	Operator       string `json:"operator"`
	RouteID        string `json:"route_id"`
	RouteShortName string `json:"route_short_name"`
	RouteColor     string `json:"route_color,omitempty"`
	Headsign       string `json:"headsign"`
	TripCount      int    `json:"trip_count"`
}

type platformDTO struct {
	ID               string               `json:"id"`
	Operator         string               `json:"operator"`
	Name             string               `json:"name"`
	Code             string               `json:"code,omitempty"`
	PlatformCode     string               `json:"platform_code,omitempty"`
	IsOnDemand       bool                 `json:"is_on_demand"`
	Lat              float64              `json:"lat"`
	Lon              float64              `json:"lon"`
	ZoneID           string               `json:"zone_id,omitempty"`
	StreetName       string               `json:"street_name,omitempty"`
	TownName         string               `json:"town_name,omitempty"`
	SampleDirections []sampleDirectionDTO `json:"sample_directions,omitempty"`
}

type bayDTO struct {
	BayID            string               `json:"bay_id"`
	Label            string               `json:"label"`
	PlatformCode     string               `json:"platform_code,omitempty"`
	Operators        []string             `json:"operators"`
	StopIDs          []string             `json:"stop_ids"`
	IsOnDemand       bool                 `json:"is_on_demand"`
	Lat              float64              `json:"lat"`
	Lon              float64              `json:"lon"`
	SampleDirections []sampleDirectionDTO `json:"sample_directions"`
}

type stopGroupDTO struct {
	GroupID        string        `json:"group_id"`
	Name           string        `json:"name"`
	Operators      []string      `json:"operators"`
	Lat            float64       `json:"lat"`
	Lon            float64       `json:"lon"`
	StopCount      int           `json:"stop_count"`
	DistanceMeters *float64      `json:"distance_m,omitempty"`
	Platforms      []platformDTO `json:"platforms"`
}

type routeSummaryDTO struct {
	ID        string `json:"id"`
	Operator  string `json:"operator"`
	ShortName string `json:"short_name"`
	LongName  string `json:"long_name,omitempty"`
	RouteType int    `json:"route_type"`
	Color     string `json:"color,omitempty"`
	TextColor string `json:"text_color,omitempty"`
}

type departureDTO struct {
	Operator             string    `json:"operator"`
	StopID               string    `json:"stop_id"`
	StopName             string    `json:"stop_name"`
	PlatformCode         string    `json:"platform_code,omitempty"`
	RouteID              string    `json:"route_id"`
	RouteShortName       string    `json:"route_short_name"`
	RouteColor           string    `json:"route_color,omitempty"`
	RouteTextColor       string    `json:"route_text_color,omitempty"`
	TripID               string    `json:"trip_id"`
	ServiceDate          string    `json:"service_date"`
	Headsign             string    `json:"headsign"`
	Brigade              string    `json:"brigade,omitempty"`
	WheelchairAccessible bool      `json:"wheelchair_accessible"`
	ScheduledTime        time.Time `json:"scheduled_time"`
	EstimatedTime        time.Time `json:"estimated_time"`
	DelaySeconds         *int      `json:"delay_seconds"`
	IsRealtime           bool      `json:"is_realtime"`
	HasLiveLocation      bool      `json:"has_live_location"`
	Canceled             bool      `json:"canceled"`
	MinutesUntil         int       `json:"minutes_until"`
	VehicleID            string    `json:"vehicle_id,omitempty"`
}

// Healthz verifies PostgreSQL connectivity and reports live feed status.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.pool.Ping(ctx); err != nil {
		h.log.Error("healthz db ping failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}

	setCacheControl(w, 0)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"time":   time.Now().In(h.loc),
		"rt":     h.rtStore.Status(time.Now().UTC()),
	})
}

// Meta returns static feed synchronization state and GTFS-RT freshness metrics.
func (h *Handler) Meta(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := h.pool.Query(ctx, `
		SELECT feed, source_url, COALESCE(feed_version, ''), feed_start_date, feed_end_date,
		       last_checked_at, last_updated_at, COALESCE(last_error, ''), stats
		FROM feed_state
		ORDER BY feed
	`)
	if err != nil {
		h.internalErr(w, "query feed_state", err)
		return
	}
	defer rows.Close()

	type staticFeedMeta struct {
		Feed          string          `json:"feed"`
		SourceURL     string          `json:"source_url"`
		FeedVersion   string          `json:"feed_version,omitempty"`
		StartDate     *string         `json:"feed_start_date,omitempty"`
		EndDate       *string         `json:"feed_end_date,omitempty"`
		LastCheckedAt *time.Time      `json:"last_checked_at,omitempty"`
		LastUpdatedAt *time.Time      `json:"last_updated_at,omitempty"`
		LastError     string          `json:"last_error,omitempty"`
		Stats         json.RawMessage `json:"stats"`
	}

	var staticFeeds []staticFeedMeta
	for rows.Next() {
		var m staticFeedMeta
		var startD, endD *time.Time
		if err := rows.Scan(
			&m.Feed, &m.SourceURL, &m.FeedVersion, &startD, &endD,
			&m.LastCheckedAt, &m.LastUpdatedAt, &m.LastError, &m.Stats,
		); err != nil {
			h.internalErr(w, "scan feed_state", err)
			return
		}
		if startD != nil {
			s := startD.Format("2006-01-02")
			m.StartDate = &s
		}
		if endD != nil {
			s := endD.Format("2006-01-02")
			m.EndDate = &s
		}
		staticFeeds = append(staticFeeds, m)
	}
	if err := rows.Err(); err != nil {
		h.internalErr(w, "rows feed_state", err)
		return
	}

	setCacheControl(w, 5)
	writeJSON(w, http.StatusOK, map[string]any{
		"timezone":       h.cfg.Timezone,
		"static_feeds":   staticFeeds,
		"realtime_feeds": h.rtStore.Status(time.Now().UTC()),
	})
}

// ListStopGroups searches stop groups by accent-insensitive query and/or GPS coordinates.
func (h *Handler) ListStopGroups(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	op, ok := parseOperatorFilter(r.URL.Query().Get("operator"))
	if !ok {
		writeError(w, http.StatusBadRequest, "operator must be 'ztm' or 'pks'")
		return
	}
	limit := parseIntClamped(r.URL.Query().Get("limit"), 20, 1, 2000)
	offset := parseIntClamped(r.URL.Query().Get("offset"), 0, 0, 10000)

	latStr := strings.TrimSpace(r.URL.Query().Get("lat"))
	lonStr := strings.TrimSpace(r.URL.Query().Get("lon"))
	hasCoords := latStr != "" || lonStr != ""

	var lat, lon float64
	radius := 800.0
	if hasCoords {
		if latStr == "" || lonStr == "" {
			writeError(w, http.StatusBadRequest, "both lat and lon are required when filtering by coordinates")
			return
		}
		var err error
		lat, err = strconv.ParseFloat(latStr, 64)
		if err != nil || lat < -90 || lat > 90 {
			writeError(w, http.StatusBadRequest, "invalid lat")
			return
		}
		lon, err = strconv.ParseFloat(lonStr, 64)
		if err != nil || lon < -180 || lon > 180 {
			writeError(w, http.StatusBadRequest, "invalid lon")
			return
		}
		if radStr := strings.TrimSpace(r.URL.Query().Get("radius")); radStr != "" {
			rad, err := strconv.ParseFloat(radStr, 64)
			if err != nil || rad <= 0 {
				writeError(w, http.StatusBadRequest, "invalid radius")
				return
			}
			radius = math.Min(rad, 10000)
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var (
		groups   []stopGroupDTO
		groupIDs []string
		err      error
	)

	if hasCoords {
		groups, groupIDs, err = h.queryNearbyStopGroups(ctx, lat, lon, radius, q, op, limit)
	} else {
		groups, groupIDs, err = h.querySearchStopGroups(ctx, q, op, limit, offset)
	}
	if err != nil {
		h.internalErr(w, "query stop groups", err)
		return
	}

	if err := h.attachPlatforms(ctx, groups, groupIDs); err != nil {
		h.internalErr(w, "attach stop platforms", err)
		return
	}

	setCacheControl(w, 60)
	writeJSON(w, http.StatusOK, map[string]any{
		"count":       len(groups),
		"stop_groups": groups,
	})
}

func (h *Handler) queryNearbyStopGroups(ctx context.Context, lat, lon, radius float64, q, op string, limit int) ([]stopGroupDTO, []string, error) {
	dLat := radius / 111320.0
	cosLat := math.Cos(lat * math.Pi / 180.0)
	if cosLat < 0.1 {
		cosLat = 0.1
	}
	dLon := radius / (111320.0 * cosLat)
	foldedQ := gtfsstatic.FoldPolish(q)

	rows, err := h.pool.Query(ctx, `
		WITH candidates AS (
			SELECT group_id, name, operators, lat, lon, stop_count,
			       6371000.0 * 2.0 * ASIN(SQRT(
			           POWER(SIN(RADIANS(lat - $1) / 2.0), 2) +
			           COS(RADIANS($1)) * COS(RADIANS(lat)) * POWER(SIN(RADIANS(lon - $2) / 2.0), 2)
			       )) AS dist_m
			FROM stop_groups
			WHERE lat BETWEEN $1 - $3 AND $1 + $3
			  AND lon BETWEEN $2 - $4 AND $2 + $4
			  AND ($5 = '' OR search_text LIKE '%' || $5 || '%')
			  AND ($6 = '' OR $6 = ANY(operators))
		)
		SELECT group_id, name, operators, lat, lon, stop_count, dist_m
		FROM candidates
		WHERE dist_m <= $7
		ORDER BY dist_m ASC, name ASC
		LIMIT $8
	`, lat, lon, dLat, dLon, foldedQ, op, radius, limit)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	groups := make([]stopGroupDTO, 0)
	ids := make([]string, 0)
	for rows.Next() {
		var g stopGroupDTO
		var dist float64
		if err := rows.Scan(&g.GroupID, &g.Name, &g.Operators, &g.Lat, &g.Lon, &g.StopCount, &dist); err != nil {
			return nil, nil, err
		}
		distRounded := math.Round(dist*10) / 10
		g.DistanceMeters = &distRounded
		g.Platforms = []platformDTO{}
		groups = append(groups, g)
		ids = append(ids, g.GroupID)
	}
	return groups, ids, rows.Err()
}

func (h *Handler) querySearchStopGroups(ctx context.Context, q, op string, limit, offset int) ([]stopGroupDTO, []string, error) {
	foldedQ := gtfsstatic.FoldPolish(q)

	var (
		rows pgx.Rows
		err  error
	)
	if foldedQ == "" {
		rows, err = h.pool.Query(ctx, `
			SELECT group_id, name, operators, lat, lon, stop_count
			FROM stop_groups
			WHERE ($1 = '' OR $1 = ANY(operators))
			ORDER BY name ASC
			LIMIT $2 OFFSET $3
		`, op, limit, offset)
	} else {
		rows, err = h.pool.Query(ctx, `
			SELECT group_id, name, operators, lat, lon, stop_count
			FROM stop_groups
			WHERE ($2 = '' OR $2 = ANY(operators))
			  AND (search_text LIKE '%' || $1 || '%' OR similarity(search_text, $1) > 0.25)
			ORDER BY
				CASE WHEN LOWER(name) LIKE $1 || '%' THEN 0
				     WHEN search_text LIKE '%' || $1 || '%' THEN 1
				     ELSE 2 END,
				similarity(search_text, $1) DESC,
				array_length(operators, 1) DESC,
				name ASC
			LIMIT $3 OFFSET $4
		`, foldedQ, op, limit, offset)
	}
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	groups := make([]stopGroupDTO, 0)
	ids := make([]string, 0)
	for rows.Next() {
		var g stopGroupDTO
		if err := rows.Scan(&g.GroupID, &g.Name, &g.Operators, &g.Lat, &g.Lon, &g.StopCount); err != nil {
			return nil, nil, err
		}
		g.Platforms = []platformDTO{}
		groups = append(groups, g)
		ids = append(ids, g.GroupID)
	}
	return groups, ids, rows.Err()
}

func (h *Handler) attachPlatforms(ctx context.Context, groups []stopGroupDTO, groupIDs []string) error {
	if len(groupIDs) == 0 {
		return nil
	}
	rows, err := h.pool.Query(ctx, `
		SELECT group_id, feed, stop_id, COALESCE(stop_code, ''), stop_name,
		       platform_code, is_on_demand, stop_lat, stop_lon,
		       COALESCE(zone_id, ''), COALESCE(street_name, ''), COALESCE(town_name, '')
		FROM stops
		WHERE group_id = ANY($1)
		ORDER BY feed ASC, platform_code ASC, stop_name ASC
	`, groupIDs)
	if err != nil {
		return err
	}
	defer rows.Close()

	byGroup := make(map[string][]platformDTO, len(groupIDs))
	for rows.Next() {
		var gid, feed, rawID string
		var p platformDTO
		if err := rows.Scan(
			&gid, &feed, &rawID, &p.Code, &p.Name,
			&p.PlatformCode, &p.IsOnDemand, &p.Lat, &p.Lon,
			&p.ZoneID, &p.StreetName, &p.TownName,
		); err != nil {
			return err
		}
		p.ID = feed + ":" + rawID
		p.Operator = feed
		byGroup[gid] = append(byGroup[gid], p)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := range groups {
		if ps, ok := byGroup[groups[i].GroupID]; ok {
			groups[i].Platforms = ps
		}
	}
	return nil
}

// GetStopGroup returns full stop group metadata, platforms, and serving ZTM+PKS routes.
func (h *Handler) GetStopGroup(w http.ResponseWriter, r *http.Request) {
	groupID := strings.TrimSpace(chi.URLParam(r, "groupID"))
	if groupID == "" {
		writeError(w, http.StatusBadRequest, "missing groupID")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var g stopGroupDTO
	err := h.pool.QueryRow(ctx, `
		SELECT group_id, name, operators, lat, lon, stop_count
		FROM stop_groups
		WHERE group_id = $1
	`, groupID).Scan(&g.GroupID, &g.Name, &g.Operators, &g.Lat, &g.Lon, &g.StopCount)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "stop group not found")
		return
	}
	if err != nil {
		h.internalErr(w, "get stop_group", err)
		return
	}
	g.Platforms = []platformDTO{}

	groups := []stopGroupDTO{g}
	if err := h.attachPlatforms(ctx, groups, []string{groupID}); err != nil {
		h.internalErr(w, "attach stop_group platforms", err)
		return
	}

	if err := h.attachSampleDirections(ctx, groups[0].Platforms, groupID); err != nil {
		h.internalErr(w, "query stop_group sample directions", err)
		return
	}
	bays := buildBays(groups[0].Name, groups[0].Platforms)

	routeRows, err := h.pool.Query(ctx, `
		SELECT DISTINCT r.feed, r.route_id, r.route_short_name, COALESCE(r.route_long_name, ''),
		       r.route_type, COALESCE(r.route_color, ''), COALESCE(r.route_text_color, '')
		FROM stops s
		JOIN stop_times st ON st.feed = s.feed AND st.stop_id = s.stop_id
		JOIN trips t ON t.feed = st.feed AND t.trip_id = st.trip_id
		JOIN routes r ON r.feed = t.feed AND r.route_id = t.route_id
		WHERE s.group_id = $1
	`, groupID)
	if err != nil {
		h.internalErr(w, "query stop_group routes", err)
		return
	}
	defer routeRows.Close()

	routes := make([]routeSummaryDTO, 0)
	for routeRows.Next() {
		var rt routeSummaryDTO
		var rawID string
		if err := routeRows.Scan(&rt.Operator, &rawID, &rt.ShortName, &rt.LongName, &rt.RouteType, &rt.Color, &rt.TextColor); err != nil {
			h.internalErr(w, "scan stop_group routes", err)
			return
		}
		rt.ID = rt.Operator + ":" + rawID
		routes = append(routes, rt)
	}
	if err := routeRows.Err(); err != nil {
		h.internalErr(w, "rows stop_group routes", err)
		return
	}
	sortRoutesNaturally(routes)

	setCacheControl(w, 120)
	writeJSON(w, http.StatusOK, map[string]any{
		"stop_group": groups[0],
		"bays":       bays,
		"routes":     routes,
	})
}

func (h *Handler) attachSampleDirections(ctx context.Context, platforms []platformDTO, groupID string) error {
	if len(platforms) == 0 {
		return nil
	}
	rows, err := h.pool.Query(ctx, `
		SELECT st.feed, st.stop_id, r.route_id, r.route_short_name, COALESCE(r.route_color, ''),
		       COALESCE(NULLIF(t.trip_headsign, ''), r.route_long_name, '') AS headsign,
		       COUNT(*) AS trip_cnt
		FROM stops s
		JOIN stop_times st ON st.feed = s.feed AND st.stop_id = s.stop_id
		JOIN trips t ON t.feed = st.feed AND t.trip_id = st.trip_id
		JOIN routes r ON r.feed = t.feed AND r.route_id = t.route_id
		WHERE s.group_id = $1
		  AND st.pickup_type <> 1
		  AND EXISTS (
		      SELECT 1 FROM stop_times nx
		      WHERE nx.feed = st.feed AND nx.trip_id = st.trip_id AND nx.stop_sequence > st.stop_sequence
		  )
		GROUP BY st.feed, st.stop_id, r.route_id, r.route_short_name, r.route_color, headsign
		ORDER BY trip_cnt DESC, r.route_short_name ASC
	`, groupID)
	if err != nil {
		return err
	}
	defer rows.Close()

	byStop := make(map[string][]sampleDirectionDTO, len(platforms))
	for rows.Next() {
		var feed, rawSID, rawRID string
		var sd sampleDirectionDTO
		if err := rows.Scan(&feed, &rawSID, &rawRID, &sd.RouteShortName, &sd.RouteColor, &sd.Headsign, &sd.TripCount); err != nil {
			return err
		}
		sd.Operator = feed
		sd.RouteID = feed + ":" + rawRID
		key := feed + ":" + rawSID
		byStop[key] = append(byStop[key], sd)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := range platforms {
		if list, ok := byStop[platforms[i].ID]; ok {
			platforms[i].SampleDirections = list
		}
	}
	return nil
}

func buildBays(groupName string, platforms []platformDTO) []bayDTO {
	type bayAcc struct {
		bay         bayDTO
		hasZTM      bool
		hasPKS      bool
		dirDedupMap map[string]sampleDirectionDTO
	}
	var accs []*bayAcc

	for _, p := range platforms {
		var target *bayAcc
		for _, a := range accs {
			sameCode := p.PlatformCode != "" && a.bay.PlatformCode == p.PlatformCode
			closeEnough := gtfsstatic.HaversineMeters(a.bay.Lat, a.bay.Lon, p.Lat, p.Lon) <= 45.0
			if (sameCode && closeEnough) || (p.PlatformCode == "" && a.bay.PlatformCode == "" && gtfsstatic.HaversineMeters(a.bay.Lat, a.bay.Lon, p.Lat, p.Lon) <= 20.0) {
				target = a
				break
			}
		}
		if target == nil {
			bayID := p.ID
			label := p.Name
			if p.PlatformCode != "" {
				bayID = p.PlatformCode
				label = groupName + " " + p.PlatformCode
			}
			target = &bayAcc{
				bay: bayDTO{
					BayID:        bayID,
					Label:        label,
					PlatformCode: p.PlatformCode,
					Lat:          p.Lat,
					Lon:          p.Lon,
				},
				dirDedupMap: make(map[string]sampleDirectionDTO),
			}
			accs = append(accs, target)
		} else {
			n := float64(len(target.bay.StopIDs))
			target.bay.Lat = (target.bay.Lat*n + p.Lat) / (n + 1)
			target.bay.Lon = (target.bay.Lon*n + p.Lon) / (n + 1)
		}

		target.bay.StopIDs = append(target.bay.StopIDs, p.ID)
		if p.IsOnDemand {
			target.bay.IsOnDemand = true
		}
		if p.Operator == "ztm" {
			target.hasZTM = true
		} else if p.Operator == "pks" {
			target.hasPKS = true
		}
		for _, d := range p.SampleDirections {
			k := d.Operator + "|" + d.RouteShortName + "|" + d.Headsign
			if existing, ok := target.dirDedupMap[k]; ok {
				existing.TripCount += d.TripCount
				target.dirDedupMap[k] = existing
			} else {
				target.dirDedupMap[k] = d
			}
		}
	}

	bays := make([]bayDTO, 0, len(accs))
	for _, a := range accs {
		if a.hasZTM {
			a.bay.Operators = append(a.bay.Operators, "ztm")
		}
		if a.hasPKS {
			a.bay.Operators = append(a.bay.Operators, "pks")
		}
		dirs := make([]sampleDirectionDTO, 0, len(a.dirDedupMap))
		for _, d := range a.dirDedupMap {
			dirs = append(dirs, d)
		}
		sort.Slice(dirs, func(i, j int) bool {
			if dirs[i].TripCount != dirs[j].TripCount {
				return dirs[i].TripCount > dirs[j].TripCount
			}
			return dirs[i].RouteShortName < dirs[j].RouteShortName
		})
		if len(dirs) > 6 {
			dirs = dirs[:6]
		}
		a.bay.SampleDirections = dirs
		bays = append(bays, a.bay)
	}

	sort.Slice(bays, func(i, j int) bool {
		if bays[i].PlatformCode != bays[j].PlatformCode {
			return bays[i].PlatformCode < bays[j].PlatformCode
		}
		return bays[i].Label < bays[j].Label
	})
	return bays
}

// GetStopGroupDepartures returns merged live+scheduled ZTM and PKS departures for a stop group.
func (h *Handler) GetStopGroupDepartures(w http.ResponseWriter, r *http.Request) {
	groupID := strings.TrimSpace(chi.URLParam(r, "groupID"))
	if groupID == "" {
		writeError(w, http.StatusBadRequest, "missing groupID")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var groupName string
	err := h.pool.QueryRow(ctx, `SELECT name FROM stop_groups WHERE group_id = $1`, groupID).Scan(&groupName)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "stop group not found")
		return
	}
	if err != nil {
		h.internalErr(w, "lookup stop group", err)
		return
	}

	rows, err := h.pool.Query(ctx, `SELECT feed, stop_id FROM stops WHERE group_id = $1`, groupID)
	if err != nil {
		h.internalErr(w, "lookup group stops", err)
		return
	}
	defer rows.Close()

	var allowedStopSet map[string]struct{}
	if rawStopIDs := strings.TrimSpace(r.URL.Query().Get("stop_ids")); rawStopIDs != "" {
		allowedStopSet = make(map[string]struct{})
		for _, tok := range strings.Split(rawStopIDs, ",") {
			if _, _, ok := parseCanonicalID(tok); !ok {
				writeError(w, http.StatusBadRequest, "stop_ids must contain comma-separated '<operator>:<id>' values")
				return
			}
			allowedStopSet[strings.TrimSpace(tok)] = struct{}{}
		}
	}

	var feeds, stopIDs []string
	for rows.Next() {
		var f, sid string
		if err := rows.Scan(&f, &sid); err != nil {
			h.internalErr(w, "scan group stops", err)
			return
		}
		if allowedStopSet != nil {
			if _, ok := allowedStopSet[f+":"+sid]; !ok {
				continue
			}
		}
		feeds = append(feeds, f)
		stopIDs = append(stopIDs, sid)
	}
	if err := rows.Err(); err != nil {
		h.internalErr(w, "rows group stops", err)
		return
	}

	h.serveDepartures(w, r, ctx, map[string]any{
		"group_id": groupID,
		"name":     groupName,
	}, feeds, stopIDs)
}

// GetStopDepartures returns live+scheduled departures for a single canonical stop ID (e.g., "ztm:65").
func (h *Handler) GetStopDepartures(w http.ResponseWriter, r *http.Request) {
	rawParam := chi.URLParam(r, "stopID")
	feed, rawID, ok := parseCanonicalID(rawParam)
	if !ok {
		writeError(w, http.StatusBadRequest, "stopID must be formatted as '<operator>:<id>' (e.g. ztm:65)")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var stopName, groupID string
	err := h.pool.QueryRow(ctx,
		`SELECT stop_name, COALESCE(group_id, '') FROM stops WHERE feed = $1 AND stop_id = $2`,
		feed, rawID,
	).Scan(&stopName, &groupID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "stop not found")
		return
	}
	if err != nil {
		h.internalErr(w, "lookup stop", err)
		return
	}

	h.serveDepartures(w, r, ctx, map[string]any{
		"stop_id":  rawParam,
		"group_id": groupID,
		"name":     stopName,
	}, []string{feed}, []string{rawID})
}

func (h *Handler) serveDepartures(w http.ResponseWriter, r *http.Request, ctx context.Context, subject map[string]any, feeds, stopIDs []string) {
	opFilter, ok := parseOperatorFilter(r.URL.Query().Get("operator"))
	if !ok {
		writeError(w, http.StatusBadRequest, "operator must be 'ztm' or 'pks'")
		return
	}
	routeFilter := strings.TrimSpace(r.URL.Query().Get("route"))
	windowMins := parseIntClamped(r.URL.Query().Get("window"), 120, 5, 1440)
	limit := parseIntClamped(r.URL.Query().Get("limit"), 40, 1, 150)

	at := time.Now().In(h.loc)
	if atStr := strings.TrimSpace(r.URL.Query().Get("at")); atStr != "" {
		parsedAt, err := parseAtParam(atStr, h.loc)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid 'at' timestamp (use RFC3339 or unix seconds)")
			return
		}
		at = parsedAt.In(h.loc)
	}

	departures, err := h.queryDepartures(ctx, feeds, stopIDs, at, windowMins, opFilter, routeFilter, limit)
	if err != nil {
		h.internalErr(w, "query departures", err)
		return
	}

	setCacheControl(w, 5)
	subject["at"] = at
	subject["window_minutes"] = windowMins
	subject["count"] = len(departures)
	subject["departures"] = departures
	writeJSON(w, http.StatusOK, subject)
}

func (h *Handler) queryDepartures(
	ctx context.Context,
	feeds, stopIDs []string,
	at time.Time,
	windowMins int,
	opFilter, routeFilter string,
	limit int,
) ([]departureDTO, error) {
	if len(feeds) == 0 {
		return []departureDTO{}, nil
	}

	windowEnd := at.Add(time.Duration(windowMins) * time.Minute)
	baseDate := time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, h.loc)

	type dateWindow struct {
		serviceDate time.Time
		origin      time.Time
		minSec      int
		maxSec      int
	}

	var windows []dateWindow
	for offset := -1; offset <= 1; offset++ {
		sd := baseDate.AddDate(0, 0, offset)
		// Per GTFS spec, service day seconds are relative to (noon - 12h) to handle DST transitions accurately.
		noon := time.Date(sd.Year(), sd.Month(), sd.Day(), 12, 0, 0, 0, h.loc)
		origin := noon.Add(-12 * time.Hour)

		minSec := int(at.Sub(origin).Seconds()) - 1800 // include 30m earlier departures to catch delayed buses
		maxSec := int(windowEnd.Sub(origin).Seconds()) + 600
		if maxSec < 0 {
			continue
		}
		if minSec < 0 {
			minSec = 0
		}
		windows = append(windows, dateWindow{
			serviceDate: time.Date(sd.Year(), sd.Month(), sd.Day(), 0, 0, 0, 0, time.UTC),
			origin:      origin,
			minSec:      minSec,
			maxSec:      maxSec,
		})
	}

	if len(windows) == 0 {
		return []departureDTO{}, nil
	}

	winDates := make([]time.Time, len(windows))
	winMin := make([]int, len(windows))
	winMax := make([]int, len(windows))
	originByDate := make(map[string]time.Time, len(windows))
	for i, w := range windows {
		winDates[i] = w.serviceDate
		winMin[i] = w.minSec
		winMax[i] = w.maxSec
		originByDate[w.serviceDate.Format("2006-01-02")] = w.origin
	}

	rows, err := h.pool.Query(ctx, `
		WITH target_stops AS (
			SELECT UNNEST($1::text[]) AS feed, UNNEST($2::text[]) AS stop_id
		),
		service_windows AS (
			SELECT UNNEST($3::date[]) AS service_date,
			       UNNEST($4::int[])  AS min_sec,
			       UNNEST($5::int[])  AS max_sec
		)
		SELECT cd.service_date, st.feed, st.stop_id, s.stop_name, s.platform_code,
		       t.route_id, r.route_short_name, COALESCE(r.route_color, ''), COALESCE(r.route_text_color, ''),
		       st.trip_id, st.stop_sequence, st.departure_secs,
		       COALESCE(t.trip_headsign, ''), COALESCE(t.brigade, ''), t.wheelchair_accessible
		FROM target_stops ts
		JOIN stops s ON s.feed = ts.feed AND s.stop_id = ts.stop_id
		JOIN stop_times st ON st.feed = ts.feed AND st.stop_id = ts.stop_id
		JOIN service_windows sw ON st.departure_secs BETWEEN sw.min_sec AND sw.max_sec
		JOIN trips t ON t.feed = st.feed AND t.trip_id = st.trip_id
		JOIN routes r ON r.feed = t.feed AND r.route_id = t.route_id
		JOIN calendar_dates cd ON cd.feed = t.feed AND cd.service_id = t.service_id
		                      AND cd.service_date = sw.service_date AND cd.exception_type = 1
		WHERE ($6 = '' OR st.feed = $6)
		  AND ($7 = '' OR LOWER(r.route_short_name) = LOWER($7) OR (st.feed || ':' || t.route_id) = $7)
		  AND st.pickup_type <> 1
		  AND EXISTS (
		      SELECT 1 FROM stop_times nx
		      WHERE nx.feed = st.feed AND nx.trip_id = st.trip_id AND nx.stop_sequence > st.stop_sequence
		  )
	`, feeds, stopIDs, winDates, winMin, winMax, opFilter, routeFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nowUTC := time.Now().UTC()
	cutoffPast := at.Add(-1 * time.Minute)
	out := make([]departureDTO, 0, limit)

	for rows.Next() {
		var (
			svcDate                                                time.Time
			feed, rawStopID, stopName, platformCode                string
			rawRouteID, routeShort, routeColor, routeTextColor     string
			rawTripID, headsign, brigade                           string
			stopSeq, depSecs, wheelchair                           int
		)
		if err := rows.Scan(
			&svcDate, &feed, &rawStopID, &stopName, &platformCode,
			&rawRouteID, &routeShort, &routeColor, &routeTextColor,
			&rawTripID, &stopSeq, &depSecs,
			&headsign, &brigade, &wheelchair,
		); err != nil {
			return nil, err
		}

		svcDateStr := svcDate.Format("2006-01-02")
		origin, ok := originByDate[svcDateStr]
		if !ok {
			continue
		}
		scheduled := origin.Add(time.Duration(depSecs) * time.Second)
		est := h.rtStore.EstimateDeparture(feed, rawTripID, stopSeq, rawStopID, scheduled, nowUTC)
		if est.Skipped {
			continue
		}
		if est.EstimatedTime.Before(cutoffPast) || est.EstimatedTime.After(windowEnd) {
			continue
		}

		minsUntil := int(math.Round(est.EstimatedTime.Sub(at).Minutes()))
		out = append(out, departureDTO{
			Operator:             feed,
			StopID:               feed + ":" + rawStopID,
			StopName:             stopName,
			PlatformCode:         platformCode,
			RouteID:              feed + ":" + rawRouteID,
			RouteShortName:       routeShort,
			RouteColor:           routeColor,
			RouteTextColor:       routeTextColor,
			TripID:               feed + ":" + rawTripID,
			ServiceDate:          svcDateStr,
			Headsign:             headsign,
			Brigade:              brigade,
			WheelchairAccessible: wheelchair == 1,
			ScheduledTime:        scheduled,
			EstimatedTime:        est.EstimatedTime,
			DelaySeconds:         est.DelaySeconds,
			IsRealtime:           est.IsRealtime,
			HasLiveLocation:      est.HasLiveLocation,
			Canceled:             est.Canceled,
			MinutesUntil:         minsUntil,
			VehicleID:            est.VehicleID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.Slice(out, func(i, j int) bool {
		if !out[i].EstimatedTime.Equal(out[j].EstimatedTime) {
			return out[i].EstimatedTime.Before(out[j].EstimatedTime)
		}
		return out[i].RouteShortName < out[j].RouteShortName
	})

	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ListRoutes returns all ZTM and PKS routes with optional operator and search filters.
func (h *Handler) ListRoutes(w http.ResponseWriter, r *http.Request) {
	op, ok := parseOperatorFilter(r.URL.Query().Get("operator"))
	if !ok {
		writeError(w, http.StatusBadRequest, "operator must be 'ztm' or 'pks'")
		return
	}
	q := gtfsstatic.FoldPolish(strings.TrimSpace(r.URL.Query().Get("q")))

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := h.pool.Query(ctx, `
		SELECT feed, route_id, route_short_name, COALESCE(route_long_name, ''),
		       route_type, COALESCE(route_color, ''), COALESCE(route_text_color, '')
		FROM routes
		WHERE ($1 = '' OR feed = $1)
		  AND ($2 = '' OR LOWER(route_short_name) LIKE '%' || $2 || '%' OR LOWER(COALESCE(route_long_name, '')) LIKE '%' || $2 || '%')
	`, op, q)
	if err != nil {
		h.internalErr(w, "query routes", err)
		return
	}
	defer rows.Close()

	routes := make([]routeSummaryDTO, 0)
	for rows.Next() {
		var rt routeSummaryDTO
		var rawID string
		if err := rows.Scan(&rt.Operator, &rawID, &rt.ShortName, &rt.LongName, &rt.RouteType, &rt.Color, &rt.TextColor); err != nil {
			h.internalErr(w, "scan routes", err)
			return
		}
		rt.ID = rt.Operator + ":" + rawID
		routes = append(routes, rt)
	}
	if err := rows.Err(); err != nil {
		h.internalErr(w, "rows routes", err)
		return
	}

	sortRoutesNaturally(routes)

	setCacheControl(w, 300)
	writeJSON(w, http.StatusOK, map[string]any{
		"count":  len(routes),
		"routes": routes,
	})
}

func (h *Handler) fetchRouteSummary(ctx context.Context, feed, rawID string) (routeSummaryDTO, bool, error) {
	var rt routeSummaryDTO
	err := h.pool.QueryRow(ctx, `
		SELECT feed, route_short_name, COALESCE(route_long_name, ''),
		       route_type, COALESCE(route_color, ''), COALESCE(route_text_color, '')
		FROM routes
		WHERE feed = $1 AND route_id = $2
	`, feed, rawID).Scan(&rt.Operator, &rt.ShortName, &rt.LongName, &rt.RouteType, &rt.Color, &rt.TextColor)
	if errors.Is(err, pgx.ErrNoRows) {
		return rt, false, nil
	}
	if err != nil {
		return rt, false, err
	}
	rt.ID = feed + ":" + rawID
	return rt, true, nil
}

// GetRoute returns route metadata and representative variants/stop sequences per direction.
func (h *Handler) GetRoute(w http.ResponseWriter, r *http.Request) {
	rawParam := chi.URLParam(r, "routeID")
	feed, rawID, ok := parseCanonicalID(rawParam)
	if !ok {
		writeError(w, http.StatusBadRequest, "routeID must be formatted as '<operator>:<id>'")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rt, found, err := h.fetchRouteSummary(ctx, feed, rawID)
	if err != nil {
		h.internalErr(w, "get route", err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "route not found")
		return
	}

	// Pick the trip with the most stops for each (direction_id, trip_headsign) as representative variant.
	varRows, err := h.pool.Query(ctx, `
		WITH trip_counts AS (
			SELECT t.direction_id, COALESCE(t.trip_headsign, '') AS headsign,
			       t.trip_id, COALESCE(t.shape_id, '') AS shape_id,
			       COUNT(st.stop_sequence) AS stop_cnt
			FROM trips t
			JOIN stop_times st ON st.feed = t.feed AND st.trip_id = t.trip_id
			WHERE t.feed = $1 AND t.route_id = $2
			GROUP BY t.direction_id, COALESCE(t.trip_headsign, ''), t.trip_id, COALESCE(t.shape_id, '')
		),
		ranked AS (
			SELECT direction_id, headsign, trip_id, shape_id, stop_cnt,
			       COUNT(*) OVER (PARTITION BY direction_id, headsign) AS variant_trips,
			       ROW_NUMBER() OVER (PARTITION BY direction_id, headsign ORDER BY stop_cnt DESC, trip_id ASC) AS rn
			FROM trip_counts
		)
		SELECT direction_id, headsign, trip_id, shape_id, variant_trips
		FROM ranked
		WHERE rn = 1
		ORDER BY direction_id ASC, variant_trips DESC, headsign ASC
	`, feed, rawID)
	if err != nil {
		h.internalErr(w, "query route variants", err)
		return
	}
	defer varRows.Close()

	type variantStopDTO struct {
		StopSequence int     `json:"stop_sequence"`
		StopID       string  `json:"stop_id"`
		GroupID      string  `json:"group_id"`
		Name         string  `json:"name"`
		PlatformCode string  `json:"platform_code,omitempty"`
		IsOnDemand   bool    `json:"is_on_demand"`
		Lat          float64 `json:"lat"`
		Lon          float64 `json:"lon"`
	}
	type routeVariantDTO struct {
		DirectionID        int              `json:"direction_id"`
		Headsign           string           `json:"headsign"`
		RepresentativeTrip string           `json:"representative_trip_id"`
		ShapeID            string           `json:"shape_id,omitempty"`
		TripCount          int              `json:"trip_count"`
		Stops              []variantStopDTO `json:"stops"`
	}

	var variants []routeVariantDTO
	var repTripIDs []string
	for varRows.Next() {
		var v routeVariantDTO
		var repTrip, shapeID string
		if err := varRows.Scan(&v.DirectionID, &v.Headsign, &repTrip, &shapeID, &v.TripCount); err != nil {
			h.internalErr(w, "scan route variants", err)
			return
		}
		v.RepresentativeTrip = feed + ":" + repTrip
		if shapeID != "" {
			v.ShapeID = feed + ":" + shapeID
		}
		v.Stops = []variantStopDTO{}
		variants = append(variants, v)
		repTripIDs = append(repTripIDs, repTrip)
	}
	if err := varRows.Err(); err != nil {
		h.internalErr(w, "rows route variants", err)
		return
	}

	if len(repTripIDs) > 0 {
		stopRows, err := h.pool.Query(ctx, `
			SELECT st.trip_id, st.stop_sequence, st.stop_id, COALESCE(s.group_id, ''),
			       s.stop_name, s.platform_code, s.is_on_demand, s.stop_lat, s.stop_lon
			FROM stop_times st
			JOIN stops s ON s.feed = st.feed AND s.stop_id = st.stop_id
			WHERE st.feed = $1 AND st.trip_id = ANY($2)
			ORDER BY st.trip_id, st.stop_sequence ASC
		`, feed, repTripIDs)
		if err != nil {
			h.internalErr(w, "query variant stops", err)
			return
		}
		defer stopRows.Close()

		byTrip := make(map[string][]variantStopDTO, len(repTripIDs))
		for stopRows.Next() {
			var tid, rawSID string
			var st variantStopDTO
			if err := stopRows.Scan(&tid, &st.StopSequence, &rawSID, &st.GroupID, &st.Name, &st.PlatformCode, &st.IsOnDemand, &st.Lat, &st.Lon); err != nil {
				h.internalErr(w, "scan variant stops", err)
				return
			}
			st.StopID = feed + ":" + rawSID
			byTrip[feed+":"+tid] = append(byTrip[feed+":"+tid], st)
		}
		if err := stopRows.Err(); err != nil {
			h.internalErr(w, "rows variant stops", err)
			return
		}
		for i := range variants {
			if ss, ok := byTrip[variants[i].RepresentativeTrip]; ok {
				variants[i].Stops = ss
			}
		}
	}

	setCacheControl(w, 300)
	writeJSON(w, http.StatusOK, map[string]any{
		"route":    rt,
		"variants": variants,
	})
}

type routeDirectionDTO struct {
	DirectionID      int      `json:"direction_id"`
	Headsign         string   `json:"headsign"`
	VariantHeadsigns []string `json:"variant_headsigns,omitempty"`
	TripCount        int      `json:"trip_count"`
}

type timetableRouteStopDTO struct {
	StopSequence int     `json:"stop_sequence"`
	StopID       string  `json:"stop_id"`
	GroupID      string  `json:"group_id"`
	Name         string  `json:"name"`
	PlatformCode string  `json:"platform_code,omitempty"`
	IsOnDemand   bool    `json:"is_on_demand"`
	IsTerminus   bool    `json:"is_terminus"`
	Lat          float64 `json:"lat"`
	Lon          float64 `json:"lon"`
}

type timetableEntryDTO struct {
	TripID               string `json:"trip_id"`
	Hour                 int    `json:"hour"`
	DisplayHour          int    `json:"display_hour"`
	Minute               int    `json:"minute"`
	Time                 string `json:"time"`
	VariantCode          string `json:"variant_code,omitempty"`
	Headsign             string `json:"headsign"`
	WheelchairAccessible bool   `json:"wheelchair_accessible"`
}

type timetableHourRowDTO struct {
	Hour        int                 `json:"hour"`
	DisplayHour int                 `json:"display_hour"`
	Departures  []timetableEntryDTO `json:"departures"`
}

type timetableDayDTO struct {
	ServiceID   string                `json:"service_id"`
	Label       string                `json:"label"`
	ShortLabel  string                `json:"short_label"`
	ActiveToday bool                  `json:"active_today"`
	Hours       []timetableHourRowDTO `json:"hours"`
}

type timetableLegendDTO struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

type rawTimetableDep struct {
	TripID               string
	ServiceID            string
	Headsign             string
	DestStopName         string
	DepartureSecs        int
	WheelchairAccessible bool
}

// GetRouteTimetable returns direction options, ordered route stops, and a Polish-style
// "tabliczka przystankowa" departure grid grouped by service day type with variant legend.
func (h *Handler) GetRouteTimetable(w http.ResponseWriter, r *http.Request) {
	rawParam := chi.URLParam(r, "routeID")
	feed, rawID, ok := parseCanonicalID(rawParam)
	if !ok {
		writeError(w, http.StatusBadRequest, "routeID must be formatted as '<operator>:<id>'")
		return
	}

	var requestedDir *int
	if rawDir := strings.TrimSpace(r.URL.Query().Get("direction_id")); rawDir != "" {
		d, err := strconv.Atoi(rawDir)
		if err != nil {
			writeError(w, http.StatusBadRequest, "direction_id must be an integer")
			return
		}
		requestedDir = &d
	}

	rawStopParam := strings.TrimSpace(r.URL.Query().Get("stop_id"))
	requestedRawStop := ""
	if rawStopParam != "" {
		if f, sid, ok := parseCanonicalID(rawStopParam); ok {
			if f != feed {
				writeError(w, http.StatusBadRequest, "stop_id operator must match route_id operator")
				return
			}
			requestedRawStop = sid
		} else {
			requestedRawStop = rawStopParam
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rt, found, err := h.fetchRouteSummary(ctx, feed, rawID)
	if err != nil {
		h.internalErr(w, "get route timetable", err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "route not found")
		return
	}

	dirRows, err := h.pool.Query(ctx, `
		SELECT t.direction_id, COALESCE(t.trip_headsign, '') AS headsign, COUNT(*) AS trip_cnt
		FROM trips t
		WHERE t.feed = $1 AND t.route_id = $2
		GROUP BY t.direction_id, COALESCE(t.trip_headsign, '')
		ORDER BY t.direction_id ASC, trip_cnt DESC, headsign ASC
	`, feed, rawID)
	if err != nil {
		h.internalErr(w, "query route directions", err)
		return
	}
	defer dirRows.Close()

	var directions []routeDirectionDTO
	dirIndex := make(map[int]int)
	for dirRows.Next() {
		var dID, cnt int
		var hs string
		if err := dirRows.Scan(&dID, &hs, &cnt); err != nil {
			h.internalErr(w, "scan route directions", err)
			return
		}
		if idx, exists := dirIndex[dID]; exists {
			directions[idx].TripCount += cnt
			if hs != "" && hs != directions[idx].Headsign {
				directions[idx].VariantHeadsigns = append(directions[idx].VariantHeadsigns, hs)
			}
			continue
		}
		dirIndex[dID] = len(directions)
		directions = append(directions, routeDirectionDTO{
			DirectionID: dID,
			Headsign:    hs,
			TripCount:   cnt,
		})
	}
	if err := dirRows.Err(); err != nil {
		h.internalErr(w, "rows route directions", err)
		return
	}

	if len(directions) == 0 {
		setCacheControl(w, 300)
		writeJSON(w, http.StatusOK, map[string]any{
			"route":              rt,
			"directions":         []routeDirectionDTO{},
			"stops":              []timetableRouteStopDTO{},
			"active_service_ids": []string{},
			"day_types":          []timetableDayDTO{},
			"legend":             []timetableLegendDTO{},
		})
		return
	}

	selectedDir := directions[0].DirectionID
	if requestedDir != nil {
		if _, exists := dirIndex[*requestedDir]; !exists {
			writeError(w, http.StatusNotFound, "direction_id not found for route")
			return
		}
		selectedDir = *requestedDir
	}

	// Ordered stops from the longest representative trip in the selected direction, plus any branch stops.
	stopRows, err := h.pool.Query(ctx, `
		WITH longest AS (
			SELECT t.trip_id
			FROM trips t
			JOIN stop_times st ON st.feed = t.feed AND st.trip_id = t.trip_id
			WHERE t.feed = $1 AND t.route_id = $2 AND t.direction_id = $3
			GROUP BY t.trip_id
			ORDER BY COUNT(*) DESC, t.trip_id ASC
			LIMIT 1
		),
		backbone AS (
			SELECT st.stop_sequence, st.stop_id,
			       NOT (st.pickup_type <> 1 AND EXISTS (
			           SELECT 1 FROM stop_times nx
			           WHERE nx.feed = st.feed AND nx.trip_id = st.trip_id AND nx.stop_sequence > st.stop_sequence
			       )) AS is_terminus
			FROM longest l
			JOIN stop_times st ON st.feed = $1 AND st.trip_id = l.trip_id
		),
		all_dir_stops AS (
			SELECT st.stop_id,
			       MIN(st.stop_sequence) AS min_seq,
			       NOT BOOL_OR(st.pickup_type <> 1 AND EXISTS (
			           SELECT 1 FROM stop_times nx
			           WHERE nx.feed = st.feed AND nx.trip_id = st.trip_id AND nx.stop_sequence > st.stop_sequence
			       )) AS is_terminus
			FROM trips t
			JOIN stop_times st ON st.feed = t.feed AND st.trip_id = t.trip_id
			WHERE t.feed = $1 AND t.route_id = $2 AND t.direction_id = $3
			GROUP BY st.stop_id
		)
		SELECT COALESCE(b.stop_sequence, 100000 + ads.min_seq) AS sort_seq,
		       ads.stop_id, COALESCE(s.group_id, ''), s.stop_name, s.platform_code,
		       s.is_on_demand, ads.is_terminus, s.stop_lat, s.stop_lon
		FROM all_dir_stops ads
		LEFT JOIN backbone b ON b.stop_id = ads.stop_id
		JOIN stops s ON s.feed = $1 AND s.stop_id = ads.stop_id
		ORDER BY (b.stop_sequence IS NULL) ASC, sort_seq ASC, ads.stop_id ASC
	`, feed, rawID, selectedDir)
	if err != nil {
		h.internalErr(w, "query direction stops", err)
		return
	}
	defer stopRows.Close()

	stops := make([]timetableRouteStopDTO, 0)
	stopByRawID := make(map[string]timetableRouteStopDTO)
	for stopRows.Next() {
		var st timetableRouteStopDTO
		var rawSID string
		if err := stopRows.Scan(
			&st.StopSequence, &rawSID, &st.GroupID, &st.Name, &st.PlatformCode,
			&st.IsOnDemand, &st.IsTerminus, &st.Lat, &st.Lon,
		); err != nil {
			h.internalErr(w, "scan direction stops", err)
			return
		}
		st.StopSequence = len(stops) + 1
		st.StopID = feed + ":" + rawSID
		stops = append(stops, st)
		stopByRawID[rawSID] = st
	}
	if err := stopRows.Err(); err != nil {
		h.internalErr(w, "rows direction stops", err)
		return
	}

	var selectedStop *timetableRouteStopDTO
	if requestedRawStop != "" {
		st, ok := stopByRawID[requestedRawStop]
		if !ok {
			writeError(w, http.StatusNotFound, "stop_id not found on selected route direction")
			return
		}
		selectedStop = &st
	} else if len(stops) > 0 {
		pick := stops[0]
		for _, st := range stops {
			if !st.IsTerminus {
				pick = st
				break
			}
		}
		selectedStop = &pick
		requestedRawStop = strings.TrimPrefix(pick.StopID, feed+":")
	}

	today := time.Now().In(h.loc).Format("2006-01-02")
	activeRows, err := h.pool.Query(ctx, `
		SELECT service_id
		FROM calendar_dates
		WHERE feed = $1 AND service_date = $2::date AND exception_type = 1
	`, feed, today)
	if err != nil {
		h.internalErr(w, "query active service ids", err)
		return
	}
	defer activeRows.Close()

	activeSet := make(map[string]bool)
	activeList := make([]string, 0, 4)
	for activeRows.Next() {
		var sid string
		if err := activeRows.Scan(&sid); err != nil {
			h.internalErr(w, "scan active service ids", err)
			return
		}
		if !activeSet[sid] {
			activeSet[sid] = true
			activeList = append(activeList, sid)
		}
	}
	if err := activeRows.Err(); err != nil {
		h.internalErr(w, "rows active service ids", err)
		return
	}

	var rawDeps []rawTimetableDep
	if requestedRawStop != "" {
		depRows, err := h.pool.Query(ctx, `
			SELECT t.trip_id, t.service_id, COALESCE(t.trip_headsign, ''),
			       COALESCE(last_s.stop_name, ''), st.departure_secs, t.wheelchair_accessible
			FROM trips t
			JOIN stop_times st ON st.feed = t.feed AND st.trip_id = t.trip_id
			JOIN LATERAL (
				SELECT s2.stop_name
				FROM stop_times st2
				JOIN stops s2 ON s2.feed = st2.feed AND s2.stop_id = st2.stop_id
				WHERE st2.feed = t.feed AND st2.trip_id = t.trip_id
				ORDER BY st2.stop_sequence DESC
				LIMIT 1
			) last_s ON true
			WHERE t.feed = $1 AND t.route_id = $2 AND t.direction_id = $3
			  AND st.stop_id = $4
			  AND st.pickup_type <> 1
			  AND EXISTS (
			      SELECT 1 FROM stop_times nx
			      WHERE nx.feed = st.feed AND nx.trip_id = st.trip_id AND nx.stop_sequence > st.stop_sequence
			  )
			ORDER BY st.departure_secs ASC, t.trip_id ASC
		`, feed, rawID, selectedDir, requestedRawStop)
		if err != nil {
			h.internalErr(w, "query timetable departures", err)
			return
		}
		defer depRows.Close()

		for depRows.Next() {
			var d rawTimetableDep
			var rawTID string
			var wheelchair int
			if err := depRows.Scan(&rawTID, &d.ServiceID, &d.Headsign, &d.DestStopName, &d.DepartureSecs, &wheelchair); err != nil {
				h.internalErr(w, "scan timetable departures", err)
				return
			}
			d.TripID = feed + ":" + rawTID
			d.WheelchairAccessible = wheelchair == 1
			rawDeps = append(rawDeps, d)
		}
		if err := depRows.Err(); err != nil {
			h.internalErr(w, "rows timetable departures", err)
			return
		}
	}

	dayTypes, legend := buildRouteTimetableDays(rawDeps, activeSet)

	setCacheControl(w, 300)
	writeJSON(w, http.StatusOK, map[string]any{
		"route":                 rt,
		"directions":            directions,
		"selected_direction_id": selectedDir,
		"stops":                 stops,
		"selected_stop":         selectedStop,
		"active_service_ids":    activeList,
		"day_types":             dayTypes,
		"legend":                legend,
	})
}

func buildRouteTimetableDays(deps []rawTimetableDep, activeSet map[string]bool) ([]timetableDayDTO, []timetableLegendDTO) {
	if len(deps) == 0 {
		return []timetableDayDTO{}, []timetableLegendDTO{}
	}

	type variantKey struct {
		headsign string
		dest     string
	}
	variantCounts := make(map[variantKey]int)
	hasPW := false
	servicesSeen := make(map[string]bool)
	for _, d := range deps {
		variantCounts[variantKey{headsign: d.Headsign, dest: d.DestStopName}]++
		servicesSeen[d.ServiceID] = true
		if d.ServiceID == "PW" {
			hasPW = true
		}
	}

	// Primary variant has the highest frequency (tie-broken lexicographically).
	var primary variantKey
	maxCnt := -1
	for k, cnt := range variantCounts {
		if cnt > maxCnt || (cnt == maxCnt && (k.headsign < primary.headsign || (k.headsign == primary.headsign && k.dest < primary.dest))) {
			maxCnt = cnt
			primary = k
		}
	}

	var nonPrimary []variantKey
	for k := range variantCounts {
		if k != primary {
			nonPrimary = append(nonPrimary, k)
		}
	}
	sort.Slice(nonPrimary, func(i, j int) bool {
		ci, cj := variantCounts[nonPrimary[i]], variantCounts[nonPrimary[j]]
		if ci != cj {
			return ci > cj
		}
		if nonPrimary[i].headsign != nonPrimary[j].headsign {
			return nonPrimary[i].headsign < nonPrimary[j].headsign
		}
		return nonPrimary[i].dest < nonPrimary[j].dest
	})

	variantCodeByKey := make(map[variantKey]string, len(nonPrimary))
	legend := make([]timetableLegendDTO, 0, len(nonPrimary))
	for idx, k := range nonPrimary {
		var code string
		if idx < 26 {
			code = string(rune('A' + idx))
		} else {
			code = fmt.Sprintf("V%d", idx+1)
		}
		variantCodeByKey[k] = code
		desc := k.headsign
		if desc == "" {
			desc = k.dest
		}
		if k.dest != "" && !strings.EqualFold(desc, k.dest) {
			desc = fmt.Sprintf("Kurs do: %s (przystanek końcowy: %s)", desc, k.dest)
		} else if desc != "" {
			desc = "Kurs do: " + desc
		} else {
			desc = "Wariant trasy"
		}
		legend = append(legend, timetableLegendDTO{
			Code:        code,
			Description: desc,
		})
	}

	byServiceHour := make(map[string]map[int][]timetableEntryDTO, len(servicesSeen))
	for _, d := range deps {
		hr := d.DepartureSecs / 3600
		min := (d.DepartureSecs % 3600) / 60
		dispHr := hr % 24
		if byServiceHour[d.ServiceID] == nil {
			byServiceHour[d.ServiceID] = make(map[int][]timetableEntryDTO)
		}
		code := variantCodeByKey[variantKey{headsign: d.Headsign, dest: d.DestStopName}]
		byServiceHour[d.ServiceID][hr] = append(byServiceHour[d.ServiceID][hr], timetableEntryDTO{
			TripID:               d.TripID,
			Hour:                 hr,
			DisplayHour:          dispHr,
			Minute:               min,
			Time:                 fmt.Sprintf("%02d:%02d", dispHr, min),
			VariantCode:          code,
			Headsign:             d.Headsign,
			WheelchairAccessible: d.WheelchairAccessible,
		})
	}

	serviceIDs := make([]string, 0, len(servicesSeen))
	for sid := range servicesSeen {
		serviceIDs = append(serviceIDs, sid)
	}
	sort.Slice(serviceIDs, func(i, j int) bool {
		_, _, oi := describeServiceID(serviceIDs[i], hasPW)
		_, _, oj := describeServiceID(serviceIDs[j], hasPW)
		if oi != oj {
			return oi < oj
		}
		return serviceIDs[i] < serviceIDs[j]
	})

	dayTypes := make([]timetableDayDTO, 0, len(serviceIDs))
	for _, sid := range serviceIDs {
		label, shortLabel, _ := describeServiceID(sid, hasPW)
		hourMap := byServiceHour[sid]
		hours := make([]int, 0, len(hourMap))
		for hr := range hourMap {
			hours = append(hours, hr)
		}
		sort.Ints(hours)

		hourRows := make([]timetableHourRowDTO, 0, len(hours))
		for _, hr := range hours {
			entries := hourMap[hr]
			sort.Slice(entries, func(i, j int) bool {
				if entries[i].Minute != entries[j].Minute {
					return entries[i].Minute < entries[j].Minute
				}
				return entries[i].TripID < entries[j].TripID
			})
			hourRows = append(hourRows, timetableHourRowDTO{
				Hour:        hr,
				DisplayHour: hr % 24,
				Departures:  entries,
			})
		}
		dayTypes = append(dayTypes, timetableDayDTO{
			ServiceID:   sid,
			Label:       label,
			ShortLabel:  shortLabel,
			ActiveToday: activeSet[sid],
			Hours:       hourRows,
		})
	}
	return dayTypes, legend
}

func describeServiceID(serviceID string, hasPW bool) (label, shortLabel string, order int) {
	switch strings.ToUpper(strings.TrimSpace(serviceID)) {
	case "PS":
		if hasPW {
			return "Dni robocze (nauka szkolna)", "Rob. szkolne", 1
		}
		return "Dni robocze", "Robocze", 1
	case "PW":
		return "Dni robocze (ferie / wakacje)", "Rob. wolne", 2
	case "SS":
		return "Soboty", "Sobota", 3
	case "NS":
		return "Niedziele i święta", "Niedz. / Św.", 4
	default:
		return serviceID, serviceID, 9
	}
}

// GetTrip returns trip metadata, ordered stop calls with live estimates, active vehicle, and optional shape.
func (h *Handler) GetTrip(w http.ResponseWriter, r *http.Request) {
	rawParam := chi.URLParam(r, "tripID")
	feed, rawID, ok := parseCanonicalID(rawParam)
	if !ok {
		writeError(w, http.StatusBadRequest, "tripID must be formatted as '<operator>:<id>'")
		return
	}

	now := time.Now().In(h.loc)
	nowUTC := now.UTC()

	var explicitServiceDate *time.Time
	if rawSvcDate := strings.TrimSpace(r.URL.Query().Get("service_date")); rawSvcDate != "" {
		parsedDate, err := time.ParseInLocation("2006-01-02", rawSvcDate, h.loc)
		if err != nil {
			writeError(w, http.StatusBadRequest, "service_date must be formatted as YYYY-MM-DD")
			return
		}
		explicitServiceDate = &parsedDate
	}

	includeShape := r.URL.Query().Get("include_shape") == "true"

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var (
		routeID, routeShort, routeColor, routeTextColor string
		headsign, shapeID, brigade                      string
		directionID, wheelchair                         int
	)
	err := h.pool.QueryRow(ctx, `
		SELECT t.route_id, r.route_short_name, COALESCE(r.route_color, ''), COALESCE(r.route_text_color, ''),
		       COALESCE(t.trip_headsign, ''), COALESCE(t.shape_id, ''), COALESCE(t.brigade, ''),
		       t.direction_id, t.wheelchair_accessible
		FROM trips t
		JOIN routes r ON r.feed = t.feed AND r.route_id = t.route_id
		WHERE t.feed = $1 AND t.trip_id = $2
	`, feed, rawID).Scan(
		&routeID, &routeShort, &routeColor, &routeTextColor,
		&headsign, &shapeID, &brigade, &directionID, &wheelchair,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "trip not found")
		return
	}
	if err != nil {
		h.internalErr(w, "get trip", err)
		return
	}

	rows, err := h.pool.Query(ctx, `
		SELECT st.stop_sequence, st.stop_id, COALESCE(s.group_id, ''), s.stop_name,
		       s.platform_code, s.is_on_demand, s.stop_lat, s.stop_lon,
		       st.arrival_secs, st.departure_secs,
		       st.pickup_type, st.drop_off_type, st.shape_dist_traveled
		FROM stop_times st
		JOIN stops s ON s.feed = st.feed AND s.stop_id = st.stop_id
		WHERE st.feed = $1 AND st.trip_id = $2
		ORDER BY st.stop_sequence ASC
	`, feed, rawID)
	if err != nil {
		h.internalErr(w, "query trip stops", err)
		return
	}
	defer rows.Close()

	type rawTripStop struct {
		stopSeq           int
		rawSID            string
		groupID           string
		stopName          string
		platformCode      string
		isOnDemand        bool
		lat               float64
		lon               float64
		arrSecs           int
		depSecs           int
		pickupType        int
		dropOffType       int
		shapeDistTraveled *float64
	}

	rawStops := make([]rawTripStop, 0)
	maxDepSecs := 0
	for rows.Next() {
		var rs rawTripStop
		if err := rows.Scan(
			&rs.stopSeq, &rs.rawSID, &rs.groupID, &rs.stopName,
			&rs.platformCode, &rs.isOnDemand, &rs.lat, &rs.lon,
			&rs.arrSecs, &rs.depSecs,
			&rs.pickupType, &rs.dropOffType, &rs.shapeDistTraveled,
		); err != nil {
			h.internalErr(w, "scan trip stops", err)
			return
		}
		if rs.depSecs > maxDepSecs {
			maxDepSecs = rs.depSecs
		}
		rawStops = append(rawStops, rs)
	}
	if err := rows.Err(); err != nil {
		h.internalErr(w, "rows trip stops", err)
		return
	}

	serviceDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, h.loc)
	if explicitServiceDate != nil {
		serviceDate = *explicitServiceDate
	} else if now.Hour() < 6 && maxDepSecs >= 24*3600 {
		// Night courses extending past midnight belong to yesterday's service day until 06:00.
		serviceDate = serviceDate.AddDate(0, 0, -1)
	}
	noon := time.Date(serviceDate.Year(), serviceDate.Month(), serviceDate.Day(), 12, 0, 0, 0, h.loc)
	origin := noon.Add(-12 * time.Hour)

	type tripStopDTO struct {
		StopSequence           int       `json:"stop_sequence"`
		StopID                 string    `json:"stop_id"`
		GroupID                string    `json:"group_id"`
		StopName               string    `json:"stop_name"`
		PlatformCode           string    `json:"platform_code,omitempty"`
		IsOnDemand             bool      `json:"is_on_demand"`
		Lat                    float64   `json:"lat"`
		Lon                    float64   `json:"lon"`
		PickupType             int       `json:"pickup_type"`
		DropOffType            int       `json:"drop_off_type"`
		ShapeDistTraveled      *float64  `json:"shape_dist_traveled,omitempty"`
		ScheduledArrival       string    `json:"scheduled_arrival"`
		ScheduledDeparture     string    `json:"scheduled_departure"`
		ScheduledArrivalTime   time.Time `json:"scheduled_arrival_time"`
		ScheduledDepartureTime time.Time `json:"scheduled_departure_time"`
		EstimatedArrival       time.Time `json:"estimated_arrival"`
		EstimatedDeparture     time.Time `json:"estimated_departure"`
		DelaySeconds           *int      `json:"delay_seconds"`
		IsRealtime             bool      `json:"is_realtime"`
		Skipped                bool      `json:"skipped"`
	}

	stops := make([]tripStopDTO, 0, len(rawStops))
	for _, rs := range rawStops {
		schedArr := origin.Add(time.Duration(rs.arrSecs) * time.Second)
		schedDep := origin.Add(time.Duration(rs.depSecs) * time.Second)
		est := h.rtStore.EstimateDeparture(feed, rawID, rs.stopSeq, rs.rawSID, schedDep, nowUTC)
		delayOffset := est.EstimatedTime.Sub(schedDep)

		stops = append(stops, tripStopDTO{
			StopSequence:           rs.stopSeq,
			StopID:                 feed + ":" + rs.rawSID,
			GroupID:                rs.groupID,
			StopName:               rs.stopName,
			PlatformCode:           rs.platformCode,
			IsOnDemand:             rs.isOnDemand,
			Lat:                    rs.lat,
			Lon:                    rs.lon,
			PickupType:             rs.pickupType,
			DropOffType:            rs.dropOffType,
			ShapeDistTraveled:      rs.shapeDistTraveled,
			ScheduledArrival:       formatGTFSSeconds(rs.arrSecs),
			ScheduledDeparture:     formatGTFSSeconds(rs.depSecs),
			ScheduledArrivalTime:   schedArr,
			ScheduledDepartureTime: schedDep,
			EstimatedArrival:       schedArr.Add(delayOffset),
			EstimatedDeparture:     est.EstimatedTime,
			DelaySeconds:           est.DelaySeconds,
			IsRealtime:             est.IsRealtime,
			Skipped:                est.Skipped,
		})
	}

	activeVehicle := h.rtStore.VehicleByTrip(feed, rawID, nowUTC)

	resp := map[string]any{
		"trip": map[string]any{
			"id":                    feed + ":" + rawID,
			"operator":              feed,
			"route_id":              feed + ":" + routeID,
			"route_short_name":      routeShort,
			"route_color":           routeColor,
			"route_text_color":      routeTextColor,
			"headsign":              headsign,
			"direction_id":          directionID,
			"brigade":               brigade,
			"wheelchair_accessible": wheelchair == 1,
			"shape_id":              conditionalPrefixed(feed, shapeID),
			"service_date":          serviceDate.Format("2006-01-02"),
			"has_live_location":     activeVehicle != nil,
		},
		"vehicle": activeVehicle,
		"stops":   stops,
	}

	if includeShape && shapeID != "" {
		var rawShape []byte
		if err := h.pool.QueryRow(ctx, `SELECT points FROM shapes WHERE feed = $1 AND shape_id = $2`, feed, shapeID).Scan(&rawShape); err == nil {
			if pts, err := gtfsstatic.DecodeShapePoints(rawShape); err == nil {
				resp["shape"] = pts
			}
		}
	}

	setCacheControl(w, 10)
	writeJSON(w, http.StatusOK, resp)
}

// GetShape returns decoded [lat, lon] coordinates for a canonical shape ID.
func (h *Handler) GetShape(w http.ResponseWriter, r *http.Request) {
	rawParam := chi.URLParam(r, "shapeID")
	feed, rawID, ok := parseCanonicalID(rawParam)
	if !ok {
		writeError(w, http.StatusBadRequest, "shapeID must be formatted as '<operator>:<id>'")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var raw []byte
	var count int
	err := h.pool.QueryRow(ctx, `SELECT points, point_count FROM shapes WHERE feed = $1 AND shape_id = $2`, feed, rawID).Scan(&raw, &count)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "shape not found")
		return
	}
	if err != nil {
		h.internalErr(w, "get shape", err)
		return
	}

	pts, err := gtfsstatic.DecodeShapePoints(raw)
	if err != nil {
		h.internalErr(w, "decode shape", err)
		return
	}

	sum := sha256.Sum256(raw)
	etag := `"` + hex.EncodeToString(sum[:8]) + `"`
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	setCacheControl(w, 3600)
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          feed + ":" + rawID,
		"operator":    feed,
		"point_count": count,
		"points":      pts,
	})
}

// ListVehicles returns live vehicles from in-memory GTFS-RT store with ETag conditional caching.
func (h *Handler) ListVehicles(w http.ResponseWriter, r *http.Request) {
	op, ok := parseOperatorFilter(r.URL.Query().Get("operator"))
	if !ok {
		writeError(w, http.StatusBadRequest, "operator must be 'ztm' or 'pks'")
		return
	}
	routeFilter := strings.TrimSpace(r.URL.Query().Get("route"))

	var bbox *[4]float64
	if rawBBox := strings.TrimSpace(r.URL.Query().Get("bbox")); rawBBox != "" {
		parsed, err := parseBBox(rawBBox)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bbox must be 'minLat,minLon,maxLat,maxLon'")
			return
		}
		bbox = &parsed
	}

	etag := fmt.Sprintf(`W/"%s-%s-%s-%s"`, h.rtStore.Version(), op, routeFilter, r.URL.Query().Get("bbox"))
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	now := time.Now().UTC()
	vehicles, anyStale := h.rtStore.Vehicles(op, routeFilter, bbox, now)

	setCacheControl(w, 5)
	writeJSON(w, http.StatusOK, map[string]any{
		"timestamp": now,
		"stale":     anyStale,
		"count":     len(vehicles),
		"vehicles":  vehicles,
	})
}

// ListAlerts returns active GTFS-RT service alerts for ZTM and PKS.
func (h *Handler) ListAlerts(w http.ResponseWriter, r *http.Request) {
	op, ok := parseOperatorFilter(r.URL.Query().Get("operator"))
	if !ok {
		writeError(w, http.StatusBadRequest, "operator must be 'ztm' or 'pks'")
		return
	}

	now := time.Now().UTC()
	alerts := h.rtStore.Alerts(op, now)

	setCacheControl(w, 15)
	writeJSON(w, http.StatusOK, map[string]any{
		"timestamp": now,
		"count":     len(alerts),
		"alerts":    alerts,
	})
}

// ListFares returns ticket tariff definitions imported from GTFS fare_products.txt.
func (h *Handler) ListFares(w http.ResponseWriter, r *http.Request) {
	op, ok := parseOperatorFilter(r.URL.Query().Get("operator"))
	if !ok {
		writeError(w, http.StatusBadRequest, "operator must be 'ztm' or 'pks'")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := h.pool.Query(ctx, `
		SELECT feed, fare_product_id, fare_product_name, COALESCE(fare_media_id, ''),
		       COALESCE(rider_category_id, ''), amount, currency
		FROM fare_products
		WHERE ($1 = '' OR feed = $1)
		ORDER BY feed ASC, fare_product_name ASC
	`, op)
	if err != nil {
		h.internalErr(w, "query fare_products", err)
		return
	}
	defer rows.Close()

	type fareDTO struct {
		ID            string `json:"id"`
		Operator      string `json:"operator"`
		Name          string `json:"name"`
		MediaID       string `json:"media_id,omitempty"`
		RiderCategory string `json:"rider_category,omitempty"`
		Amount        string `json:"amount"`
		Currency      string `json:"currency"`
	}

	fares := make([]fareDTO, 0)
	for rows.Next() {
		var f fareDTO
		var rawID string
		if err := rows.Scan(&f.Operator, &rawID, &f.Name, &f.MediaID, &f.RiderCategory, &f.Amount, &f.Currency); err != nil {
			h.internalErr(w, "scan fare_products", err)
			return
		}
		f.ID = f.Operator + ":" + rawID
		fares = append(fares, f)
	}
	if err := rows.Err(); err != nil {
		h.internalErr(w, "rows fare_products", err)
		return
	}

	setCacheControl(w, 600)
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(fares),
		"fares": fares,
	})
}

func (h *Handler) internalErr(w http.ResponseWriter, op string, err error) {
	h.log.Error(op, "error", err)
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func parseCanonicalID(s string) (feed, rawID string, ok bool) {
	if decoded, err := url.PathUnescape(s); err == nil {
		s = decoded
	}
	feed, rawID, ok = strings.Cut(strings.TrimSpace(s), ":")
	if !ok || rawID == "" || (feed != "ztm" && feed != "pks") {
		return "", "", false
	}
	return feed, rawID, true
}

func conditionalPrefixed(feed, rawID string) string {
	if rawID == "" {
		return ""
	}
	return feed + ":" + rawID
}

func parseOperatorFilter(raw string) (string, bool) {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case "", "ztm", "pks":
		return v, true
	default:
		return "", false
	}
}

func parseIntClamped(raw string, def, minVal, maxVal int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if v < minVal {
		return minVal
	}
	if v > maxVal {
		return maxVal
	}
	return v
}

func parseAtParam(raw string, loc *time.Location) (time.Time, error) {
	if unixSec, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Unix(unixSec, 0).In(loc), nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.In(loc), nil
	}
	return time.Time{}, errors.New("invalid time")
}

func parseBBox(raw string) ([4]float64, error) {
	var box [4]float64
	parts := strings.Split(raw, ",")
	if len(parts) != 4 {
		return box, errors.New("expected 4 floats")
	}
	for i := 0; i < 4; i++ {
		v, err := strconv.ParseFloat(strings.TrimSpace(parts[i]), 64)
		if err != nil {
			return box, err
		}
		box[i] = v
	}
	if box[0] > box[2] || box[1] > box[3] {
		return box, errors.New("min > max")
	}
	return box, nil
}

func formatGTFSSeconds(secs int) string {
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func sortRoutesNaturally(routes []routeSummaryDTO) {
	sort.Slice(routes, func(i, j int) bool {
		ni, okI := leadingInt(routes[i].ShortName)
		nj, okJ := leadingInt(routes[j].ShortName)
		if okI && okJ && ni != nj {
			return ni < nj
		}
		if okI != okJ {
			return okI
		}
		if routes[i].ShortName != routes[j].ShortName {
			return routes[i].ShortName < routes[j].ShortName
		}
		return routes[i].Operator < routes[j].Operator
	})
}

func leadingInt(s string) (int, bool) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, false
	}
	v, err := strconv.Atoi(s[:i])
	return v, err == nil
}
