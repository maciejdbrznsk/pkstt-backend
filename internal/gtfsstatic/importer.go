package gtfsstatic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxZipBytes = 128 << 20 // 128 MiB upper safety cap

// TripMeta holds lightweight trip+route metadata cached in memory for enriching GTFS-RT feeds.
type TripMeta struct {
	Feed           string
	TripID         string
	RouteID        string
	RouteShortName string
	RouteColor     string
	RouteTextColor string
	Headsign       string
	DirectionID    int
	Brigade        string
}

// Importer downloads, parses, and atomically loads GTFS static archives into PostgreSQL.
type Importer struct {
	pool      *pgxpool.Pool
	client    *http.Client
	userAgent string
	log       *slog.Logger
}

// NewImporter constructs a GTFS static importer.
func NewImporter(pool *pgxpool.Pool, userAgent string, log *slog.Logger) *Importer {
	return &Importer{
		pool: pool,
		client: &http.Client{
			Timeout: 90 * time.Second,
		},
		userAgent: userAgent,
		log:       log,
	}
}

// NeedsInitialSync reports whether the feed has zero stops currently loaded in PostgreSQL.
func (im *Importer) NeedsInitialSync(ctx context.Context, feed string) (bool, error) {
	var count int
	if err := im.pool.QueryRow(ctx, `SELECT COUNT(*) FROM stops WHERE feed = $1`, feed).Scan(&count); err != nil {
		return false, err
	}
	return count == 0, nil
}

// SyncFeed checks the remote ZIP using ETag/Last-Modified and atomically imports it if changed.
func (im *Importer) SyncFeed(ctx context.Context, feed, sourceURL string) (bool, error) {
	var prevETag, prevLastMod, prevSHA *string
	err := im.pool.QueryRow(ctx,
		`SELECT etag, last_modified, content_sha256 FROM feed_state WHERE feed = $1`,
		feed,
	).Scan(&prevETag, &prevLastMod, &prevSHA)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("read feed_state: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", im.userAgent)
	if prevETag != nil && *prevETag != "" {
		req.Header.Set("If-None-Match", *prevETag)
	}
	if prevLastMod != nil && *prevLastMod != "" {
		req.Header.Set("If-Modified-Since", *prevLastMod)
	}

	resp, err := im.client.Do(req)
	if err != nil {
		im.recordError(ctx, feed, sourceURL, err)
		return false, fmt.Errorf("fetch %s: %w", sourceURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		if _, err := im.pool.Exec(ctx, `
			INSERT INTO feed_state (feed, source_url, last_checked_at, last_error)
			VALUES ($1, $2, NOW(), NULL)
			ON CONFLICT (feed) DO UPDATE
			SET last_checked_at = NOW(), last_error = NULL
		`, feed, sourceURL); err != nil {
			return false, err
		}
		im.log.Debug("gtfs static unchanged (304)", "feed", feed)
		return false, nil
	}

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("unexpected HTTP status %s", resp.Status)
		im.recordError(ctx, feed, sourceURL, err)
		return false, err
	}

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("gtfs-%s-*.zip", feed))
	if err != nil {
		return false, err
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	hasher := sha256.New()
	limited := io.LimitReader(resp.Body, maxZipBytes+1)
	written, err := io.Copy(io.MultiWriter(tmpFile, hasher), limited)
	closeErr := tmpFile.Close()
	if err != nil {
		im.recordError(ctx, feed, sourceURL, err)
		return false, fmt.Errorf("download %s: %w", feed, err)
	}
	if closeErr != nil {
		return false, closeErr
	}
	if written > maxZipBytes {
		err := fmt.Errorf("archive exceeds max allowed size (%d bytes)", maxZipBytes)
		im.recordError(ctx, feed, sourceURL, err)
		return false, err
	}

	shaHex := hex.EncodeToString(hasher.Sum(nil))
	newETag := resp.Header.Get("ETag")
	newLastMod := resp.Header.Get("Last-Modified")

	if prevSHA != nil && *prevSHA == shaHex {
		needsInit, _ := im.NeedsInitialSync(ctx, feed)
		if !needsInit {
			_, _ = im.pool.Exec(ctx, `
				UPDATE feed_state
				SET etag = $2, last_modified = $3, last_checked_at = NOW(), last_error = NULL
				WHERE feed = $1
			`, feed, newETag, newLastMod)
			im.log.Debug("gtfs static unchanged (sha256 match)", "feed", feed)
			return false, nil
		}
	}

	parsed, err := ParseZipFile(feed, tmpPath)
	if err != nil {
		im.recordError(ctx, feed, sourceURL, err)
		return false, fmt.Errorf("parse %s: %w", feed, err)
	}

	dedupeFeedData(parsed)

	if err := im.replaceFeedInDB(ctx, parsed, sourceURL, newETag, newLastMod, shaHex); err != nil {
		im.recordError(ctx, feed, sourceURL, err)
		return false, fmt.Errorf("load %s into db: %w", feed, err)
	}

	im.log.Info("gtfs static feed imported",
		"feed", feed,
		"version", parsed.Info.Version,
		"stops", len(parsed.Stops),
		"routes", len(parsed.Routes),
		"trips", len(parsed.Trips),
		"stop_times", len(parsed.StopTimes),
		"shapes", len(parsed.Shapes),
	)
	return true, nil
}

func (im *Importer) replaceFeedInDB(ctx context.Context, d *FeedData, sourceURL, etag, lastMod, shaHex string) error {
	tx, err := im.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	feedTables := []string{"stop_times", "trips", "shapes", "calendar_dates", "routes", "stops", "agencies", "fare_products"}
	for _, tbl := range feedTables {
		if _, err := tx.Exec(ctx, "DELETE FROM "+tbl+" WHERE feed = $1", d.Feed); err != nil {
			return fmt.Errorf("delete %s: %w", tbl, err)
		}
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"agencies"},
		[]string{"feed", "agency_id", "agency_name", "agency_url", "agency_timezone", "agency_phone"},
		pgx.CopyFromSlice(len(d.Agencies), func(i int) ([]any, error) {
			a := d.Agencies[i]
			return []any{d.Feed, a.AgencyID, a.Name, a.URL, a.Timezone, a.Phone}, nil
		}),
	); err != nil {
		return fmt.Errorf("copy agencies: %w", err)
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"stops"},
		[]string{"feed", "stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "zone_id", "street_name", "town_name", "group_id", "platform_code", "is_on_demand"},
		pgx.CopyFromSlice(len(d.Stops), func(i int) ([]any, error) {
			s := d.Stops[i]
			return []any{d.Feed, s.StopID, s.StopCode, s.StopName, s.Lat, s.Lon, s.ZoneID, s.StreetName, s.TownName, "", s.PlatformCode, s.IsOnDemand}, nil
		}),
	); err != nil {
		return fmt.Errorf("copy stops: %w", err)
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"routes"},
		[]string{"feed", "route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"},
		pgx.CopyFromSlice(len(d.Routes), func(i int) ([]any, error) {
			r := d.Routes[i]
			return []any{d.Feed, r.RouteID, r.AgencyID, r.ShortName, r.LongName, r.RouteType, r.Color, r.TextColor}, nil
		}),
	); err != nil {
		return fmt.Errorf("copy routes: %w", err)
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"calendar_dates"},
		[]string{"feed", "service_id", "service_date", "exception_type"},
		pgx.CopyFromSlice(len(d.Calendar), func(i int) ([]any, error) {
			c := d.Calendar[i]
			return []any{d.Feed, c.ServiceID, c.ServiceDate, c.ExceptionType}, nil
		}),
	); err != nil {
		return fmt.Errorf("copy calendar_dates: %w", err)
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"shapes"},
		[]string{"feed", "shape_id", "points", "point_count"},
		pgx.CopyFromSlice(len(d.Shapes), func(i int) ([]any, error) {
			sh := d.Shapes[i]
			return []any{d.Feed, sh.ShapeID, sh.Points, sh.PointCount}, nil
		}),
	); err != nil {
		return fmt.Errorf("copy shapes: %w", err)
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"trips"},
		[]string{"feed", "trip_id", "route_id", "service_id", "trip_headsign", "direction_id", "block_id", "shape_id", "wheelchair_accessible", "brigade"},
		pgx.CopyFromSlice(len(d.Trips), func(i int) ([]any, error) {
			t := d.Trips[i]
			return []any{d.Feed, t.TripID, t.RouteID, t.ServiceID, t.Headsign, t.DirectionID, t.BlockID, t.ShapeID, t.WheelchairAccessible, t.Brigade}, nil
		}),
	); err != nil {
		return fmt.Errorf("copy trips: %w", err)
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"stop_times"},
		[]string{"feed", "trip_id", "stop_sequence", "stop_id", "arrival_secs", "departure_secs", "pickup_type", "drop_off_type", "shape_dist_traveled"},
		pgx.CopyFromSlice(len(d.StopTimes), func(i int) ([]any, error) {
			st := d.StopTimes[i]
			return []any{d.Feed, st.TripID, st.StopSequence, st.StopID, st.ArrivalSecs, st.DepartureSecs, st.PickupType, st.DropOffType, st.ShapeDistTraveled}, nil
		}),
	); err != nil {
		return fmt.Errorf("copy stop_times: %w", err)
	}

	if len(d.FareProducts) > 0 {
		if _, err := tx.CopyFrom(ctx,
			pgx.Identifier{"fare_products"},
			[]string{"feed", "fare_product_id", "fare_product_name", "fare_media_id", "rider_category_id", "amount", "currency"},
			pgx.CopyFromSlice(len(d.FareProducts), func(i int) ([]any, error) {
				fp := d.FareProducts[i]
				return []any{d.Feed, fp.FareProductID, fp.FareProductName, fp.FareMediaID, fp.RiderCategoryID, fp.Amount, fp.Currency}, nil
			}),
		); err != nil {
			return fmt.Errorf("copy fare_products: %w", err)
		}
	}

	groupCount, err := rebuildStopGroupsTx(ctx, tx)
	if err != nil {
		return fmt.Errorf("rebuild stop_groups: %w", err)
	}

	statsJSON, _ := json.Marshal(map[string]int{
		"stops":       len(d.Stops),
		"routes":      len(d.Routes),
		"trips":       len(d.Trips),
		"stop_times":  len(d.StopTimes),
		"shapes":      len(d.Shapes),
		"stop_groups": groupCount,
	})

	if _, err := tx.Exec(ctx, `
		INSERT INTO feed_state (
			feed, source_url, etag, last_modified, content_sha256,
			feed_version, feed_start_date, feed_end_date,
			last_checked_at, last_updated_at, last_error, stats
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW(), NULL, $9)
		ON CONFLICT (feed) DO UPDATE SET
			source_url = EXCLUDED.source_url,
			etag = EXCLUDED.etag,
			last_modified = EXCLUDED.last_modified,
			content_sha256 = EXCLUDED.content_sha256,
			feed_version = EXCLUDED.feed_version,
			feed_start_date = EXCLUDED.feed_start_date,
			feed_end_date = EXCLUDED.feed_end_date,
			last_checked_at = NOW(),
			last_updated_at = NOW(),
			last_error = NULL,
			stats = EXCLUDED.stats
	`, d.Feed, sourceURL, etag, lastMod, shaHex, d.Info.Version, d.Info.StartDate, d.Info.EndDate, statsJSON); err != nil {
		return fmt.Errorf("upsert feed_state: %w", err)
	}

	return tx.Commit(ctx)
}

func rebuildStopGroupsTx(ctx context.Context, tx pgx.Tx) (int, error) {
	rows, err := tx.Query(ctx, `
		SELECT feed, stop_id, COALESCE(stop_code, ''), stop_name, stop_lat, stop_lon,
		       COALESCE(zone_id, ''), COALESCE(street_name, ''), COALESCE(town_name, ''),
		       platform_code, is_on_demand
		FROM stops
	`)
	if err != nil {
		return 0, err
	}

	var allStops []Stop
	for rows.Next() {
		var s Stop
		if err := rows.Scan(
			&s.Feed, &s.StopID, &s.StopCode, &s.StopName, &s.Lat, &s.Lon,
			&s.ZoneID, &s.StreetName, &s.TownName, &s.PlatformCode, &s.IsOnDemand,
		); err != nil {
			rows.Close()
			return 0, err
		}
		allStops = append(allStops, s)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	groups := BuildStopGroups(allStops)

	if _, err := tx.Exec(ctx, `DELETE FROM stop_groups`); err != nil {
		return 0, err
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"stop_groups"},
		[]string{"group_id", "name", "search_text", "lat", "lon", "operators", "stop_count"},
		pgx.CopyFromSlice(len(groups), func(i int) ([]any, error) {
			g := groups[i]
			return []any{g.GroupID, g.Name, g.SearchText, g.Lat, g.Lon, g.Operators, g.StopCount}, nil
		}),
	); err != nil {
		return 0, err
	}

	feeds := make([]string, len(allStops))
	stopIDs := make([]string, len(allStops))
	groupIDs := make([]string, len(allStops))
	for i, s := range allStops {
		feeds[i] = s.Feed
		stopIDs[i] = s.StopID
		groupIDs[i] = s.GroupID
	}

	if _, err := tx.Exec(ctx, `
		UPDATE stops AS s
		SET group_id = v.gid
		FROM (
			SELECT UNNEST($1::text[]) AS feed,
			       UNNEST($2::text[]) AS stop_id,
			       UNNEST($3::text[]) AS gid
		) AS v
		WHERE s.feed = v.feed AND s.stop_id = v.stop_id
	`, feeds, stopIDs, groupIDs); err != nil {
		return 0, err
	}

	return len(groups), nil
}

// LoadTripMeta reads lightweight trip + route metadata across all feeds for GTFS-RT enrichment.
func (im *Importer) LoadTripMeta(ctx context.Context) (map[string]TripMeta, error) {
	rows, err := im.pool.Query(ctx, `
		SELECT t.feed, t.trip_id, t.route_id, COALESCE(r.route_short_name, t.route_id),
		       COALESCE(r.route_color, ''), COALESCE(r.route_text_color, ''),
		       COALESCE(t.trip_headsign, ''), t.direction_id, COALESCE(t.brigade, '')
		FROM trips t
		LEFT JOIN routes r ON r.feed = t.feed AND r.route_id = t.route_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]TripMeta, 16384)
	for rows.Next() {
		var m TripMeta
		if err := rows.Scan(
			&m.Feed, &m.TripID, &m.RouteID, &m.RouteShortName,
			&m.RouteColor, &m.RouteTextColor, &m.Headsign, &m.DirectionID, &m.Brigade,
		); err != nil {
			return nil, err
		}
		out[m.Feed+":"+m.TripID] = m
	}
	return out, rows.Err()
}

func (im *Importer) recordError(ctx context.Context, feed, sourceURL string, cause error) {
	msg := cause.Error()
	if len(msg) > 500 {
		msg = msg[:500]
	}
	_, _ = im.pool.Exec(ctx, `
		INSERT INTO feed_state (feed, source_url, last_checked_at, last_error)
		VALUES ($1, $2, NOW(), $3)
		ON CONFLICT (feed) DO UPDATE
		SET last_checked_at = NOW(), last_error = EXCLUDED.last_error
	`, feed, sourceURL, msg)
}

func dedupeFeedData(d *FeedData) {
	d.Agencies = dedupeSlice(d.Agencies, func(a Agency) string { return a.AgencyID })
	d.Stops = dedupeSlice(d.Stops, func(s Stop) string { return s.StopID })
	d.Routes = dedupeSlice(d.Routes, func(r Route) string { return r.RouteID })
	d.Calendar = dedupeSlice(d.Calendar, func(c CalendarDate) string { return c.ServiceID + "|" + c.ServiceDate.Format("20060102") })
	d.Shapes = dedupeSlice(d.Shapes, func(s Shape) string { return s.ShapeID })
	d.Trips = dedupeSlice(d.Trips, func(t Trip) string { return t.TripID })
	d.StopTimes = dedupeSlice(d.StopTimes, func(st StopTime) string { return fmt.Sprintf("%s|%d", st.TripID, st.StopSequence) })
	d.FareProducts = dedupeSlice(d.FareProducts, func(fp FareProduct) string { return fp.FareProductID })
}

func dedupeSlice[T any](items []T, keyFn func(T) string) []T {
	if len(items) < 2 {
		return items
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]T, 0, len(items))
	for _, item := range items {
		k := keyFn(item)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, item)
	}
	return out
}
