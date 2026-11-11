package gtfsstatic

import (
	"archive/zip"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FeedData holds parsed GTFS static entities for a single operator feed ("ztm" or "pks").
type FeedData struct {
	Feed         string
	Agencies     []Agency
	Stops        []Stop
	Routes       []Route
	Calendar     []CalendarDate
	Shapes       []Shape
	Trips        []Trip
	StopTimes    []StopTime
	FareProducts []FareProduct
	Info         FeedInfo
}

type FeedInfo struct {
	Version   string
	StartDate *time.Time
	EndDate   *time.Time
}

type Agency struct {
	AgencyID string
	Name     string
	URL      string
	Timezone string
	Phone    string
}

type Stop struct {
	Feed         string
	StopID       string
	StopCode     string
	StopName     string
	Lat          float64
	Lon          float64
	ZoneID       string
	StreetName   string
	TownName     string
	GroupID      string
	PlatformCode string
	IsOnDemand   bool
}

type Route struct {
	RouteID   string
	AgencyID  string
	ShortName string
	LongName  string
	RouteType int
	Color     string
	TextColor string
}

type CalendarDate struct {
	ServiceID     string
	ServiceDate   time.Time
	ExceptionType int
}

type Shape struct {
	ShapeID    string
	Points     []byte
	PointCount int
}

type Trip struct {
	TripID               string
	RouteID              string
	ServiceID            string
	Headsign             string
	DirectionID          int
	BlockID              string
	ShapeID              string
	WheelchairAccessible int
	Brigade              string
}

type StopTime struct {
	TripID            string
	StopSequence      int
	StopID            string
	ArrivalSecs       int
	DepartureSecs     int
	PickupType        int
	DropOffType       int
	ShapeDistTraveled *float64
}

type FareProduct struct {
	FareProductID   string
	FareProductName string
	FareMediaID     string
	RiderCategoryID string
	Amount          string
	Currency        string
}

// ParseZipFile parses a GTFS static archive from disk for the given feed name ("ztm" or "pks").
func ParseZipFile(feed, path string) (*FeedData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open zip %s: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat zip %s: %w", path, err)
	}
	zr, err := zip.NewReader(f, info.Size())
	if err != nil {
		return nil, fmt.Errorf("read zip %s: %w", path, err)
	}

	return ParseZipReader(feed, zr)
}

// ParseZipReader parses a GTFS static zip.Reader.
func ParseZipReader(feed string, zr *zip.Reader) (*FeedData, error) {
	entries := make(map[string]*zip.File, len(zr.File))
	for _, zf := range zr.File {
		entries[strings.ToLower(zf.Name)] = zf
	}

	data := &FeedData{Feed: feed}

	if err := parseOptionalCSV(entries["feed_info.txt"], func(r csvRow) error {
		if data.Info.Version == "" {
			data.Info.Version = r.get("feed_version")
			if sd, ok := parseOptionalDate(r.get("feed_start_date")); ok {
				data.Info.StartDate = &sd
			}
			if ed, ok := parseOptionalDate(r.get("feed_end_date")); ok {
				data.Info.EndDate = &ed
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("feed_info.txt: %w", err)
	}

	if err := parseRequiredCSV(entries["agency.txt"], "agency.txt", func(r csvRow) error {
		id := r.get("agency_id")
		if id == "" {
			id = "default"
		}
		data.Agencies = append(data.Agencies, Agency{
			AgencyID: id,
			Name:     r.get("agency_name"),
			URL:      r.get("agency_url"),
			Timezone: r.get("agency_timezone"),
			Phone:    r.get("agency_phone"),
		})
		return nil
	}); err != nil {
		return nil, err
	}

	if err := parseRequiredCSV(entries["stops.txt"], "stops.txt", func(r csvRow) error {
		lat, err := strconv.ParseFloat(r.get("stop_lat"), 64)
		if err != nil {
			return fmt.Errorf("invalid stop_lat %q: %w", r.get("stop_lat"), err)
		}
		lon, err := strconv.ParseFloat(r.get("stop_lon"), 64)
		if err != nil {
			return fmt.Errorf("invalid stop_lon %q: %w", r.get("stop_lon"), err)
		}
		rawName := strings.TrimSpace(r.get("stop_name"))
		_, _, platform, onDemand := NormalizeStopName(rawName)
		data.Stops = append(data.Stops, Stop{
			Feed:         feed,
			StopID:       r.get("stop_id"),
			StopCode:     r.get("stop_code"),
			StopName:     rawName,
			Lat:          lat,
			Lon:          lon,
			ZoneID:       r.get("zone_id"),
			StreetName:   r.get("street_name"),
			TownName:     r.get("town_name"),
			PlatformCode: platform,
			IsOnDemand:   onDemand,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	if err := parseRequiredCSV(entries["routes.txt"], "routes.txt", func(r csvRow) error {
		routeType := parseIntOr(r.get("route_type"), 3)
		shortName := r.get("route_short_name")
		if shortName == "" {
			shortName = r.get("route_id")
		}
		data.Routes = append(data.Routes, Route{
			RouteID:   r.get("route_id"),
			AgencyID:  r.get("agency_id"),
			ShortName: shortName,
			LongName:  r.get("route_long_name"),
			RouteType: routeType,
			Color:     normalizeHexColor(r.get("route_color")),
			TextColor: normalizeHexColor(r.get("route_text_color")),
		})
		return nil
	}); err != nil {
		return nil, err
	}

	calMap, err := parseServiceCalendar(entries["calendar.txt"], entries["calendar_dates.txt"])
	if err != nil {
		return nil, err
	}
	data.Calendar = calMap

	if err := parseRequiredCSV(entries["trips.txt"], "trips.txt", func(r csvRow) error {
		data.Trips = append(data.Trips, Trip{
			TripID:               r.get("trip_id"),
			RouteID:              r.get("route_id"),
			ServiceID:            r.get("service_id"),
			Headsign:             r.get("trip_headsign"),
			DirectionID:          parseIntOr(r.get("direction_id"), 0),
			BlockID:              r.get("block_id"),
			ShapeID:              r.get("shape_id"),
			WheelchairAccessible: parseIntOr(r.get("wheelchair_accessible"), 0),
			Brigade:              r.get("brigade"),
		})
		return nil
	}); err != nil {
		return nil, err
	}

	if err := parseRequiredCSV(entries["stop_times.txt"], "stop_times.txt", func(r csvRow) error {
		seq, err := strconv.Atoi(r.get("stop_sequence"))
		if err != nil {
			return fmt.Errorf("invalid stop_sequence %q: %w", r.get("stop_sequence"), err)
		}
		arrRaw := r.get("arrival_time")
		depRaw := r.get("departure_time")
		if arrRaw == "" {
			arrRaw = depRaw
		}
		if depRaw == "" {
			depRaw = arrRaw
		}
		arrSecs, err := ParseGTFSTime(arrRaw)
		if err != nil {
			return fmt.Errorf("invalid arrival_time %q: %w", arrRaw, err)
		}
		depSecs, err := ParseGTFSTime(depRaw)
		if err != nil {
			return fmt.Errorf("invalid departure_time %q: %w", depRaw, err)
		}
		var distPtr *float64
		if distStr := r.get("shape_dist_traveled"); distStr != "" {
			if d, err := strconv.ParseFloat(distStr, 64); err == nil {
				distPtr = &d
			}
		}
		data.StopTimes = append(data.StopTimes, StopTime{
			TripID:            r.get("trip_id"),
			StopSequence:      seq,
			StopID:            r.get("stop_id"),
			ArrivalSecs:       arrSecs,
			DepartureSecs:     depSecs,
			PickupType:        parseIntOr(r.get("pickup_type"), 0),
			DropOffType:       parseIntOr(r.get("drop_off_type"), 0),
			ShapeDistTraveled: distPtr,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	shapes, err := parseShapes(entries["shapes.txt"])
	if err != nil {
		return nil, err
	}
	data.Shapes = shapes

	if err := parseOptionalCSV(entries["fare_products.txt"], func(r csvRow) error {
		id := r.get("fare_product_id")
		if id == "" {
			return nil
		}
		data.FareProducts = append(data.FareProducts, FareProduct{
			FareProductID:   id,
			FareProductName: r.get("fare_product_name"),
			FareMediaID:     r.get("fare_media_id"),
			RiderCategoryID: r.get("rider_category_id"),
			Amount:          r.get("amount"),
			Currency:        r.get("currency"),
		})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("fare_products.txt: %w", err)
	}

	return data, nil
}

func parseServiceCalendar(calFile, calDatesFile *zip.File) ([]CalendarDate, error) {
	type serviceDay struct {
		serviceID string
		dateStr   string
	}
	active := make(map[serviceDay]int)

	// Expand optional calendar.txt if present.
	if calFile != nil {
		weekdayFields := [7]string{"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"}
		err := parseOptionalCSV(calFile, func(r csvRow) error {
			sid := r.get("service_id")
			start, ok1 := parseOptionalDate(r.get("start_date"))
			end, ok2 := parseOptionalDate(r.get("end_date"))
			if sid == "" || !ok1 || !ok2 {
				return nil
			}
			var enabled [7]bool
			for i, col := range weekdayFields {
				enabled[i] = r.get(col) == "1"
			}
			for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
				if enabled[int(d.Weekday())] {
					active[serviceDay{sid, d.Format("20060102")}] = 1
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("calendar.txt: %w", err)
		}
	}

	if calDatesFile == nil && calFile == nil {
		return nil, errors.New("missing both calendar.txt and calendar_dates.txt")
	}

	if calDatesFile != nil {
		err := parseOptionalCSV(calDatesFile, func(r csvRow) error {
			sid := r.get("service_id")
			d, ok := parseOptionalDate(r.get("date"))
			if sid == "" || !ok {
				return nil
			}
			exc := parseIntOr(r.get("exception_type"), 1)
			key := serviceDay{sid, d.Format("20060102")}
			if exc == 1 {
				active[key] = 1
			} else if exc == 2 {
				delete(active, key)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("calendar_dates.txt: %w", err)
		}
	}

	out := make([]CalendarDate, 0, len(active))
	for k, exc := range active {
		t, _ := time.Parse("20060102", k.dateStr)
		out = append(out, CalendarDate{
			ServiceID:     k.serviceID,
			ServiceDate:   t,
			ExceptionType: exc,
		})
	}
	return out, nil
}

func parseShapes(zf *zip.File) ([]Shape, error) {
	if zf == nil {
		return nil, nil
	}
	type rawPt struct {
		seq int
		lat float64
		lon float64
	}
	grouped := make(map[string][]rawPt)
	err := parseOptionalCSV(zf, func(r csvRow) error {
		id := r.get("shape_id")
		if id == "" {
			return nil
		}
		lat, err := strconv.ParseFloat(r.get("shape_pt_lat"), 64)
		if err != nil {
			return fmt.Errorf("invalid shape_pt_lat: %w", err)
		}
		lon, err := strconv.ParseFloat(r.get("shape_pt_lon"), 64)
		if err != nil {
			return fmt.Errorf("invalid shape_pt_lon: %w", err)
		}
		seq, err := strconv.Atoi(r.get("shape_pt_sequence"))
		if err != nil {
			return fmt.Errorf("invalid shape_pt_sequence: %w", err)
		}
		grouped[id] = append(grouped[id], rawPt{seq: seq, lat: lat, lon: lon})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("shapes.txt: %w", err)
	}

	out := make([]Shape, 0, len(grouped))
	for id, pts := range grouped {
		sort.Slice(pts, func(i, j int) bool { return pts[i].seq < pts[j].seq })
		coords := make([][2]float64, len(pts))
		for i, p := range pts {
			coords[i] = [2]float64{p.lat, p.lon}
		}
		out = append(out, Shape{
			ShapeID:    id,
			Points:     EncodeShapePoints(coords),
			PointCount: len(coords),
		})
	}
	return out, nil
}

// EncodeShapePoints packs [lat, lon] pairs into a compact little-endian float64 byte slice.
func EncodeShapePoints(pts [][2]float64) []byte {
	buf := make([]byte, len(pts)*16)
	for i, pt := range pts {
		binary.LittleEndian.PutUint64(buf[i*16:], math.Float64bits(pt[0]))
		binary.LittleEndian.PutUint64(buf[i*16+8:], math.Float64bits(pt[1]))
	}
	return buf
}

// DecodeShapePoints unpacks little-endian float64 [lat, lon] pairs stored by EncodeShapePoints.
func DecodeShapePoints(raw []byte) ([][2]float64, error) {
	if len(raw)%16 != 0 {
		return nil, fmt.Errorf("corrupt shape payload length: %d", len(raw))
	}
	n := len(raw) / 16
	pts := make([][2]float64, n)
	for i := 0; i < n; i++ {
		lat := math.Float64frombits(binary.LittleEndian.Uint64(raw[i*16:]))
		lon := math.Float64frombits(binary.LittleEndian.Uint64(raw[i*16+8:]))
		pts[i] = [2]float64{lat, lon}
	}
	return pts, nil
}

// ParseGTFSTime parses "HH:MM:SS" (including hours >= 24) into seconds since service midnight.
func ParseGTFSTime(s string) (int, error) {
	s = strings.TrimSpace(s)
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("expected HH:MM:SS, got %q", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 {
		return 0, fmt.Errorf("invalid hour in %q", s)
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m >= 60 {
		return 0, fmt.Errorf("invalid minute in %q", s)
	}
	sec, err := strconv.Atoi(parts[2])
	if err != nil || sec < 0 || sec >= 60 {
		return 0, fmt.Errorf("invalid second in %q", s)
	}
	return h*3600 + m*60 + sec, nil
}

func parseOptionalDate(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if len(s) != 8 {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102", s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func parseIntOr(s string, fallback int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return fallback
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return fallback
	}
	return v
}

func normalizeHexColor(s string) string {
	s = strings.TrimSpace(strings.TrimPrefix(s, "#"))
	return strings.ToUpper(s)
}

type csvRow struct {
	cols   map[string]int
	values []string
}

func (r csvRow) get(name string) string {
	idx, ok := r.cols[name]
	if !ok || idx < 0 || idx >= len(r.values) {
		return ""
	}
	return strings.TrimSpace(r.values[idx])
}

func parseRequiredCSV(zf *zip.File, name string, fn func(csvRow) error) error {
	if zf == nil {
		return fmt.Errorf("required GTFS file %s not found in zip", name)
	}
	if err := parseOptionalCSV(zf, fn); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func parseOptionalCSV(zf *zip.File, fn func(csvRow) error) error {
	if zf == nil {
		return nil
	}
	rc, err := zf.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	cr := csv.NewReader(rc)
	cr.ReuseRecord = true
	cr.LazyQuotes = true

	header, err := cr.Read()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	cols := make(map[string]int, len(header))
	for i, h := range header {
		h = strings.TrimPrefix(h, "\uFEFF")
		cols[strings.TrimSpace(h)] = i
	}

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := fn(csvRow{cols: cols, values: rec}); err != nil {
			return err
		}
	}
	return nil
}
