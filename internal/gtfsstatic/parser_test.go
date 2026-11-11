package gtfsstatic

import (
	"archive/zip"
	"bytes"
	"testing"
)

func TestParseGTFSTime(t *testing.T) {
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"00:00:00", 0, false},
		{"09:07:05", 9*3600 + 7*60 + 5, false},
		{"24:00:00", 24 * 3600, false},
		{"25:15:30", 25*3600 + 15*60 + 30, false},
		{"7:05:00", 7*3600 + 5*60, false},
		{"12:60:00", 0, true},
		{"invalid", 0, true},
	}

	for _, tc := range tests {
		got, err := ParseGTFSTime(tc.in)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ParseGTFSTime(%q) err=%v, wantErr=%v", tc.in, err, tc.wantErr)
		}
		if !tc.wantErr && got != tc.want {
			t.Fatalf("ParseGTFSTime(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestNormalizeStopName(t *testing.T) {
	tests := []struct {
		raw      string
		disp     string
		norm     string
		plat     string
		onDemand bool
	}{
		{"Lubelska szpital 01", "Lubelska szpital", "lubelska szpital", "01", false},
		{"Lubelska kościół 05 nż", "Lubelska kościół", "lubelska kosciol", "05", true},
		{"Rzeszów, Marszałkowska 03", "Marszałkowska", "marszalkowska", "03", false},
		{"Lwowska szpital pętla 02", "Lwowska szpital pętla", "lwowska szpital", "02", false},
		{"Rzeszów D.A. 5", "Rzeszów D.A.", "rzeszow d.a.", "5", false},
		{"Dworzec Lokalny", "Dworzec Lokalny", "dworzec lokalny", "", false},
	}

	for _, tc := range tests {
		disp, norm, plat, od := NormalizeStopName(tc.raw)
		if disp != tc.disp || norm != tc.norm || plat != tc.plat || od != tc.onDemand {
			t.Fatalf("NormalizeStopName(%q) = (%q, %q, %q, %v), want (%q, %q, %q, %v)",
				tc.raw, disp, norm, plat, od, tc.disp, tc.norm, tc.plat, tc.onDemand)
		}
	}
}

func TestBuildStopGroupsMergesZTMAndPKSLubelskaSzpital(t *testing.T) {
	stops := []Stop{
		{Feed: "ztm", StopID: "65", StopCode: "2L", StopName: "Lubelska szpital 01", Lat: 50.049241, Lon: 22.000492, PlatformCode: "01"},
		{Feed: "ztm", StopID: "206", StopCode: "1L", StopName: "Lubelska szpital 02", Lat: 50.049751, Lon: 22.001316, PlatformCode: "02"},
		{Feed: "pks", StopID: "199", StopName: "Lubelska szpital 01", Lat: 50.049241, Lon: 22.000492, PlatformCode: "01"},
		{Feed: "pks", StopID: "200", StopName: "Lubelska szpital 02", Lat: 50.049751, Lon: 22.001316, PlatformCode: "02"},
		// Cross-feed close proximity (< 25m) with slightly different naming ("Yanko" vs "Yanko I")
		{Feed: "ztm", StopID: "679", StopName: "Głogów Młp. Yanko I 01 nż", Lat: 50.150000, Lon: 21.970000, PlatformCode: "01", IsOnDemand: true},
		{Feed: "pks", StopID: "631", StopName: "Głogów Młp. Yanko 02", Lat: 50.150120, Lon: 21.970100, PlatformCode: "02"},
		// Same base name 15 km apart must stay separate groups with unique group IDs
		{Feed: "pks", StopID: "901", StopName: "Szkoła 01", Lat: 50.1000, Lon: 22.1000, PlatformCode: "01"},
		{Feed: "pks", StopID: "902", StopName: "Szkoła 02", Lat: 50.2500, Lon: 22.2500, PlatformCode: "02"},
	}

	groups := BuildStopGroups(stops)
	if len(groups) != 4 {
		t.Fatalf("expected 4 groups, got %d: %+v", len(groups), groups)
	}

	var lubelska *StopGroup
	for i := range groups {
		if groups[i].GroupID == "lubelska-szpital" {
			lubelska = &groups[i]
			break
		}
	}
	if lubelska == nil {
		t.Fatalf("expected group_id 'lubelska-szpital', got %+v", groups)
	}
	if lubelska.StopCount != 4 || len(lubelska.Operators) != 2 {
		t.Fatalf("expected 4 stops and 2 operators in lubelska-szpital, got %+v", lubelska)
	}
	for i := 0; i < 4; i++ {
		if stops[i].GroupID != "lubelska-szpital" {
			t.Fatalf("stop %s:%s got GroupID=%q, want lubelska-szpital", stops[i].Feed, stops[i].StopID, stops[i].GroupID)
		}
	}
	if stops[4].GroupID != stops[5].GroupID {
		t.Fatalf("expected Yanko stops within 25m to merge, got %q vs %q", stops[4].GroupID, stops[5].GroupID)
	}
	if stops[6].GroupID == stops[7].GroupID {
		t.Fatalf("expected distant Szkoła stops to remain distinct, both got %q", stops[6].GroupID)
	}
}

func TestEncodeDecodeShapePoints(t *testing.T) {
	in := [][2]float64{{50.041736, 22.008580}, {50.049241, 22.000492}}
	raw := EncodeShapePoints(in)
	out, err := DecodeShapePoints(raw)
	if err != nil {
		t.Fatalf("DecodeShapePoints failed: %v", err)
	}
	if len(out) != len(in) || out[0] != in[0] || out[1] != in[1] {
		t.Fatalf("round-trip mismatch: got %v, want %v", out, in)
	}
}

func TestParseZipReader(t *testing.T) {
	files := map[string]string{
		"agency.txt":         "\uFEFFagency_id,agency_name,agency_url,agency_timezone,agency_phone\n1,RTM,https://rtm.erzeszow.pl,Europe/Warsaw,123\n",
		"stops.txt":          "stop_id,stop_code,stop_name,stop_lat,stop_lon\n65,2L,Lubelska szpital 01,50.049241,22.000492\n",
		"routes.txt":         "route_id,agency_id,route_short_name,route_long_name,route_type,route_color,route_text_color\n43,1,43,Test,3,D32A00,FFFFFF\n",
		"calendar_dates.txt": "service_id,date,exception_type\nPW,20261111,1\n",
		"trips.txt":          "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,brigade\n43,PW,232525,Szpital,1,2148,43/2\n",
		"stop_times.txt":     "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type\n232525,25:07:00,25:07:00,65,1,0,0\n",
		"shapes.txt":         "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n2148,50.04,22.00,2\n2148,50.03,21.99,1\n",
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	fd, err := ParseZipReader("ztm", zr)
	if err != nil {
		t.Fatalf("ParseZipReader failed: %v", err)
	}
	if len(fd.Agencies) != 1 || fd.Agencies[0].AgencyID != "1" {
		t.Fatalf("unexpected agencies: %+v", fd.Agencies)
	}
	if len(fd.StopTimes) != 1 || fd.StopTimes[0].DepartureSecs != 25*3600+7*60 {
		t.Fatalf("unexpected stop_times: %+v", fd.StopTimes)
	}
	if len(fd.Trips) != 1 || fd.Trips[0].Brigade != "43/2" {
		t.Fatalf("unexpected trips: %+v", fd.Trips)
	}
	if len(fd.Shapes) != 1 || fd.Shapes[0].PointCount != 2 {
		t.Fatalf("unexpected shapes: %+v", fd.Shapes)
	}
	pts, err := DecodeShapePoints(fd.Shapes[0].Points)
	if err != nil || pts[0][0] != 50.03 {
		t.Fatalf("expected shapes sorted by sequence (50.03 first), got %v, err=%v", pts, err)
	}
}
