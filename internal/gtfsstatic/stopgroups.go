package gtfsstatic

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
)

// StopGroup represents a unified stop hub combining nearby ZTM and PKS platforms.
type StopGroup struct {
	GroupID    string
	Name       string
	SearchText string
	Lat        float64
	Lon        float64
	Operators  []string
	StopCount  int
}

const (
	sameNameClusterRadiusMeters   = 400.0
	crossFeedProximityMergeMeters = 25.0
)

var (
	reRzeszowPrefix = regexp.MustCompile(`(?i)^rzesz[óo]w,\s*`)
	reOnDemand      = regexp.MustCompile(`(?i)\s+n[żz]\s*$`)
	rePlatformNum   = regexp.MustCompile(`^(.+?)\s+(\d{1,2}[A-Za-z]?)$`)
	rePetlaSuffix   = regexp.MustCompile(`(?i)\s+p[ęe]tla$`)
	reMultiSpace    = regexp.MustCompile(`\s+`)
)

// NormalizeStopName extracts display base name, folded search base name, platform code, and on-demand flag.
func NormalizeStopName(rawName string) (displayBase string, normBase string, platformCode string, isOnDemand bool) {
	s := strings.TrimSpace(rawName)
	s = reRzeszowPrefix.ReplaceAllString(s, "")
	if reOnDemand.MatchString(s) {
		isOnDemand = true
		s = reOnDemand.ReplaceAllString(s, "")
	}
	s = reMultiSpace.ReplaceAllString(strings.TrimSpace(s), " ")

	if m := rePlatformNum.FindStringSubmatch(s); len(m) == 3 {
		displayBase = strings.TrimSpace(m[1])
		platformCode = strings.ToUpper(m[2])
	} else {
		displayBase = s
	}

	norm := rePetlaSuffix.ReplaceAllString(displayBase, "")
	norm = FoldPolish(strings.TrimSpace(norm))
	norm = reMultiSpace.ReplaceAllString(norm, " ")
	if norm == "" {
		norm = FoldPolish(displayBase)
	}
	return displayBase, norm, platformCode, isOnDemand
}

// FoldPolish converts Polish diacritics and case into plain lowercase ASCII for search and matching.
func FoldPolish(s string) string {
	s = strings.ToLower(s)
	replacer := strings.NewReplacer(
		"ą", "a",
		"ć", "c",
		"ę", "e",
		"ł", "l",
		"ń", "n",
		"ó", "o",
		"ś", "s",
		"ź", "z",
		"ż", "z",
	)
	return replacer.Replace(s)
}

func slugify(s string) string {
	folded := FoldPolish(s)
	var b strings.Builder
	b.Grow(len(folded))
	lastDash := false
	for i := 0; i < len(folded); i++ {
		c := folded[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b.WriteByte(c)
			lastDash = false
		} else if !lastDash && b.Len() > 0 {
			b.WriteByte('-')
			lastDash = true
		}
	}
	res := strings.Trim(b.String(), "-")
	if res == "" {
		return "stop"
	}
	return res
}

type cluster struct {
	normBase string
	stops    []*Stop
	lat      float64
	lon      float64
	hasZTM   bool
	hasPKS   bool
}

func (c *cluster) add(s *Stop) {
	n := float64(len(c.stops))
	c.lat = (c.lat*n + s.Lat) / (n + 1)
	c.lon = (c.lon*n + s.Lon) / (n + 1)
	c.stops = append(c.stops, s)
	if s.Feed == "ztm" {
		c.hasZTM = true
	} else if s.Feed == "pks" {
		c.hasPKS = true
	}
}

func (c *cluster) mergeFrom(other *cluster) {
	for _, s := range other.stops {
		c.add(s)
	}
}

// BuildStopGroups clusters ZTM and PKS stops into unified logical stop groups and sets each Stop.GroupID.
func BuildStopGroups(stops []Stop) []StopGroup {
	if len(stops) == 0 {
		return nil
	}

	// Sort deterministically by (normBase, feed, stopID) so cluster assignment is reproducible across imports.
	indices := make([]int, len(stops))
	normBases := make([]string, len(stops))
	for i := range stops {
		indices[i] = i
		_, nb, _, _ := NormalizeStopName(stops[i].StopName)
		normBases[i] = nb
	}
	sort.Slice(indices, func(a, b int) bool {
		ia, ib := indices[a], indices[b]
		if normBases[ia] != normBases[ib] {
			return normBases[ia] < normBases[ib]
		}
		if stops[ia].Feed != stops[ib].Feed {
			return stops[ia].Feed < stops[ib].Feed
		}
		return stops[ia].StopID < stops[ib].StopID
	})

	// Stage 1: Group stops sharing the same normalized base name within 400m.
	byNorm := make(map[string][]*cluster)
	var clusters []*cluster
	for _, idx := range indices {
		s := &stops[idx]
		nb := normBases[idx]
		var target *cluster
		for _, c := range byNorm[nb] {
			if HaversineMeters(c.lat, c.lon, s.Lat, s.Lon) <= sameNameClusterRadiusMeters {
				target = c
				break
			}
		}
		if target == nil {
			target = &cluster{normBase: nb}
			byNorm[nb] = append(byNorm[nb], target)
			clusters = append(clusters, target)
		}
		target.add(s)
	}

	// Stage 2: Merge cross-feed clusters that sit at the exact same bay (<= 25m) despite slightly different naming.
	merged := make([]bool, len(clusters))
	for i := 0; i < len(clusters); i++ {
		if merged[i] || !clusters[i].hasZTM {
			continue
		}
		for j := 0; j < len(clusters); j++ {
			if i == j || merged[j] || clusters[j].hasZTM {
				continue
			}
			if clusters[j].hasPKS && clusterMinDistanceMeters(clusters[i], clusters[j]) <= crossFeedProximityMergeMeters {
				clusters[i].mergeFrom(clusters[j])
				merged[j] = true
			}
		}
	}

	active := make([]*cluster, 0, len(clusters))
	for i, c := range clusters {
		if !merged[i] {
			active = append(active, c)
		}
	}

	// Build display metadata & provisional slug per cluster.
	type candidate struct {
		c           *cluster
		displayName string
		slug        string
	}
	cands := make([]candidate, len(active))
	slugCounts := make(map[string]int, len(active))
	for i, c := range active {
		display := pickDisplayName(c.stops)
		sl := slugify(display)
		cands[i] = candidate{c: c, displayName: display, slug: sl}
		slugCounts[sl]++
	}

	sort.Slice(cands, func(i, j int) bool {
		if cands[i].slug != cands[j].slug {
			return cands[i].slug < cands[j].slug
		}
		if cands[i].c.lat != cands[j].c.lat {
			return cands[i].c.lat < cands[j].c.lat
		}
		return cands[i].c.lon < cands[j].c.lon
	})

	usedIDs := make(map[string]struct{}, len(cands))
	groups := make([]StopGroup, 0, len(cands))
	for _, cand := range cands {
		gid := cand.slug
		if slugCounts[cand.slug] > 1 {
			gid = fmt.Sprintf("%s-%d-%d", cand.slug, int(math.Round(cand.c.lat*1000)), int(math.Round(cand.c.lon*1000)))
		}
		if _, exists := usedIDs[gid]; exists {
			suffix := 2
			for {
				tryID := fmt.Sprintf("%s-%d", gid, suffix)
				if _, ok := usedIDs[tryID]; !ok {
					gid = tryID
					break
				}
				suffix++
			}
		}
		usedIDs[gid] = struct{}{}

		ops := make([]string, 0, 2)
		if cand.c.hasZTM {
			ops = append(ops, "ztm")
		}
		if cand.c.hasPKS {
			ops = append(ops, "pks")
		}

		for _, s := range cand.c.stops {
			s.GroupID = gid
		}

		groups = append(groups, StopGroup{
			GroupID:    gid,
			Name:       cand.displayName,
			SearchText: buildSearchText(cand.displayName, cand.c.stops),
			Lat:        cand.c.lat,
			Lon:        cand.c.lon,
			Operators:  ops,
			StopCount:  len(cand.c.stops),
		})
	}

	return groups
}

func clusterMinDistanceMeters(a, b *cluster) float64 {
	best := math.MaxFloat64
	for _, sa := range a.stops {
		for _, sb := range b.stops {
			if d := HaversineMeters(sa.Lat, sa.Lon, sb.Lat, sb.Lon); d < best {
				best = d
			}
		}
	}
	return best
}

func pickDisplayName(stops []*Stop) string {
	// Prefer ZTM stop naming when available (cleaner without "Rzeszów," prefix), then shortest clean displayBase.
	best := ""
	bestIsZTM := false
	for _, s := range stops {
		disp, _, _, _ := NormalizeStopName(s.StopName)
		if disp == "" {
			disp = s.StopName
		}
		isZTM := s.Feed == "ztm"
		if best == "" || (isZTM && !bestIsZTM) || (isZTM == bestIsZTM && len(disp) < len(best)) {
			best = disp
			bestIsZTM = isZTM
		}
	}
	return best
}

func buildSearchText(displayName string, stops []*Stop) string {
	seen := make(map[string]struct{})
	var parts []string
	add := func(raw string) {
		folded := strings.TrimSpace(FoldPolish(raw))
		if folded == "" {
			return
		}
		if _, ok := seen[folded]; !ok {
			seen[folded] = struct{}{}
			parts = append(parts, folded)
		}
	}
	add(displayName)
	for _, s := range stops {
		add(s.StopName)
		add(s.StopCode)
		add(s.StreetName)
		add(s.TownName)
	}
	return strings.Join(parts, " ")
}

// HaversineMeters calculates approximate surface distance in meters between two WGS84 coordinates.
func HaversineMeters(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusM = 6371000.0
	rad := math.Pi / 180.0
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusM * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}
