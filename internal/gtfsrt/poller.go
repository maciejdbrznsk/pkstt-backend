package gtfsrt

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

const maxRTBodyBytes = 10 << 20 // 10 MiB safety limit

// Poller fetches GTFS-RT protobuf feeds for ZTM and PKS conditionally and updates Store.
type Poller struct {
	store     *Store
	client    *http.Client
	userAgent string
	log       *slog.Logger
}

// NewPoller creates a conditional GTFS-RT poller.
func NewPoller(store *Store, userAgent string, log *slog.Logger) *Poller {
	return &Poller{
		store: store,
		client: &http.Client{
			Timeout: 8 * time.Second,
		},
		userAgent: userAgent,
		log:       log,
	}
}

// PollOnce fetches a single feed conditionally; returns nil on 200 or 304.
func (p *Poller) PollOnce(ctx context.Context, feed, url string) error {
	now := time.Now().UTC()
	prevETag, prevLastMod := p.store.GetConditionalHeaders(feed)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", p.userAgent)
	if prevETag != "" {
		req.Header.Set("If-None-Match", prevETag)
	}
	if prevLastMod != "" {
		req.Header.Set("If-Modified-Since", prevLastMod)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.store.MarkError(feed, now, err)
		return fmt.Errorf("fetch %s: %w", feed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		p.store.MarkChecked(feed, now)
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("unexpected status %s", resp.Status)
		p.store.MarkError(feed, now, err)
		return fmt.Errorf("fetch %s: %w", feed, err)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRTBodyBytes+1))
	if err != nil {
		p.store.MarkError(feed, now, err)
		return fmt.Errorf("read %s: %w", feed, err)
	}
	if len(body) > maxRTBodyBytes {
		err := fmt.Errorf("gtfs-rt body exceeds %d bytes", maxRTBodyBytes)
		p.store.MarkError(feed, now, err)
		return err
	}

	msg := &gtfs.FeedMessage{}
	if err := proto.Unmarshal(body, msg); err != nil {
		p.store.MarkError(feed, now, err)
		return fmt.Errorf("decode protobuf %s: %w", feed, err)
	}

	p.store.ApplyFeed(feed, msg, resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"), now)
	return nil
}
