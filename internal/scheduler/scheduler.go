package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/maciejdbrznsk/pkstt-backend/internal/config"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsrt"
	"github.com/maciejdbrznsk/pkstt-backend/internal/gtfsstatic"
)

// Runner coordinates periodic static GTFS imports and high-frequency GTFS-RT polling.
type Runner struct {
	cfg      config.Config
	importer *gtfsstatic.Importer
	poller   *gtfsrt.Poller
	rtStore  *gtfsrt.Store
	log      *slog.Logger
}

// NewRunner creates a background scheduler runner.
func NewRunner(cfg config.Config, importer *gtfsstatic.Importer, poller *gtfsrt.Poller, rtStore *gtfsrt.Store, log *slog.Logger) *Runner {
	return &Runner{
		cfg:      cfg,
		importer: importer,
		poller:   poller,
		rtStore:  rtStore,
		log:      log,
	}
}

// Start runs initial data bootstrap and starts static + real-time background loops until ctx is canceled.
func (r *Runner) Start(ctx context.Context) {
	// Load any existing static trip metadata immediately for instant RT enrichment on restarts.
	if meta, err := r.importer.LoadTripMeta(ctx); err == nil && len(meta) > 0 {
		r.rtStore.SetTripMeta(meta)
		r.log.Info("loaded cached trip metadata from db", "trips", len(meta))
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		r.runStaticLoop(ctx)
	}()

	go func() {
		defer wg.Done()
		r.runRTLoop(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
}

func (r *Runner) runStaticLoop(ctx context.Context) {
	r.syncAllStatic(ctx)

	ticker := time.NewTicker(r.cfg.StaticSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.syncAllStatic(ctx)
		}
	}
}

func (r *Runner) syncAllStatic(ctx context.Context) {
	feeds := []struct {
		name string
		url  string
	}{
		{name: "ztm", url: r.cfg.ZTMStaticURL},
		{name: "pks", url: r.cfg.PKSStaticURL},
	}

	anyUpdated := false
	for _, f := range feeds {
		if ctx.Err() != nil {
			return
		}
		updated, err := r.importer.SyncFeed(ctx, f.name, f.url)
		if err != nil {
			r.log.Error("static gtfs sync failed", "feed", f.name, "error", err)
			continue
		}
		if updated {
			anyUpdated = true
		}
	}

	if anyUpdated {
		meta, err := r.importer.LoadTripMeta(ctx)
		if err != nil {
			r.log.Error("reload trip metadata failed", "error", err)
			return
		}
		r.rtStore.SetTripMeta(meta)
		r.log.Info("refreshed in-memory trip metadata", "trips", len(meta))
	}
}

func (r *Runner) runRTLoop(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		r.pollFeedLoop(ctx, "ztm", r.cfg.ZTMRTURL)
	}()
	go func() {
		defer wg.Done()
		r.pollFeedLoop(ctx, "pks", r.cfg.PKSRTURL)
	}()

	wg.Wait()
}

func (r *Runner) pollFeedLoop(ctx context.Context, feed, url string) {
	consecutiveErrs := 0
	for {
		if err := r.poller.PollOnce(ctx, feed, url); err != nil {
			consecutiveErrs++
			backoff := minDuration(r.cfg.RTPollInterval*time.Duration(1<<min(consecutiveErrs, 3)), 60*time.Second)
			r.log.Warn("gtfs-rt poll failed", "feed", feed, "attempt", consecutiveErrs, "retry_in", backoff, "error", err)
			if !sleepContext(ctx, backoff) {
				return
			}
			continue
		}
		consecutiveErrs = 0
		if !sleepContext(ctx, r.cfg.RTPollInterval) {
			return
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
