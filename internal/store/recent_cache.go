package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

// RecentCache keeps the newest events in Redis so the operations console can
// render its default "last events" view without paging the events table.
type RecentCache struct {
	client *redis.Client
	key    string
	limit  int64
	// downUntil is when the cache is tried again after a failure (unix nanos).
	downUntil atomic.Int64
}

// ErrCacheDown is returned without touching Redis while the cache is held off.
var ErrCacheDown = errors.New("recent event cache is holding off after a failure")

const (
	cacheTimeout = time.Second
	// cacheHoldOff is how long a failure keeps the cache out of the way, so an
	// outage costs one slow call, not one per request. The cache is optional:
	// everything it holds is in PostgreSQL.
	cacheHoldOff = 10 * time.Second
)

func NewRecentCache(address, password string, db int) *RecentCache {
	return &RecentCache{client: redis.NewClient(&redis.Options{Addr: address, Password: password, DB: db,
		DialTimeout: cacheTimeout, ReadTimeout: cacheTimeout, WriteTimeout: cacheTimeout, MaxRetries: 1}), key: "chronicle:events:recent", limit: 1000}
}

// guard runs a cache call under a short deadline and holds the cache off after a
// failure.
func (c *RecentCache) guard(ctx context.Context, call func(context.Context) error) error {
	if time.Now().UnixNano() < c.downUntil.Load() {
		return ErrCacheDown
	}
	ctx, cancel := context.WithTimeout(ctx, 2*cacheTimeout)
	defer cancel()
	err := call(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		c.downUntil.Store(time.Now().Add(cacheHoldOff).UnixNano())
	}
	return err
}

// Limit reports how many events the cache retains. A request for more than
// this cannot be served from the cache alone.
func (c *RecentCache) Limit() int { return int(c.limit) }

// Add appends a batch of events in one round trip. Events must be supplied in
// ascending time order so the newest ends up at the head of the list.
func (c *RecentCache) Add(ctx context.Context, events ...event.Event) error {
	if len(events) == 0 {
		return nil
	}
	payloads := make([]any, 0, len(events))
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		payloads = append(payloads, payload)
	}
	return c.guard(ctx, func(ctx context.Context) error {
		pipe := c.client.Pipeline()
		pipe.LPush(ctx, c.key, payloads...)
		pipe.LTrim(ctx, c.key, 0, c.limit-1)
		pipe.Expire(ctx, c.key, 24*time.Hour)
		_, err := pipe.Exec(ctx)
		return err
	})
}

// Recent returns up to limit cached events, newest first, restricted to the
// requested window. It returns fewer events than requested when the cache does
// not hold enough history; callers decide whether to fall back to PostgreSQL.
func (c *RecentCache) Recent(ctx context.Context, from, to time.Time, limit int) ([]event.Event, error) {
	if limit <= 0 || int64(limit) > c.limit {
		limit = int(c.limit)
	}
	var raw []string
	err := c.guard(ctx, func(ctx context.Context) (err error) {
		raw, err = c.client.LRange(ctx, c.key, 0, int64(limit)-1).Result()
		return err
	})
	if err != nil {
		return nil, err
	}
	events := make([]event.Event, 0, len(raw))
	for _, item := range raw {
		var e event.Event
		if err := json.Unmarshal([]byte(item), &e); err != nil {
			return nil, err
		}
		if e.IngestedAt.Before(from) || e.IngestedAt.After(to) {
			continue
		}
		events = append(events, e)
	}
	return events, nil
}

func (c *RecentCache) Close() error { return c.client.Close() }
