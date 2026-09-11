package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

type RecentCache struct {
	client *redis.Client
	key    string
	limit  int64
}

func NewRecentCache(address, password string, db int) *RecentCache {
	return &RecentCache{client: redis.NewClient(&redis.Options{Addr: address, Password: password, DB: db}), key: "chronicle:events:recent", limit: 1000}
}

func (c *RecentCache) Add(ctx context.Context, e event.Event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	pipe := c.client.Pipeline()
	pipe.LPush(ctx, c.key, payload)
	pipe.LTrim(ctx, c.key, 0, c.limit-1)
	pipe.Expire(ctx, c.key, 24*time.Hour)
	_, err = pipe.Exec(ctx)
	return err
}

func (c *RecentCache) Close() error { return c.client.Close() }
