package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A cache that cannot be reached costs one bounded wait, then nothing until the
// hold-off ends; the API reads PostgreSQL meanwhile.
func TestAnUnreachableCacheFailsFastAndThenStaysOutOfTheWay(t *testing.T) {
	c := NewRecentCache("127.0.0.1:1", "", 0) // nothing listens here

	started := time.Now()
	if _, err := c.Recent(context.Background(), time.Time{}, time.Now(), 10); err == nil {
		t.Fatal("expected an error")
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("the first failure took %s", took)
	}

	started = time.Now()
	_, err := c.Recent(context.Background(), time.Time{}, time.Now(), 10)
	if !errors.Is(err, ErrCacheDown) || time.Since(started) > 50*time.Millisecond {
		t.Fatalf("a held-off cache should answer at once: %v after %s", err, time.Since(started))
	}

	// After the hold-off it is tried again.
	c.downUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if _, err := c.Recent(context.Background(), time.Time{}, time.Now(), 10); errors.Is(err, ErrCacheDown) {
		t.Fatal("the cache was never tried again")
	}
}
