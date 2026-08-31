package collect

import (
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/oklog/ulid/v2"
)

type BaseCollector struct {
	Out chan<- event.Event
}

func (c *BaseCollector) Emit(e event.Event) {
	e.IngestedAt = time.Now().UTC()
	if e.OccurredAt.IsZero() {
		e.OccurredAt = e.IngestedAt
	}
	e.ID = ulid.Make().String()
	c.Out <- e
}
