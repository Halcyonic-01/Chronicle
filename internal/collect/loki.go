package collect

import (
	"context"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

type LokiCollector struct {
	BaseCollector
}

func NewLokiCollector(out chan<- event.Event) *LokiCollector {
	return &LokiCollector{
		BaseCollector: BaseCollector{Out: out},
	}
}

func (l *LokiCollector) Run(ctx context.Context) error {
	// Stub for Loki logs
	<-ctx.Done()
	return ctx.Err()
}
