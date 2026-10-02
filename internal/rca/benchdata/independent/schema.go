package independent

import (
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

var epoch = time.Date(2026, 5, 14, 9, 30, 0, 0, time.UTC)

func secs(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// Epoch is t=0 of every incident.
func Epoch() time.Time { return epoch }

// Expected results.
const (
	RootCause   = "ROOT_CAUSE"    // one recorded cause, which should lead
	Ambiguous   = "AMBIGUOUS"     // the recorded evidence cannot separate several causes
	NoRootCause = "NO_ROOT_CAUSE" // no recorded event explains the failure
)

// Categories.
const (
	CatKnown      = "A-known-observable"
	CatDifficult  = "B-previously-unobserved"
	CatRival      = "C-rival-causes"
	CatRootEffect = "D-root-vs-effect"
	CatNoRoot     = "E-no-root-cause"
	CatTiming     = "T-timing"
	CatLate       = "L-delayed-observation"
)

// Root names one actual cause: the event kind (its Type), the entity and its kind.
type Root struct {
	Kind       string // event type, e.g. "deploy"
	EntityKind string
	Namespace  string
	Entity     string
}

// Incident is one failure with its full ground truth.
type Incident struct {
	ID       string
	Category string
	Title    string

	// Ground truth.
	ActualRoots []Root // the real cause(s); empty when the cause is not recorded anywhere
	Plausible   []Root // every cause the recorded evidence cannot rule out (includes ActualRoots when Ambiguous)
	Observable  bool   // an event for the actual cause exists in the stream
	Ambiguous   bool   // several plausible causes intentionally exist
	AllRequired bool   // independent simultaneous causes: every one should be found
	Expected    string // RootCause, Ambiguous or NoRootCause
	// InScope is false when the incident deliberately sits outside what the
	// analyzer is designed to see (a cause older than the lookback window, an
	// event whose true time was lost). Such incidents are reported separately.
	InScope bool
	Notes   string

	Events  []event.Event
	Symptom event.Event
	Edges   []graph.Edge
}
