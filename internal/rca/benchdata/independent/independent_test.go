package independent

import (
	"fmt"
	"sort"
	"testing"

	"github.com/Halcyonic-01/Chronicle/internal/event"
	"github.com/Halcyonic-01/Chronicle/internal/graph"
)

// These checks look only at the fixtures. They never run the analyzer, so the
// set stays untouched by RCA output while it is being written.

func matchRoot(e event.Event, r Root) bool {
	return e.Type == r.Kind && e.EntityKind == r.EntityKind && e.Namespace == r.Namespace && e.EntityName == r.Entity
}

func earliest(events []event.Event, r Root) *event.Event {
	var best *event.Event
	for i, e := range events {
		if matchRoot(e, r) && (best == nil || e.OccurredAt.Before(best.OccurredAt)) {
			best = &events[i]
		}
	}
	return best
}

func TestGroundTruthIsConsistentAndReachable(t *testing.T) {
	gr := graph.New()
	gr.SetEdges(Edges())
	ids, events := map[string]bool{}, map[string]bool{}
	for _, inc := range All() {
		if ids[inc.ID] {
			t.Fatalf("duplicate incident id %s", inc.ID)
		}
		ids[inc.ID] = true
		for _, e := range inc.Events {
			if events[e.ID] {
				t.Fatalf("%s: duplicate event id %s", inc.ID, e.ID)
			}
			events[e.ID] = true
		}
		switch inc.Expected {
		case RootCause:
			if !inc.Observable || len(inc.ActualRoots) == 0 || inc.Ambiguous {
				t.Errorf("%s: ROOT_CAUSE needs an observable, unambiguous actual root", inc.ID)
			}
		case Ambiguous:
			if !inc.Observable || !inc.Ambiguous || len(inc.Plausible) < 2 {
				t.Errorf("%s: AMBIGUOUS needs at least two plausible roots", inc.ID)
			}
		case NoRootCause:
			if inc.Observable || len(inc.ActualRoots) != 0 {
				t.Errorf("%s: NO_ROOT_CAUSE must have no actual root among the recorded events", inc.ID)
			}
		default:
			t.Errorf("%s: unknown expected result %q", inc.ID, inc.Expected)
		}
		if inc.Symptom.ID == "" {
			t.Errorf("%s: no symptom", inc.ID)
		}
		upstream := gr.Upstream(fmt.Sprintf("%s/%s/%s", inc.Symptom.Namespace, inc.Symptom.EntityKind, inc.Symptom.EntityName), 8)
		for _, r := range append(append([]Root{}, inc.ActualRoots...), inc.Plausible...) {
			e := earliest(inc.Events, r)
			if e == nil {
				t.Errorf("%s: root %+v is not in the event stream", inc.ID, r)
				continue
			}
			if inc.InScope && !e.OccurredAt.Before(inc.Symptom.OccurredAt) {
				t.Errorf("%s: root %s does not precede the symptom", inc.ID, r.Kind)
			}
			key := fmt.Sprintf("%s/%s/%s", e.Namespace, e.EntityKind, e.EntityName)
			if _, ok := upstream[key]; !ok {
				t.Errorf("%s: root %s is not upstream of the symptom", inc.ID, key)
			}
		}
	}
}

func TestEveryCategoryAndExpectationIsRepresented(t *testing.T) {
	cats, exp := map[string]int{}, map[string]int{}
	var scope [2]int
	for _, inc := range All() {
		cats[inc.Category]++
		exp[inc.Expected]++
		if inc.InScope {
			scope[0]++
		} else {
			scope[1]++
		}
	}
	for _, c := range []string{CatKnown, CatDifficult, CatRival, CatRootEffect, CatNoRoot, CatTiming} {
		if cats[c] == 0 {
			t.Errorf("category %s has no incidents", c)
		}
	}
	names := make([]string, 0, len(cats))
	for c := range cats {
		names = append(names, c)
	}
	sort.Strings(names)
	t.Logf("incidents=%d categories=%v expected=%v in-contract=%d outside-contract=%d", len(All()), cats, exp, scope[0], scope[1])
}
