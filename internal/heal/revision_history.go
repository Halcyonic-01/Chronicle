package heal

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// A rollback returns to a revision, so Chronicle must be able to show from its own
// record that the revision ran healthily before it is proposed. "The one before"
// is not evidence of that: it may be as bad as the change being undone.
//
// What counts as healthy: the revision was the one running for at least
// MinHealthy, and in that time (after the rollout that brought it in had settled)
// no failure fact or alert was recorded for the workload or its pods. What cannot
// be shown is not assumed: a revision Chronicle did not observe for that long
// is unknown, and unknown is not good enough.

// RevisionPeriod is one revision of a workload and when it was the running one,
// as far as Chronicle's record shows.
type RevisionPeriod struct {
	Hash string
	// Since is when the revision became the running one; zero when Chronicle did
	// not see it begin (it was already running when Chronicle started watching).
	Since time.Time
	// Until is when it was replaced.
	Until time.Time
}

// RevisionHistory is what the engine needs from the store to choose a rollback
// target. Optional: a store that cannot answer blocks a rollback that names
// revisions, rather than letting it through unverified.
type RevisionHistory interface {
	// PriorRevisions lists the revisions the workload ran before `bad`, newest
	// first, at most limit.
	PriorRevisions(ctx context.Context, namespace, workload, bad string, limit int) ([]RevisionPeriod, error)
	// RanHealthily says whether the revision ran, as the running one, for at least
	// min without a failure being recorded, and why not if it did not.
	RanHealthily(ctx context.Context, namespace, workload string, p RevisionPeriod, min time.Duration) (ok bool, why string, err error)
}

// Defaults: how long a revision must have run healthily to be returned to, and
// how many earlier revisions are looked through.
const (
	DefaultRollbackMinHealthy = 30 * time.Minute
	maxRevisionsConsidered    = 5
	// rolloutGrace is the time after a revision takes over that is the rollout
	// itself: pods being replaced are not a failure of the revision.
	rolloutGrace = 2 * time.Minute
)

// RevisionStep is one recorded change of template, from the change events.
type RevisionStep struct {
	From, To string
	At       time.Time
}

// priorRevisions builds the chain of revisions before `bad` from recorded
// steps, newest first. A single edit is recorded as several events (a deploy
// and a config change) with the same hashes; they are one step.
func priorRevisions(steps []RevisionStep, bad string, limit int) []RevisionPeriod {
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].At.Before(steps[j].At) })
	var out []RevisionPeriod
	until := time.Time{}
	current := bad
	// Walk back: the latest step that arrived at `current` says what came before.
	for len(out) < limit {
		idx := -1
		for i := len(steps) - 1; i >= 0; i-- {
			if steps[i].To == current && (until.IsZero() || !steps[i].At.After(until)) {
				idx = i
				break
			}
		}
		if idx < 0 {
			return out
		}
		step := steps[idx]
		if step.From == "" || step.From == current {
			return out
		}
		replaced := step.At
		p := RevisionPeriod{Hash: step.From, Until: replaced}
		// When did that revision begin? The step that arrived at it.
		for i := idx - 1; i >= 0; i-- {
			if steps[i].To == step.From {
				p.Since = steps[i].At
				break
			}
		}
		out = append(out, p)
		current, until = step.From, p.Since
		if p.Since.IsZero() {
			return out // its beginning was not observed; nothing earlier is known
		}
	}
	return out
}

// judgeRevision decides whether a period counts as having run healthily, given
// when Chronicle's record of the workload begins and the failures recorded in
// the period. Pure, so every refusal is tested without a database.
func judgeRevision(p RevisionPeriod, observedFrom time.Time, failures []time.Time, min time.Duration) (bool, string) {
	start := p.Since
	if start.IsZero() {
		if observedFrom.IsZero() {
			return false, "Chronicle has no record of this workload before the change"
		}
		start = observedFrom
	}
	if ran := p.Until.Sub(start); ran < min {
		what := "was seen running"
		if p.Since.IsZero() {
			what = "was observed running (it was already running when Chronicle began watching)"
		}
		return false, fmt.Sprintf("it %s for %s, less than the %s required", what, ran.Round(time.Second), min)
	}
	for _, f := range failures {
		if f.After(start.Add(rolloutGrace)) && f.Before(p.Until) {
			return false, fmt.Sprintf("a failure was recorded at %s while it was the running revision", f.UTC().Format(time.RFC3339))
		}
	}
	return true, fmt.Sprintf("ran for %s without a recorded failure", p.Until.Sub(start).Round(time.Minute))
}

// withTarget sets the revision a rollback must return to in a decision's payload.
func withTarget(payload json.RawMessage, hash string) (json.RawMessage, error) {
	m := map[string]any{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
	}
	m["target_template_hash"] = hash
	return json.Marshal(m)
}

// chooseRevision picks the revision a rollback returns to, or says why none may
// be proposed. It prefers the revision just before the change, and otherwise the
// newest earlier one that is known to have run healthily.
func (e *Engine) chooseRevision(ctx context.Context, action *Action, workload, bad string) (blockedWhy string, err error) {
	history, ok := e.Store.(RevisionHistory)
	if !ok {
		return "the store cannot show which revisions ran healthily, so no rollback target can be verified", nil
	}
	min := e.RollbackMinHealthy
	if min <= 0 {
		min = DefaultRollbackMinHealthy
	}
	prior, err := history.PriorRevisions(ctx, action.Namespace, workload, bad, maxRevisionsConsidered)
	if err != nil {
		return "", fmt.Errorf("read the revision history: %w", err)
	}
	if len(prior) == 0 {
		return "Chronicle has no record of a revision before this change to return to", nil
	}
	var why []string
	for i, p := range prior {
		healthy, reason, err := history.RanHealthily(ctx, action.Namespace, workload, p, min)
		if err != nil {
			return "", fmt.Errorf("judge revision %s: %w", p.Hash, err)
		}
		if !healthy {
			why = append(why, fmt.Sprintf("revision %s: %s", short(p.Hash), reason))
			continue
		}
		payload, err := withTarget(action.Payload, p.Hash)
		if err != nil {
			return "", err
		}
		action.Payload = payload
		if i == 0 {
			action.Reasoning = append(action.Reasoning, fmt.Sprintf("the previous revision %s %s", short(p.Hash), reason))
		} else {
			action.Reasoning = append(action.Reasoning, fmt.Sprintf("skips %d revision(s) not known to have run healthily; revision %s %s (%s)",
				i, short(p.Hash), reason, strings.Join(why, "; ")))
		}
		return "", nil
	}
	return fmt.Sprintf("no earlier revision of %s is known to have run healthily for %s: %s", workload, min, strings.Join(why, "; ")), nil
}

func short(hash string) string {
	if len(hash) > 8 {
		return hash[:8]
	}
	return hash
}
