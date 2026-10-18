package heal

import (
	"context"
	"sort"
)

// ReliabilityRow is how decisions of one action type, at one confidence band,
// turned out, counted once per outage (one failure analysed forty times is one
// piece of evidence). It is what a confidence floor should be chosen from, and
// the floors do not move until an action type has enough of it.
type ReliabilityRow struct {
	ActionType string `json:"action_type"`
	// BandFrom and BandTo bound the confidence of the decisions counted.
	BandFrom float64 `json:"confidence_from"`
	BandTo   float64 `json:"confidence_to"`
	// Confirmed and Contradicted are judged outcomes of proposed decisions;
	// Unknown could not be judged and is never credited. BlockedRight counts
	// outages read correctly but not proposed: evidence about the floor.
	Confirmed    int `json:"confirmed"`
	Contradicted int `json:"contradicted"`
	Unknown      int `json:"unknown"`
	BlockedRight int `json:"blocked_but_right"`
}

func (r ReliabilityRow) Decisive() int { return r.Confirmed + r.Contradicted }

// Precision is how often a decisive decision was right; zero with none.
func (r ReliabilityRow) Precision() float64 {
	if r.Decisive() == 0 {
		return 0
	}
	return float64(r.Confirmed) / float64(r.Decisive())
}

// ActionReliability sums one action type across its bands and says whether it has
// enough judged outcomes for its floor to be reviewed at all.
type ActionReliability struct {
	ActionType   string  `json:"action_type"`
	Decisive     int     `json:"decisive"`
	Confirmed    int     `json:"confirmed"`
	Contradicted int     `json:"contradicted"`
	Unknown      int     `json:"unknown"`
	BlockedRight int     `json:"blocked_but_right"`
	Precision    float64 `json:"precision"`
	// Ready: enough decisive outcomes to review this floor. Below it the number
	// is shown but is not a basis for moving anything.
	Ready bool             `json:"ready_for_floor_review"`
	Bands []ReliabilityRow `json:"bands"`
}

// BuildReliability groups rows by action type. Pure, so the rule that a floor
// waits for evidence can be tested without a database.
func BuildReliability(rows []ReliabilityRow, minDecisive int) []ActionReliability {
	by := map[string]*ActionReliability{}
	for _, r := range rows {
		a := by[r.ActionType]
		if a == nil {
			a = &ActionReliability{ActionType: r.ActionType}
			by[r.ActionType] = a
		}
		a.Confirmed += r.Confirmed
		a.Contradicted += r.Contradicted
		a.Unknown += r.Unknown
		a.BlockedRight += r.BlockedRight
		a.Bands = append(a.Bands, r)
	}
	out := make([]ActionReliability, 0, len(by))
	for _, a := range by {
		a.Decisive = a.Confirmed + a.Contradicted
		if a.Decisive > 0 {
			a.Precision = float64(a.Confirmed) / float64(a.Decisive)
		}
		a.Ready = a.Decisive >= minDecisive && minDecisive > 0
		sort.Slice(a.Bands, func(i, j int) bool { return a.Bands[i].BandFrom < a.Bands[j].BandFrom })
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ActionType < out[j].ActionType })
	return out
}

// Reliability reads the outcome of every judged outage by action type and
// confidence band. As in EvidenceSummary, an outage Chronicle acted on itself is
// left out: its own action cannot be evidence about its own reliability.
func (s *PostgresAuditStore) Reliability(ctx context.Context) ([]ReliabilityRow, error) {
	rows, err := s.pool.Query(ctx, `
		WITH per_outage AS (
			SELECT DISTINCT ON (cause_event_id) cause_event_id, action_type, confidence, outcome, proposed
			FROM heal_actions
			WHERE outcome IS NOT NULL AND confidence > 0 AND COALESCE(cause_event_id,'') <> ''
			  AND cause_event_id NOT IN (
			      SELECT cause_event_id FROM heal_actions
			      WHERE started_at IS NOT NULL AND cause_event_id IS NOT NULL)
			ORDER BY cause_event_id, proposed DESC, confidence DESC
		)
		SELECT action_type, width_bucket(confidence, 0, 1, 10) AS band,
		       count(*) FILTER (WHERE proposed AND outcome = 'confirmed'),
		       count(*) FILTER (WHERE proposed AND outcome = 'contradicted'),
		       count(*) FILTER (WHERE proposed AND outcome NOT IN ('confirmed','contradicted')),
		       count(*) FILTER (WHERE NOT proposed AND outcome = 'confirmed')
		FROM per_outage GROUP BY 1, 2 ORDER BY 1, 2`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReliabilityRow
	for rows.Next() {
		var r ReliabilityRow
		var band int
		if err := rows.Scan(&r.ActionType, &band, &r.Confirmed, &r.Contradicted, &r.Unknown, &r.BlockedRight); err != nil {
			return nil, err
		}
		r.BandFrom, r.BandTo = float64(band-1)/10, float64(band)/10
		out = append(out, r)
	}
	return out, rows.Err()
}

// EvidenceByAction is EvidenceSummary split by the action type each outage's
// decision was about.
func (s *PostgresAuditStore) EvidenceByAction(ctx context.Context) (map[string]EvidenceSummary, error) {
	rows, err := s.pool.Query(ctx, `
		WITH per_outage AS (
			SELECT cause_event_id,
			       (array_agg(action_type ORDER BY proposed DESC, confidence DESC))[1] AS action_type,
			       bool_or(proposed) AS would_act,
			       (array_agg(outcome ORDER BY confidence DESC)
			          FILTER (WHERE outcome IN ('confirmed','contradicted')))[1] AS verdict
			FROM heal_actions
			WHERE outcome IS NOT NULL AND COALESCE(cause_event_id,'') <> ''
			  AND cause_event_id NOT IN (
			      SELECT cause_event_id FROM heal_actions
			      WHERE started_at IS NOT NULL AND cause_event_id IS NOT NULL)
			GROUP BY cause_event_id
		)
		SELECT action_type,
		       count(*) FILTER (WHERE would_act AND verdict = 'confirmed'),
		       count(*) FILTER (WHERE would_act AND verdict = 'contradicted'),
		       count(*) FILTER (WHERE verdict IS NULL),
		       count(*) FILTER (WHERE NOT would_act AND verdict = 'confirmed')
		FROM per_outage GROUP BY action_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]EvidenceSummary{}
	for rows.Next() {
		var typ string
		var e EvidenceSummary
		if err := rows.Scan(&typ, &e.Confirmed, &e.Contradicted, &e.Unknown, &e.MissedByFloor); err != nil {
			return nil, err
		}
		out[typ] = e
	}
	return out, rows.Err()
}
