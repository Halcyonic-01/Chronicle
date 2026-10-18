package heal

import (
	"context"
	"testing"
	"time"
)

func TestAFloorIsOnlyReadyForReviewWithEnoughJudgedOutcomes(t *testing.T) {
	rows := []ReliabilityRow{
		{ActionType: ActionRestoreReplicas, BandFrom: 0.8, BandTo: 0.9, Confirmed: 14, Contradicted: 1, Unknown: 9},
		{ActionType: ActionRestoreReplicas, BandFrom: 0.9, BandTo: 1, Confirmed: 5, Contradicted: 0},
		{ActionType: ActionRollbackDeployment, BandFrom: 0.9, BandTo: 1, Confirmed: 12, Contradicted: 0, Unknown: 40, BlockedRight: 3},
	}
	got := BuildReliability(rows, 20)
	byType := map[string]ActionReliability{}
	for _, a := range got {
		byType[a.ActionType] = a
	}
	restore, rollback := byType[ActionRestoreReplicas], byType[ActionRollbackDeployment]
	if restore.Decisive != 20 || !restore.Ready || restore.Precision < 0.95 || len(restore.Bands) != 2 {
		t.Errorf("restore: %+v", restore)
	}
	// Forty outcomes nobody could judge are not evidence: twelve decisive is not enough.
	if rollback.Decisive != 12 || rollback.Ready || rollback.Unknown != 40 || rollback.BlockedRight != 3 {
		t.Errorf("rollback: %+v", rollback)
	}
	if got[0].ActionType > got[1].ActionType {
		t.Error("the table is not in a stable order")
	}
	if BuildReliability(rows, 0)[0].Ready {
		t.Error("with no minimum configured nothing may be called ready")
	}
}

// Each action type is held to its own record, and to the overall one.
func TestEachActionTypeMustClearTheEvidenceBarOnItsOwn(t *testing.T) {
	p := livePolicy()
	p.ObservationSince = time.Now().Add(-31 * 24 * time.Hour)
	p.MinDecisive, p.MinPrecision = 20, 0.8
	p.Evidence = EvidenceSummary{Confirmed: 30, Contradicted: 0} // overall: excellent, from restores
	a := approvedRestart("per-action")
	a.ActionType = ActionRestartPod

	p.ActionEvidenceKnown, p.ActionEvidence = true, EvidenceSummary{Confirmed: 2}
	if err := p.Allows(a); err == nil {
		t.Fatal("a restart with two judged outcomes ran on the strength of other actions' record")
	}
	p.ActionEvidence = EvidenceSummary{Confirmed: 15, Contradicted: 10}
	if err := p.Allows(a); err == nil {
		t.Fatal("a restart that has been right 60% of the time ran")
	}
	p.ActionEvidence = EvidenceSummary{Confirmed: 25, Contradicted: 1}
	if err := p.Allows(a); err != nil {
		t.Fatalf("a restart with its own good record was refused: %v", err)
	}
	// A store that cannot split the evidence leaves today's overall check as it was.
	p.ActionEvidenceKnown, p.ActionEvidence = false, EvidenceSummary{}
	if err := p.Allows(a); err != nil {
		t.Fatalf("without per-action data the overall record should decide: %v", err)
	}
	// And the overall record still has to pass.
	p.Evidence = EvidenceSummary{Confirmed: 5, Contradicted: 20}
	p.ActionEvidenceKnown, p.ActionEvidence = true, EvidenceSummary{Confirmed: 25}
	if err := p.Allows(a); err == nil {
		t.Fatal("a good per-action record overrode a bad overall one")
	}
}

type splitEvidence struct {
	executionMemory
	by  map[string]EvidenceSummary
	err error
}

func (s *splitEvidence) EvidenceByAction(context.Context) (map[string]EvidenceSummary, error) {
	return s.by, s.err
}

func TestAnUnreadableActionRecordKeepsTheGateShut(t *testing.T) {
	c := liveController(&splitEvidence{by: map[string]EvidenceSummary{ActionRestartPod: {Confirmed: 30}}}, &recordingExecutor{})
	if got, known := c.EvidenceFor(context.Background(), ActionRestartPod); !known || got.Confirmed != 30 {
		t.Fatalf("%+v %v", got, known)
	}
	if got, known := c.EvidenceFor(context.Background(), ActionRollbackDeployment); !known || got.Decisive() != 0 {
		t.Fatalf("an action with no record: %+v %v", got, known)
	}
	c = liveController(&splitEvidence{err: context.DeadlineExceeded}, &recordingExecutor{})
	if got, known := c.EvidenceFor(context.Background(), ActionRestartPod); !known || got.Decisive() != 0 {
		t.Fatalf("a failed read must count as no evidence, not as none needed: %+v %v", got, known)
	}
}

// The per-action split and the reliability table count one outage once, credit
// only proposed decisions, and leave out what Chronicle acted on itself.
func TestPostgresEvidenceAndReliabilityAreSplitByActionType(t *testing.T) {
	s, pool, sx := healTestStore(t)
	ctx, now := context.Background(), time.Now().UTC()
	baseEv, err := s.EvidenceByAction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	baseRel, err := s.Reliability(ctx)
	if err != nil {
		t.Fatal(err)
	}
	judge := func(a *Action, action, outcome string, proposed bool, executed bool, confidence float64) {
		a.ActionType, a.Confidence, a.Proposed = action, confidence, proposed
		if err := s.RecordAction(ctx, a); err != nil {
			t.Fatal(err)
		}
		q := `UPDATE heal_actions SET outcome=$2, status='expired' WHERE id=$1`
		if executed {
			q = `UPDATE heal_actions SET outcome=$2, status='succeeded', started_at=now() WHERE id=$1`
		}
		if _, err := pool.Exec(ctx, q, a.ID, outcome); err != nil {
			t.Fatal(err)
		}
	}
	judge(proposal("r1-"+sx, "c-r1-"+sx, "redis", now), ActionRestoreReplicas, "confirmed", true, false, 0.85)
	judge(proposal("r2-"+sx, "c-r2-"+sx, "redis", now), ActionRestoreReplicas, "contradicted", true, false, 0.85)
	judge(proposal("r3-"+sx, "c-r3-"+sx, "redis", now), ActionRestoreReplicas, "confirmed", true, true, 0.85) // acted on by Chronicle: not evidence
	judge(proposal("b1-"+sx, "c-b1-"+sx, "api", now), ActionRollbackDeployment, "confirmed", true, false, 0.95)
	judge(proposal("b2-"+sx, "c-b2-"+sx, "api", now), ActionRollbackDeployment, "confirmed", false, false, 0.5) // read right, not proposed
	// One outage analysed twice is one piece of evidence.
	first := proposal("d1-"+sx, "c-dup-"+sx, "redis", now)
	second := proposal("d2-"+sx, "c-dup-"+sx, "redis", now)
	judge(first, ActionRestartPod, "confirmed", true, false, 0.9)
	judge(second, ActionRestartPod, "confirmed", true, false, 0.9)

	ev, err := s.EvidenceByAction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	diff := func(typ string) EvidenceSummary {
		now, was := ev[typ], baseEv[typ]
		return EvidenceSummary{Confirmed: now.Confirmed - was.Confirmed, Contradicted: now.Contradicted - was.Contradicted, MissedByFloor: now.MissedByFloor - was.MissedByFloor}
	}
	if got := diff(ActionRestoreReplicas); got.Confirmed != 1 || got.Contradicted != 1 {
		t.Errorf("restore: %+v (the outage Chronicle executed must not count)", got)
	}
	if got := diff(ActionRollbackDeployment); got.Confirmed != 1 || got.MissedByFloor != 1 {
		t.Errorf("rollback: %+v", got)
	}
	if got := diff(ActionRestartPod); got.Confirmed != 1 {
		t.Errorf("restart: %+v (one outage, two analyses)", got)
	}

	rel, err := s.Reliability(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sum := func(rows []ReliabilityRow, typ string) (confirmed, contradicted int) {
		for _, r := range rows {
			if r.ActionType == typ {
				confirmed += r.Confirmed
				contradicted += r.Contradicted
			}
		}
		return
	}
	nc, nx := sum(rel, ActionRestoreReplicas)
	bc, bx := sum(baseRel, ActionRestoreReplicas)
	if nc-bc != 1 || nx-bx != 1 {
		t.Errorf("reliability for restore: %d confirmed, %d contradicted more", nc-bc, nx-bx)
	}
}
