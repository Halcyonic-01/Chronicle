package heal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/Halcyonic-01/Chronicle/internal/gitops"
)

// GitOps-aware healing. A workload whose desired state lives in Git (Argo CD or
// Flux) is never written to directly: the controller would undo the change, or
// the cluster would drift from Git. Instead Chronicle proposes a git_revert: a
// pull request putting the application's files back to the last revision that
// ran healthily. Everything else about the decision is unchanged: the same
// rules, floors, evidence and approval gates, the same audit record. The cluster
// changes only when people merge the pull request and the controller syncs it;
// then Chronicle checks the symptom cleared, as after any action.

// GitOpsRevision is one revision a GitOps controller deployed, from the
// collectors' records.
type GitOpsRevision struct {
	Revision    string
	At          time.Time
	RepoURL     string
	Branch      string
	Path        string
	Namespace   string // the destination namespace, when the controller says
	AutoSync    bool
	MultiSource bool
	Chart       string // a Helm chart from a chart repository: not a Git path
	SourceKind  string // a Flux source that is not a GitRepository
	Suspended   bool
}

// GitOpsHistory reads what a GitOps application deployed, newest first, up to at.
type GitOpsHistory interface {
	GitOpsRevisions(ctx context.Context, controller, app string, at time.Time) ([]GitOpsRevision, error)
}

// currentAndPrevious picks the revision running at the end of the records and
// the different one before it. Repeats of a revision (a collector restart
// records the current one again) are one period, beginning at its first record.
func currentAndPrevious(newestFirst []GitOpsRevision) (cur, prev GitOpsRevision, ok bool) {
	if len(newestFirst) == 0 || newestFirst[0].Revision == "" {
		return cur, prev, false
	}
	cur = newestFirst[0]
	i := 1
	for ; i < len(newestFirst) && newestFirst[i].Revision == cur.Revision; i++ {
		cur.At = newestFirst[i].At
	}
	if i == len(newestFirst) {
		return cur, prev, false
	}
	prev = newestFirst[i]
	for i++; i < len(newestFirst) && newestFirst[i].Revision == prev.Revision; i++ {
		prev.At = newestFirst[i].At
	}
	return cur, prev, true
}

// gitopsTarget names the GitOps application behind a cause, or "" for a cause
// that is not GitOps-managed (or is managed by a release controller, which owns
// rollback itself).
func gitopsTarget(cause eventView) (controller, app string) {
	if m := gjson.GetBytes(cause.Payload, "gitops").String(); m != "" {
		kind, name, _ := strings.Cut(m, ":")
		if kind == "argocd" || kind == "flux" {
			return kind, name
		}
		return "", ""
	}
	switch {
	case cause.Source == "argocd" && cause.EntityKind == "Application":
		return "argocd", cause.EntityName
	case cause.Source == "flux" && cause.EntityKind == "Kustomization":
		return "flux", cause.EntityName
	}
	return "", ""
}

// eventView is the part of an event the planner reads.
type eventView struct {
	Source, EntityKind, EntityName, Namespace string
	OccurredAt                                time.Time
	Payload                                   json.RawMessage
}

// syncAllowance is how much later than the cluster change the controller's own
// record of the revision may appear: the collectors poll Argo CD and Flux.
const syncAllowance = 2 * time.Minute

// planGitRevert turns a rollback of a GitOps-managed workload into a git_revert,
// or says why it cannot (blocked reports whether that is a refusal of the action
// rather than "no automatic remedy"). It changes action only on success.
func (e *Engine) planGitRevert(ctx context.Context, action *Action, cause eventView, controller, app string) (why string, blocked bool, err error) {
	manual := gitopsProposal(controller + ":" + app)
	history, ok := e.Store.(GitOpsHistory)
	if !ok {
		return manual, false, nil
	}
	revisions, err := history.GitOpsRevisions(ctx, controller, app, cause.OccurredAt.Add(syncAllowance))
	if err != nil {
		return "", false, fmt.Errorf("read the GitOps history of %s: %w", app, err)
	}
	cur, prev, found := currentAndPrevious(revisions)
	switch {
	case !found:
		return manual + "; Chronicle has no record of an earlier revision of it to return to", false, nil
	case cur.MultiSource:
		return manual + "; it has several sources, which a Git revert does not handle", false, nil
	case cur.Chart != "":
		return manual + fmt.Sprintf("; it deploys chart %q from a chart repository, not files in Git", cur.Chart), false, nil
	case cur.SourceKind != "":
		return manual + fmt.Sprintf("; its source is a %s, not a Git repository", cur.SourceKind), false, nil
	case cur.RepoURL == "":
		return manual + "; the repository it deploys from is unknown", false, nil
	}
	namespace := firstNonEmpty(cur.Namespace, cause.Namespace)
	workload := app
	if cause.EntityKind == "Deployment" {
		workload = cause.EntityName
	}
	// From here the decision is a git_revert, blocked or not: the record names
	// the remedy that was considered, never a direct rollback.
	payload := map[string]any{}
	_ = json.Unmarshal(action.Payload, &payload)
	payload["gitops"], payload["gitops_app"] = controller+":"+app, app
	payload["repo_url"], payload["branch"], payload["path"] = cur.RepoURL, cur.Branch, cur.Path
	payload["bad_revision"], payload["good_revision"] = cur.Revision, prev.Revision
	payload["auto_sync"], payload["workload_name"] = cur.AutoSync || (controller == "flux" && !cur.Suspended), workload
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", false, err
	}
	action.ActionType, action.Target, action.Namespace, action.Workload = ActionGitRevert, controller+":"+app, namespace, app
	action.Payload = raw

	// The revision returned to must have run healthily, as for any rollback. Its
	// time ended when the bad revision reached the cluster, which the cluster's own
	// change dates more exactly than the controller's record of the sync.
	until := cur.At
	if cause.OccurredAt.After(prev.At) && cause.OccurredAt.Before(until) {
		until = cause.OccurredAt
	}
	store, ok := e.Store.(RevisionHistory)
	if !ok {
		return "the store cannot show which revisions ran healthily, so no revert target can be verified", true, nil
	}
	min := e.RollbackMinHealthy
	if min <= 0 {
		min = DefaultRollbackMinHealthy
	}
	healthy, reason, err := store.RanHealthily(ctx, namespace, workload, RevisionPeriod{Hash: prev.Revision, Since: prev.At, Until: until}, min)
	if err != nil {
		return "", false, fmt.Errorf("judge revision %s: %w", short(prev.Revision), err)
	}
	if !healthy {
		return fmt.Sprintf("the revision of %s before %s (%s) is not known to have run healthily: %s",
			app, short(cur.Revision), short(prev.Revision), reason), true, nil
	}
	action.Reasoning = append(action.Reasoning, fmt.Sprintf("revision %s %s", short(prev.Revision), reason))
	where := cur.RepoURL
	if cur.Path != "" {
		where += " (" + cur.Path + ")"
	}
	action.Reasoning = append(action.Reasoning, fmt.Sprintf("%s is managed by %s from %s; it was running revision %s when this broke",
		app, controllerName(controller), where, short(cur.Revision)))
	if !gjson.GetBytes(raw, "auto_sync").Bool() {
		action.Reasoning = append(action.Reasoning, fmt.Sprintf("automatic sync is off for %s: after the pull request is merged, someone must sync it", app))
	}
	return "", false, nil
}

func controllerName(c string) string {
	if c == "flux" {
		return "Flux"
	}
	return "Argo CD"
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// ---- executing ---------------------------------------------------------------

// GitReverter opens the pull request for a git_revert and reads its status.
type GitReverter interface {
	Revert(context.Context, gitops.Request) (gitops.Change, error)
	PullStatus(ctx context.Context, repoURL string, number int) (gitops.Status, error)
}

func (e *KubernetesExecutor) gitRevert(ctx context.Context, action *Action) (string, error) {
	if e.Git == nil {
		return "", refuse("Git remediation is not configured (HEAL_GIT_TOKEN and HEAL_GIT_API_URL)")
	}
	p := action.Payload
	approvers := make([]string, 0, len(action.Approvals))
	for _, a := range action.Approvals {
		approvers = append(approvers, a.Display)
	}
	if len(approvers) == 0 && action.DecisionBy != "" {
		approvers = append(approvers, action.DecisionBy)
	}
	evidence := action.Reasoning
	if len(evidence) > 6 {
		evidence = evidence[:6]
	}
	change, err := e.Git.Revert(ctx, gitops.Request{
		DecisionID: action.ID, App: action.Target,
		RepoURL: gjson.GetBytes(p, "repo_url").String(), Branch: gjson.GetBytes(p, "branch").String(), Path: gjson.GetBytes(p, "path").String(),
		BadRevision: gjson.GetBytes(p, "bad_revision").String(), GoodRevision: gjson.GetBytes(p, "good_revision").String(),
		Summary:   fmt.Sprintf("%s (%s) is the likely cause of the incident, at confidence %.2f.", action.CauseType, action.Rule, action.Confidence),
		Evidence:  evidence,
		Approvers: approvers,
	})
	var refused *gitops.Refusal
	if errors.As(err, &refused) {
		return "", refuse("%s", refused.Reason)
	}
	if err != nil {
		return "", err
	}
	action.GitChange = &GitChange{URL: change.URL, Number: change.Number, Branch: change.Branch, Commit: change.Commit, State: GitOpen}
	if change.Reused {
		return fmt.Sprintf("pull request #%d was already open: %s", change.Number, change.URL), nil
	}
	return fmt.Sprintf("opened pull request #%d restoring %d file(s) to revision %s: %s", change.Number, len(change.Files),
		short(gjson.GetBytes(p, "good_revision").String()), change.URL), nil
}

// ---- following the pull request ----------------------------------------------

// GitChangeStore is what the tracker needs from the store.
type GitChangeStore interface {
	RecordGitChange(ctx context.Context, id string, change GitChange) error
	PendingGitChanges(ctx context.Context, limit int) ([]Action, error)
	UpdateGitChange(ctx context.Context, id string, change GitChange) error
	GitOpsHistory
}

// GitStatusReader reads a pull request's state.
type GitStatusReader interface {
	PullStatus(ctx context.Context, repoURL string, number int) (gitops.Status, error)
}

// trackGitChanges moves each open Git remediation along: merged, then synced by
// the controller (the symptom check starts then, since only then has anything
// changed in the cluster), or closed without merging.
func (c *Controller) trackGitChanges(ctx context.Context, now time.Time) {
	store, ok := c.Store.(GitChangeStore)
	if !ok || c.Git == nil {
		return
	}
	pending, err := store.PendingGitChanges(ctx, 50)
	if err != nil {
		slog.Warn("could not list open Git remediations", "err", err)
		return
	}
	for i := range pending {
		a := &pending[i]
		g := *a.GitChange
		switch g.State {
		case GitOpen:
			st, err := c.Git.PullStatus(ctx, gjson.GetBytes(a.Payload, "repo_url").String(), g.Number)
			if err != nil {
				slog.Warn("could not read a pull request", "action", a.ID, "err", err)
				continue
			}
			switch st.State {
			case "merged":
				at := st.MergedAt
				if at.IsZero() {
					at = now
				}
				g.State, g.MergedAt, g.Detail = GitMerged, &at, "merged; waiting for "+a.Target+" to sync"
				if st.MergeCommit != "" {
					g.Detail = fmt.Sprintf("merged as %s; waiting for %s to sync", short(st.MergeCommit), a.Target)
				}
			case "closed":
				g.State, g.Detail = GitClosed, "closed without merging: nothing changed in the cluster"
			default:
				continue
			}
		case GitMerged:
			controller, app, _ := strings.Cut(a.Target, ":")
			revisions, err := store.GitOpsRevisions(ctx, controller, app, now)
			if err != nil || len(revisions) == 0 {
				continue
			}
			latest := revisions[0]
			bad := gjson.GetBytes(a.Payload, "bad_revision").String()
			if latest.Revision == bad || g.MergedAt == nil || latest.At.Before(g.MergedAt.Add(-time.Minute)) {
				if g.MergedAt != nil && now.Sub(*g.MergedAt) > 30*time.Minute && !strings.Contains(g.Detail, "not synced") {
					g.Detail = fmt.Sprintf("merged %s ago but %s has not synced it: is automatic sync on?", now.Sub(*g.MergedAt).Round(time.Minute), app)
					_ = store.UpdateGitChange(ctx, a.ID, g)
				}
				continue
			}
			synced := now
			g.State, g.SyncedAt, g.Detail = GitSynced, &synced, fmt.Sprintf("%s synced revision %s", app, short(latest.Revision))
		default:
			continue
		}
		if err := store.UpdateGitChange(ctx, a.ID, g); err != nil {
			slog.Warn("could not record a pull request's progress", "action", a.ID, "err", err)
			continue
		}
		a.GitChange = &g
		if g.State == GitSynced && c.Symptoms != nil {
			if err := c.Symptoms.Begin(ctx, a.ID, now); err != nil {
				slog.Warn("could not start the symptom check", "action", a.ID, "err", err)
			}
		}
		if c.Notifier != nil && (g.State == GitMerged || g.State == GitClosed) {
			a.Result = fmt.Sprintf("pull request #%d %s", g.Number, g.Detail)
			if err := c.Notifier.Notify(context.Background(), a); err != nil {
				slog.Warn("notification failed", "action", a.ID, "err", err)
			}
		}
	}
}
