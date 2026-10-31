// Package gitops turns a healing decision about a GitOps-managed workload into a
// change in Git: a branch that puts the application's files back to the last
// revision that ran healthily, and a pull request for people to review and merge.
// Argo CD or Flux then syncs the merged change, so the cluster's desired state
// stays in Git and nothing is changed behind the controller's back.
//
// It never pushes to the branch the controller watches. It refuses, rather than
// guesses, when Git does not match the evidence: a revision that is not on the
// branch, files changed since the bad revision, or nothing left to revert.
package gitops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5/memfs"
	git "github.com/go-git/go-git/v5"
	gitconfig "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"
)

// Config is how Chronicle reaches Git. Every field but the token is safe to show.
type Config struct {
	// Token is a personal access token with permission to push branches and
	// open pull requests on the allowed repositories (HEAL_GIT_TOKEN).
	Token string
	// Username goes with the token for HTTPS pushes (HEAL_GIT_USERNAME). GitHub
	// accepts any; Gitea and Forgejo want the token's owner.
	Username string
	// APIURL is the pull-request API (HEAL_GIT_API_URL): https://api.github.com,
	// or https://<host>/api/v1 for Gitea and Forgejo.
	APIURL string
	// AllowedRepos are the only repositories Chronicle may push to
	// (HEAL_GIT_ALLOWED_REPOS). Empty means none.
	AllowedRepos []string
	AuthorName   string
	AuthorEmail  string
	HTTP         *http.Client
}

// ConfigFromEnv reads the configuration; ok is false when no token or API is set.
func ConfigFromEnv() (Config, bool) {
	c := Config{
		Token:       strings.TrimSpace(os.Getenv("HEAL_GIT_TOKEN")),
		Username:    valueOr(os.Getenv("HEAL_GIT_USERNAME"), "chronicle"),
		APIURL:      strings.TrimRight(strings.TrimSpace(os.Getenv("HEAL_GIT_API_URL")), "/"),
		AuthorName:  valueOr(os.Getenv("HEAL_GIT_AUTHOR_NAME"), "Chronicle"),
		AuthorEmail: valueOr(os.Getenv("HEAL_GIT_AUTHOR_EMAIL"), "chronicle@localhost"),
	}
	for _, r := range strings.Split(os.Getenv("HEAL_GIT_ALLOWED_REPOS"), ",") {
		if r = strings.TrimSpace(r); r != "" {
			c.AllowedRepos = append(c.AllowedRepos, r)
		}
	}
	return c, c.Token != "" && c.APIURL != ""
}

// Request is what to revert, and why.
type Request struct {
	DecisionID string
	// App names the application for people: "argocd:api", "flux:apps".
	App     string
	RepoURL string
	Branch  string
	// Path is the application's directory in the repository; "" is the root.
	Path string
	// BadRevision is the commit the controller deployed when things broke;
	// GoodRevision the one it ran healthily before.
	BadRevision  string
	GoodRevision string
	// Summary and Evidence go into the pull request for the reviewer.
	Summary   string
	Evidence  []string
	Approvers []string
}

// Change is the pull request that was opened.
type Change struct {
	URL    string   `json:"url"`
	Number int      `json:"number"`
	Branch string   `json:"branch"`
	Commit string   `json:"commit"`
	Files  []string `json:"files"`
	// Reused: the branch and pull request already existed (a retry).
	Reused bool `json:"reused,omitempty"`
}

// Status is where a pull request stands.
type Status struct {
	State       string // open, merged or closed (closed without merging)
	MergeCommit string
	MergedAt    time.Time
}

// Refusal: Git does not match the evidence, so nothing was changed.
type Refusal struct{ Reason string }

func (r *Refusal) Error() string { return r.Reason }

func refuse(format string, args ...any) error { return &Refusal{Reason: fmt.Sprintf(format, args...)} }

// Remediator does the Git work.
type Remediator struct{ cfg Config }

func New(cfg Config) *Remediator {
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	return &Remediator{cfg: cfg}
}

// Allowed says whether Chronicle may push to the repository.
func (r *Remediator) Allowed(repoURL string) bool {
	want := normalize(repoURL)
	for _, a := range r.cfg.AllowedRepos {
		if normalize(a) == want {
			return true
		}
	}
	return false
}

// Revert opens a pull request that puts the application's files back to the
// good revision. Safe to call again for the same decision: the branch is named
// after it, and an open pull request for that branch is returned as it is.
func (r *Remediator) Revert(ctx context.Context, req Request) (Change, error) {
	switch {
	case req.RepoURL == "" || req.Branch == "" || req.BadRevision == "" || req.GoodRevision == "":
		return Change{}, refuse("the decision lacks the repository, branch or revisions a Git revert needs")
	case !r.Allowed(req.RepoURL):
		return Change{}, refuse("repository %s is not in HEAL_GIT_ALLOWED_REPOS", req.RepoURL)
	case req.BadRevision == req.GoodRevision:
		return Change{}, refuse("the bad and good revisions are the same commit")
	}
	branch := BranchName(req.App, req.DecisionID)
	if existing, ok, err := r.findOpenPull(ctx, req.RepoURL, branch); err != nil {
		return Change{}, err
	} else if ok {
		existing.Reused = true
		return existing, nil
	}

	auth := &githttp.BasicAuth{Username: r.cfg.Username, Password: r.cfg.Token}
	fs := memfs.New()
	repo, err := git.CloneContext(ctx, memory.NewStorage(), fs, &git.CloneOptions{
		URL: req.RepoURL, Auth: auth, ReferenceName: plumbing.NewBranchReferenceName(req.Branch), SingleBranch: true,
	})
	if err != nil {
		return Change{}, fmt.Errorf("clone %s: %w", req.RepoURL, err)
	}
	head, err := repo.Head()
	if err != nil {
		return Change{}, err
	}
	tip, err := repo.CommitObject(head.Hash())
	if err != nil {
		return Change{}, err
	}
	bad, err := commitOnBranch(repo, tip, req.BadRevision)
	if err != nil {
		return Change{}, err
	}
	good, err := commitOnBranch(repo, bad, req.GoodRevision)
	if err != nil {
		return Change{}, err
	}

	files, err := changedUnder(good, bad, req.Path)
	if err != nil {
		return Change{}, err
	}
	if len(files) == 0 {
		return Change{}, refuse("revisions %s and %s do not differ under %q: there is nothing to revert", short(req.GoodRevision), short(req.BadRevision), displayPath(req.Path))
	}
	// Every file must be as the bad revision left it. A later commit means
	// somebody is already working on it, and overwriting that would be a guess.
	var changedSince, alreadyGood []string
	for _, f := range files {
		atTip, err := content(tip, f)
		if err != nil {
			return Change{}, err
		}
		atBad, err := content(bad, f)
		if err != nil {
			return Change{}, err
		}
		atGood, err := content(good, f)
		if err != nil {
			return Change{}, err
		}
		if !equal(atTip, atBad) {
			changedSince = append(changedSince, f)
		}
		if equal(atTip, atGood) {
			alreadyGood = append(alreadyGood, f)
		}
	}
	if len(alreadyGood) == len(files) {
		return Change{}, refuse("no change: the files under %q on %s already match revision %s", displayPath(req.Path), req.Branch, short(req.GoodRevision))
	}
	if len(changedSince) > 0 {
		return Change{}, refuse("refusing to revert: %s changed on %s after revision %s; revert by hand",
			strings.Join(changedSince, ", "), req.Branch, short(req.BadRevision))
	}

	wt, err := repo.Worktree()
	if err != nil {
		return Change{}, err
	}
	if err := wt.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch), Create: true, Hash: tip.Hash}); err != nil {
		return Change{}, err
	}
	for _, f := range files {
		atGood, err := content(good, f)
		if err != nil {
			return Change{}, err
		}
		if atGood == nil {
			if _, err := wt.Remove(f); err != nil {
				return Change{}, err
			}
			continue
		}
		if err := fs.MkdirAll(path.Dir(f), 0o755); err != nil {
			return Change{}, err
		}
		file, err := fs.Create(f)
		if err != nil {
			return Change{}, err
		}
		_, werr := file.Write(atGood)
		cerr := file.Close()
		if werr != nil || cerr != nil {
			return Change{}, errors.Join(werr, cerr)
		}
		if _, err := wt.Add(f); err != nil {
			return Change{}, err
		}
	}
	commit, err := wt.Commit(commitMessage(req, files), &git.CommitOptions{
		Author: &object.Signature{Name: r.cfg.AuthorName, Email: r.cfg.AuthorEmail, When: time.Now()},
	})
	if err != nil {
		return Change{}, err
	}
	spec := gitconfig.RefSpec("refs/heads/" + branch + ":refs/heads/" + branch)
	if err := repo.PushContext(ctx, &git.PushOptions{RemoteName: "origin", Auth: auth, RefSpecs: []gitconfig.RefSpec{spec}}); err != nil {
		return Change{}, fmt.Errorf("push %s: %w", branch, err)
	}
	pr, err := r.openPull(ctx, req, branch, files)
	if err != nil {
		return Change{}, err
	}
	pr.Commit, pr.Files = commit.String(), files
	return pr, nil
}

// BranchName is the branch a decision's revert is pushed to: one per decision,
// so a retry finds its own work.
func BranchName(app, decisionID string) string {
	clean := func(s string) string {
		var b strings.Builder
		for _, r := range strings.ToLower(s) {
			switch {
			case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
				b.WriteRune(r)
			default:
				b.WriteRune('-')
			}
		}
		return strings.Trim(b.String(), "-")
	}
	return "chronicle/revert-" + clean(app) + "-" + clean(decisionID)
}

// commitOnBranch finds a revision and checks it is an ancestor of (or equal to)
// the given commit, so a revision from another branch is never used.
func commitOnBranch(repo *git.Repository, descendant *object.Commit, revision string) (*object.Commit, error) {
	c, err := repo.CommitObject(plumbing.NewHash(revision))
	if err != nil {
		return nil, refuse("revision %s is not in the repository", short(revision))
	}
	if c.Hash == descendant.Hash {
		return c, nil
	}
	ok, err := c.IsAncestor(descendant)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, refuse("revision %s is not an ancestor of %s on the branch", short(revision), short(descendant.Hash.String()))
	}
	return c, nil
}

// changedUnder lists the files under dir that differ between two commits.
func changedUnder(from, to *object.Commit, dir string) ([]string, error) {
	fromTree, err := from.Tree()
	if err != nil {
		return nil, err
	}
	toTree, err := to.Tree()
	if err != nil {
		return nil, err
	}
	changes, err := object.DiffTree(fromTree, toTree)
	if err != nil {
		return nil, err
	}
	prefix := strings.Trim(strings.TrimPrefix(dir, "./"), "/")
	seen := map[string]bool{}
	for _, ch := range changes {
		for _, name := range []string{ch.From.Name, ch.To.Name} {
			if name != "" && (prefix == "" || prefix == "." || name == prefix || strings.HasPrefix(name, prefix+"/")) {
				seen[name] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return out, nil
}

// content is a file's bytes at a commit, or nil when it does not exist there.
func content(c *object.Commit, name string) ([]byte, error) {
	f, err := c.File(name)
	if errors.Is(err, object.ErrFileNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r, err := f.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if b == nil {
		b = []byte{}
	}
	return b, nil
}

func equal(a, b []byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return string(a) == string(b)
}

func commitMessage(req Request, files []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Revert %s to %s\n\n", req.App, short(req.GoodRevision))
	fmt.Fprintf(&b, "Restores %d file(s) under %q to revision %s, the last one %s ran healthily,\nundoing revision %s.\n\n",
		len(files), displayPath(req.Path), req.GoodRevision, req.App, req.BadRevision)
	fmt.Fprintf(&b, "Chronicle-Decision: %s\n", req.DecisionID)
	for _, a := range req.Approvers {
		fmt.Fprintf(&b, "Chronicle-Approved-By: %s\n", a)
	}
	return b.String()
}

func pullBody(req Request, files []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Chronicle proposes reverting **%s** to revision `%s`, the last one it ran healthily.\n\n", req.App, short(req.GoodRevision))
	if req.Summary != "" {
		fmt.Fprintf(&b, "%s\n\n", req.Summary)
	}
	fmt.Fprintf(&b, "| | |\n|---|---|\n| Decision | `%s` |\n| Deployed when it broke | `%s` |\n| Restored to | `%s` |\n| Path | `%s` |\n| Approved in Chronicle by | %s |\n\n",
		req.DecisionID, req.BadRevision, req.GoodRevision, displayPath(req.Path), strings.Join(req.Approvers, ", "))
	if len(req.Evidence) > 0 {
		b.WriteString("**Why Chronicle thinks this caused the incident**\n\n")
		for _, e := range req.Evidence {
			fmt.Fprintf(&b, "- %s\n", e)
		}
		b.WriteString("\n")
	}
	b.WriteString("**Files restored**\n\n")
	for _, f := range files {
		fmt.Fprintf(&b, "- `%s`\n", f)
	}
	b.WriteString("\nNothing changes in the cluster until this is merged and the GitOps controller syncs it. Chronicle then checks that the workload recovered and the alert cleared.\n")
	return b.String()
}

func short(rev string) string {
	if len(rev) > 8 {
		return rev[:8]
	}
	return rev
}

func displayPath(p string) string {
	if strings.Trim(p, "./") == "" {
		return "/"
	}
	return p
}

func valueOr(v, fallback string) string {
	if v = strings.TrimSpace(v); v != "" {
		return v
	}
	return fallback
}

// normalize makes repository URLs comparable: scheme, case (GitHub, Gitea and
// Forgejo treat owner and repository names case-insensitively), a .git suffix
// and trailing slashes do not matter.
func normalize(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return strings.TrimSuffix(strings.TrimRight(raw, "/"), ".git")
	}
	return u.Host + strings.TrimSuffix(strings.TrimRight(u.Path, "/"), ".git")
}

// ownerRepo reads owner and name from a repository URL.
func ownerRepo(repoURL string) (string, string, error) {
	parts := strings.Split(strings.Trim(normalize(repoURL), "/"), "/")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("cannot read owner and repository from %q", repoURL)
	}
	return parts[len(parts)-2], parts[len(parts)-1], nil
}
