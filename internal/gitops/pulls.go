package gitops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// The pull-request API that GitHub, Gitea and Forgejo share: the same paths, the
// same request fields and the same response fields for what Chronicle reads.

type pull struct {
	Number         int    `json:"number"`
	HTMLURL        string `json:"html_url"`
	State          string `json:"state"`
	Merged         bool   `json:"merged"`
	MergeCommitSHA string `json:"merge_commit_sha"`
	MergedAt       string `json:"merged_at"`
	Head           struct {
		Ref string `json:"ref"`
	} `json:"head"`
}

func (r *Remediator) api(ctx context.Context, method, repoURL, suffix string, body any, out any) error {
	owner, name, err := ownerRepo(repoURL)
	if err != nil {
		return err
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s/repos/%s/%s%s", r.cfg.APIURL, owner, name, suffix), reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "token "+r.cfg.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.cfg.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		// The body can echo the request; only the status is reported.
		return fmt.Errorf("%s %s: HTTP %d", method, suffix, resp.StatusCode)
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func (r *Remediator) openPull(ctx context.Context, req Request, branch string, files []string) (Change, error) {
	var p pull
	err := r.api(ctx, http.MethodPost, req.RepoURL, "/pulls", map[string]string{
		"title": fmt.Sprintf("Chronicle: revert %s to %s", req.App, short(req.GoodRevision)),
		"head":  branch,
		"base":  req.Branch,
		"body":  pullBody(req, files),
	}, &p)
	if err != nil {
		return Change{}, fmt.Errorf("open pull request: %w", err)
	}
	return Change{URL: p.HTMLURL, Number: p.Number, Branch: branch}, nil
}

// findOpenPull finds an open pull request from the branch, so a retry does not
// open a second one.
func (r *Remediator) findOpenPull(ctx context.Context, repoURL, branch string) (Change, bool, error) {
	for page := 1; page <= 5; page++ {
		var pulls []pull
		if err := r.api(ctx, http.MethodGet, repoURL, fmt.Sprintf("/pulls?state=open&per_page=50&limit=50&page=%d", page), nil, &pulls); err != nil {
			return Change{}, false, fmt.Errorf("list pull requests: %w", err)
		}
		for _, p := range pulls {
			if p.Head.Ref == branch {
				return Change{URL: p.HTMLURL, Number: p.Number, Branch: branch}, true, nil
			}
		}
		if len(pulls) < 50 {
			break
		}
	}
	return Change{}, false, nil
}

// PullStatus reads where a pull request stands.
func (r *Remediator) PullStatus(ctx context.Context, repoURL string, number int) (Status, error) {
	var p pull
	if err := r.api(ctx, http.MethodGet, repoURL, fmt.Sprintf("/pulls/%d", number), nil, &p); err != nil {
		return Status{}, err
	}
	switch {
	case p.Merged:
		at, _ := time.Parse(time.RFC3339, p.MergedAt)
		return Status{State: "merged", MergeCommit: p.MergeCommitSHA, MergedAt: at}, nil
	case p.State == "closed":
		return Status{State: "closed"}, nil
	}
	return Status{State: "open"}, nil
}
