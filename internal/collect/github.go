package collect

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/go-github/v60/github"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

type GitHubCollector struct {
	BaseCollector
	client   *github.Client
	owner    string
	repo     string
	lastSeen time.Time
}

func NewGitHubCollector(client *github.Client, owner, repo string, out chan<- event.Event) *GitHubCollector {
	return &GitHubCollector{
		BaseCollector: BaseCollector{Out: out},
		client:        client,
		owner:         owner,
		repo:          repo,
		lastSeen:      time.Now().Add(-24 * time.Hour), // look back 1 day initially
	}
}

func (g *GitHubCollector) Run(ctx context.Context) error {
	interval := 30 * time.Second
	if raw := os.Getenv("GITHUB_POLL_INTERVAL"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			interval = parsed
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			commits, _, err := g.client.Repositories.ListCommits(ctx, g.owner, g.repo, &github.CommitsListOptions{
				Since: g.lastSeen,
			})
			if err != nil {
				// GitHub is an optional source; transient failures must not stop K8s ingestion.
				continue // transient error
			}

			// Process backwards to keep chronological order
			for i := len(commits) - 1; i >= 0; i-- {
				c := commits[i]

				// Ensure we don't process the exact same commit again due to precision issues
				if c.Commit.Author.GetDate().Time.After(g.lastSeen) {
					g.Emit(g.commitToEvent(c))
					g.lastSeen = c.Commit.Author.GetDate().Time
				}
			}
		}
	}
}

func (g *GitHubCollector) commitToEvent(c *github.RepositoryCommit) event.Event {
	// We need to fetch the full commit to get files modified
	// In a real implementation we'd do a GetCommit here, but for now we'll construct the event

	msg := c.Commit.GetMessage()
	title := strings.Split(msg, "\n")[0]

	return event.Event{
		Source:     "github",
		EntityKind: "Repository",
		EntityName: g.repo,
		Namespace:  "default",
		Type:       "commit",
		Severity:   "info",
		OccurredAt: c.Commit.Author.GetDate().Time,
		Title:      fmt.Sprintf("%s: %s", c.GetSHA()[:7], title),
		Payload: mustJSON(map[string]any{
			"commit_sha": c.GetSHA(),
			"author":     c.Author.GetLogin(),
			// In full implementation, fetch modified files and check touches_infra
		}),
	}
}
