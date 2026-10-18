// Package auth identifies who is approving a healing action.
//
// An approver presents an OIDC ID token. Chronicle verifies it against the
// provider's published keys (signature, issuer, audience, expiry, age) and
// admits only people in a configured group or email list. The identity written
// to the audit trail comes from the verified token, never from the request body.
package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Identity is a verified approver.
type Identity struct {
	Issuer  string   `json:"issuer"`
	Subject string   `json:"subject"`
	Email   string   `json:"email,omitempty"`
	Name    string   `json:"name,omitempty"`
	Groups  []string `json:"groups,omitempty"`
}

// Display is the name recorded as the approver: the email when there is one.
func (i Identity) Display() string {
	if i.Email != "" {
		return i.Email
	}
	return i.Subject
}

var (
	// ErrUnauthenticated: no token, or one that does not verify.
	ErrUnauthenticated = errors.New("approver authentication required")
	// ErrTokenTooOld: a token that verifies but was issued longer ago than
	// OIDC_MAX_TOKEN_AGE. The approver only has to sign in again.
	ErrTokenTooOld = errors.New("sign-in is too old; sign in again")
	// ErrForbidden: a valid token for someone who may not approve.
	ErrForbidden = errors.New("not permitted to approve healing actions")
	// ErrProviderUnavailable: the identity provider cannot be reached, so
	// nothing can be verified. Approval fails closed.
	ErrProviderUnavailable = errors.New("identity provider unavailable")
)

// Authenticator turns a request into a verified approver.
type Authenticator interface {
	Authenticate(r *http.Request) (Identity, error)
}

// Config describes the identity provider and who may approve.
type Config struct {
	Issuer   string // OIDC_ISSUER_URL
	ClientID string // OIDC_CLIENT_ID: the audience tokens must carry

	// At least one of the two lists is required. Empty means nobody, never
	// everybody.
	AllowedGroups []string // OIDC_ALLOWED_GROUPS
	AllowedEmails []string // OIDC_ALLOWED_EMAILS
	GroupsClaim   string   // OIDC_GROUPS_CLAIM, default "groups"

	// MaxTokenAge bounds how old a token may be (by iat), so a token that
	// leaked long ago stops working even if the provider set a late expiry.
	MaxTokenAge time.Duration // OIDC_MAX_TOKEN_AGE, default 5m

	// Scopes and the endpoints are for the console's sign-in; the server only
	// verifies tokens.
	Scopes []string // OIDC_SCOPES

	// HTTPClient is used for discovery and key fetches (tests).
	HTTPClient *http.Client
}

// ConfigFromEnv reads the configuration. Configured is false when no issuer is
// set, which keeps the shared approval token as the only mechanism.
func ConfigFromEnv() (cfg Config, configured bool) {
	cfg = Config{
		Issuer:        strings.TrimSpace(os.Getenv("OIDC_ISSUER_URL")),
		ClientID:      strings.TrimSpace(os.Getenv("OIDC_CLIENT_ID")),
		AllowedGroups: splitList(os.Getenv("OIDC_ALLOWED_GROUPS")),
		AllowedEmails: splitList(os.Getenv("OIDC_ALLOWED_EMAILS")),
		GroupsClaim:   strings.TrimSpace(os.Getenv("OIDC_GROUPS_CLAIM")),
		Scopes:        splitList(os.Getenv("OIDC_SCOPES")),
	}
	if d, err := time.ParseDuration(os.Getenv("OIDC_MAX_TOKEN_AGE")); err == nil && d > 0 {
		cfg.MaxTokenAge = d
	}
	return cfg, cfg.Issuer != ""
}

func splitList(raw string) []string {
	var out []string
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// Validate rejects a configuration that would admit the wrong people.
func (c Config) Validate() error {
	if c.Issuer == "" {
		return errors.New("OIDC_ISSUER_URL is empty")
	}
	if !strings.HasPrefix(c.Issuer, "https://") && !isLocal(c.Issuer) {
		return fmt.Errorf("OIDC_ISSUER_URL %q must be https", c.Issuer)
	}
	if c.ClientID == "" {
		return errors.New("OIDC_CLIENT_ID is required: it is the audience tokens must be issued for")
	}
	if len(c.AllowedGroups) == 0 && len(c.AllowedEmails) == 0 {
		return errors.New("set OIDC_ALLOWED_GROUPS or OIDC_ALLOWED_EMAILS: with neither, nobody could approve")
	}
	return nil
}

// isLocal allows plain http only for a loopback provider: localhost,
// 127.0.0.1, or a *.localhost name (reserved for loopback, RFC 6761), for local
// development and the disposable test cluster.
func isLocal(issuer string) bool {
	u, err := url.Parse(issuer)
	if err != nil || u.Scheme != "http" {
		return false
	}
	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1" || strings.HasSuffix(host, ".localhost")
}

// OIDC verifies ID tokens from one provider.
type OIDC struct {
	cfg Config

	// Discovery is lazy: Chronicle starts, and every read-only feature works,
	// when the provider is down. Approval simply fails closed until it is back.
	mu       sync.Mutex
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	lastTry  time.Time

	now func() time.Time
}

// retryEvery limits discovery attempts while the provider is down.
const retryEvery = 5 * time.Second

// NewOIDC checks the configuration. It does not contact the provider.
func NewOIDC(cfg Config) (*OIDC, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.GroupsClaim == "" {
		cfg.GroupsClaim = "groups"
	}
	if cfg.MaxTokenAge <= 0 {
		cfg.MaxTokenAge = DefaultMaxTokenAge
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{"openid", "email", "profile", "groups"}
	}
	return &OIDC{cfg: cfg, now: time.Now}, nil
}

// Config returns the effective configuration.
func (o *OIDC) Config() Config { return o.cfg }

func (o *OIDC) ready(ctx context.Context) (*oidc.IDTokenVerifier, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.verifier != nil {
		return o.verifier, nil
	}
	if !o.lastTry.IsZero() && o.now().Sub(o.lastTry) < retryEvery {
		return nil, ErrProviderUnavailable
	}
	o.lastTry = o.now()
	if o.cfg.HTTPClient != nil {
		ctx = oidc.ClientContext(ctx, o.cfg.HTTPClient)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	provider, err := oidc.NewProvider(ctx, o.cfg.Issuer)
	if err != nil {
		slog.Warn("OIDC discovery failed; approvals are refused until it succeeds", "issuer", o.cfg.Issuer, "err", err)
		return nil, ErrProviderUnavailable
	}
	o.provider = provider
	// Expiry is checked here; the age bound and the allowlist below.
	o.verifier = provider.Verifier(&oidc.Config{
		ClientID:             o.cfg.ClientID,
		SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256, oidc.PS256},
	})
	return o.verifier, nil
}

// Endpoints are the provider's sign-in endpoints, for the console. It returns
// false while the provider cannot be reached.
func (o *OIDC) Endpoints(ctx context.Context) (authorize, token string, ok bool) {
	if _, err := o.ready(ctx); err != nil {
		return "", "", false
	}
	endpoint := o.provider.Endpoint()
	return endpoint.AuthURL, endpoint.TokenURL, true
}

// TokenFrom reads the presented token. X-Chronicle-Heal-Token is checked first
// because Authorization may carry the separate API token.
func TokenFrom(r *http.Request) string {
	if token := strings.TrimSpace(r.Header.Get("X-Chronicle-Heal-Token")); token != "" {
		return token
	}
	return strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
}

// Authenticate verifies the request's token and checks that its owner may
// approve.
func (o *OIDC) Authenticate(r *http.Request) (Identity, error) {
	raw := TokenFrom(r)
	if raw == "" {
		return Identity{}, ErrUnauthenticated
	}
	verifier, err := o.ready(r.Context())
	if err != nil {
		return Identity{}, err
	}
	ctx := r.Context()
	if o.cfg.HTTPClient != nil {
		ctx = oidc.ClientContext(ctx, o.cfg.HTTPClient)
	}
	token, err := verifier.Verify(ctx, raw)
	if err != nil {
		// A refusal says nothing about why; the log does.
		slog.Info("approver token rejected", "err", err)
		return Identity{}, ErrUnauthenticated
	}
	if age := o.now().Sub(token.IssuedAt); age > o.cfg.MaxTokenAge {
		slog.Info("approver token rejected", "reason", "too old", "age", age)
		return Identity{}, ErrTokenTooOld
	} else if token.IssuedAt.After(o.now().Add(time.Minute)) {
		slog.Info("approver token rejected", "reason", "issued in the future", "age", age)
		return Identity{}, ErrUnauthenticated
	}
	var claims map[string]any
	if err := token.Claims(&claims); err != nil {
		return Identity{}, ErrUnauthenticated
	}
	id := Identity{Issuer: token.Issuer, Subject: token.Subject, Groups: stringsClaim(claims[o.cfg.GroupsClaim])}
	id.Email, _ = claims["email"].(string)
	id.Name, _ = claims["name"].(string)
	if id.Subject == "" {
		return Identity{}, ErrUnauthenticated
	}
	// An email is a name the user may be able to choose, so it counts only
	// when the provider says it verified it.
	if verified, _ := claims["email_verified"].(bool); !verified {
		id.Email = ""
	}
	if !o.allowed(id) {
		slog.Warn("approver is not permitted", "subject", id.Subject, "email", id.Email)
		return id, ErrForbidden
	}
	return id, nil
}

// DefaultMaxTokenAge: an approval is made with a token issued in the last five
// minutes, so a stolen or forgotten session cannot approve for long.
const DefaultMaxTokenAge = 5 * time.Minute

// Permits says whether someone with these groups and (verified) email would be
// allowed to approve today. The execution worker uses it to re-check an approver
// who approved some minutes ago against the current allowlist.
func (o *OIDC) Permits(groups []string, email string) bool {
	return o.allowed(Identity{Groups: groups, Email: email})
}

func (o *OIDC) allowed(id Identity) bool {
	for _, want := range o.cfg.AllowedGroups {
		for _, have := range id.Groups {
			if have == want {
				return true
			}
		}
	}
	if id.Email != "" {
		for _, want := range o.cfg.AllowedEmails {
			if strings.EqualFold(want, id.Email) {
				return true
			}
		}
	}
	return false
}

// stringsClaim reads a claim that is a list of strings or one string.
func stringsClaim(v any) []string {
	switch value := v.(type) {
	case string:
		return splitList(value)
	case []any:
		var out []string
		for _, item := range value {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
