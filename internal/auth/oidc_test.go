package auth

import (
	"crypto/rsa"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/auth/authtest"
)

type claims = authtest.Claims

// idp adapts the shared fake provider to this file's shorter names.
type idp struct {
	*authtest.IDP
	srv        *httptest.Server
	key, other *rsa.PrivateKey
	down       *atomic.Bool
}

func newIDP(t *testing.T) *idp {
	p := authtest.New(t)
	return &idp{IDP: p, srv: p.Server, key: p.Key, other: p.Other, down: &p.Down}
}

func (p *idp) token(t *testing.T, key *rsa.PrivateKey, override claims) string {
	t.Helper()
	return p.Token(t, key, override)
}

func (p *idp) verifier(t *testing.T, mutate func(*Config)) *OIDC {
	t.Helper()
	cfg := Config{Issuer: p.srv.URL, ClientID: "chronicle", AllowedGroups: []string{"sre"}}
	if mutate != nil {
		mutate(&cfg)
	}
	o, err := NewOIDC(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func request(token string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/heal/actions/x/approve", nil)
	if token != "" {
		r.Header.Set("X-Chronicle-Heal-Token", token)
	}
	return r
}

func TestAMemberOfAnAllowedGroupIsAuthenticated(t *testing.T) {
	p := newIDP(t)
	id, err := p.verifier(t, nil).Authenticate(request(p.token(t, p.key, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if id.Subject != "user-1" || id.Email != "ana@example.com" || id.Issuer != p.srv.URL || id.Display() != "ana@example.com" {
		t.Fatalf("unexpected identity %+v", id)
	}
}

// Every way a token can be wrong is refused, and none of them as "forbidden":
// the caller learns nothing about why.
func TestTokensThatDoNotVerifyAreRefused(t *testing.T) {
	p := newIDP(t)
	o := p.verifier(t, nil)
	cases := []struct {
		name  string
		token string
	}{
		{"none", ""},
		{"garbage", "not-a-jwt"},
		{"signed by a key the provider does not publish", p.token(t, p.other, nil)},
		{"another audience", p.token(t, p.key, claims{"aud": "some-other-app"})},
		{"another issuer", p.token(t, p.key, claims{"iss": "https://evil.example"})},
		{"expired", p.token(t, p.key, claims{"iat": time.Now().Add(-2 * time.Hour).Unix(), "exp": time.Now().Add(-time.Hour).Unix()})},
		{"issued in the future", p.token(t, p.key, claims{"iat": time.Now().Add(time.Hour).Unix()})},
		{"alg none", p.UnsignedToken()},
		{"HMAC with the public key as the secret", p.HMACToken(t, []byte("0123456789abcdef0123456789abcdef"), nil)},
		{"no subject", p.token(t, p.key, claims{"sub": nil})},
	}
	for _, c := range cases {
		if _, err := o.Authenticate(request(c.token)); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("%s: got %v, want ErrUnauthenticated", c.name, err)
		}
	}
}

// An approval is made with a recent sign-in: five minutes by default. A token
// older than that is refused with its own error, so the console can ask the
// approver to sign in again rather than say "not authenticated".
func TestATokenOlderThanFiveMinutesIsRefusedAsTooOld(t *testing.T) {
	p := newIDP(t)
	o := p.verifier(t, nil)
	at := func(age time.Duration) string {
		return p.token(t, p.key, claims{"iat": time.Now().Add(-age).Unix(), "exp": time.Now().Add(time.Hour).Unix()})
	}
	if _, err := o.Authenticate(request(at(4 * time.Minute))); err != nil {
		t.Fatalf("a four-minute-old sign-in was refused: %v", err)
	}
	if _, err := o.Authenticate(request(at(6 * time.Minute))); !errors.Is(err, ErrTokenTooOld) {
		t.Fatalf("a six-minute-old sign-in: got %v, want ErrTokenTooOld", err)
	}
	if DefaultMaxTokenAge != 5*time.Minute {
		t.Fatalf("the default token age is %s", DefaultMaxTokenAge)
	}
}

// Permits is what the worker uses to re-check an approver against today's list.
func TestPermitsFollowsTheCurrentAllowlist(t *testing.T) {
	p := newIDP(t)
	o := p.verifier(t, nil)
	if !o.Permits([]string{"sre"}, "") {
		t.Fatal("a member of the approver group is not permitted")
	}
	if o.Permits([]string{"viewers"}, "") || o.Permits(nil, "") {
		t.Fatal("someone outside the allowlist is permitted")
	}
}

func TestAValidTokenFromSomeoneNotAllowedIsForbidden(t *testing.T) {
	p := newIDP(t)
	o := p.verifier(t, nil)
	_, err := o.Authenticate(request(p.token(t, p.key, claims{"groups": []string{"interns"}})))
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden", err)
	}
	if _, err := o.Authenticate(request(p.token(t, p.key, claims{"groups": nil}))); !errors.Is(err, ErrForbidden) {
		t.Fatalf("no groups: got %v, want ErrForbidden", err)
	}
}

// An email is a name the user may be able to pick, so it counts only if the
// provider verified it.
func TestAnEmailAllowlistNeedsAVerifiedEmail(t *testing.T) {
	p := newIDP(t)
	o := p.verifier(t, func(c *Config) { c.AllowedGroups = nil; c.AllowedEmails = []string{"Ana@Example.com"} })
	if _, err := o.Authenticate(request(p.token(t, p.key, claims{"groups": nil}))); err != nil {
		t.Fatalf("verified, case-insensitive match: %v", err)
	}
	if _, err := o.Authenticate(request(p.token(t, p.key, claims{"groups": nil, "email_verified": false}))); !errors.Is(err, ErrForbidden) {
		t.Fatalf("unverified email: got %v, want ErrForbidden", err)
	}
	if _, err := o.Authenticate(request(p.token(t, p.key, claims{"groups": nil, "email_verified": nil}))); !errors.Is(err, ErrForbidden) {
		t.Fatalf("no email_verified claim: got %v, want ErrForbidden", err)
	}
}

func TestAnUnverifiedEmailIsNotRecordedAsTheApprover(t *testing.T) {
	p := newIDP(t)
	id, err := p.verifier(t, nil).Authenticate(request(p.token(t, p.key, claims{"email": "ceo@example.com", "email_verified": false})))
	if err != nil {
		t.Fatal(err)
	}
	if id.Email != "" || id.Display() != "user-1" {
		t.Fatalf("an unverified email was trusted: %+v", id)
	}
}

func TestGroupsClaimIsConfigurableAndMayBeAString(t *testing.T) {
	p := newIDP(t)
	o := p.verifier(t, func(c *Config) { c.GroupsClaim = "roles"; c.AllowedGroups = []string{"approver"} })
	if _, err := o.Authenticate(request(p.token(t, p.key, claims{"roles": "viewer, approver"}))); err != nil {
		t.Fatalf("string claim: %v", err)
	}
	if _, err := o.Authenticate(request(p.token(t, p.key, nil))); !errors.Is(err, ErrForbidden) {
		t.Fatalf("the default groups claim must not be read: %v", err)
	}
}

func TestTheTokenMayArriveAsABearerToken(t *testing.T) {
	p := newIDP(t)
	r := request("")
	r.Header.Set("Authorization", "Bearer "+p.token(t, p.key, nil))
	if _, err := p.verifier(t, nil).Authenticate(r); err != nil {
		t.Fatal(err)
	}
}

// Nobody is admitted by default.
func TestConfigurationFailsClosed(t *testing.T) {
	good := Config{Issuer: "https://idp.example.com", ClientID: "c", AllowedGroups: []string{"sre"}}
	if _, err := NewOIDC(good); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"no client id":          func(c *Config) { c.ClientID = "" },
		"no one allowed":        func(c *Config) { c.AllowedGroups = nil },
		"plain http, not local": func(c *Config) { c.Issuer = "http://idp.example.com" },
		"no issuer":             func(c *Config) { c.Issuer = "" },
		"blank entries only":    func(c *Config) { c.AllowedGroups = splitList(" , ") },
	} {
		c := good
		mutate(&c)
		if _, err := NewOIDC(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The collector must start, and approvals must fail closed, while the
// provider is down; they work again once it is back.
func TestAnUnreachableProviderFailsClosedAndRecovers(t *testing.T) {
	p := newIDP(t)
	p.down.Store(true)
	o := p.verifier(t, nil)
	now := time.Now()
	o.now = func() time.Time { return now }
	token := p.token(t, p.key, nil)

	if _, err := o.Authenticate(request(token)); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("got %v, want ErrProviderUnavailable", err)
	}
	p.down.Store(false)
	if _, err := o.Authenticate(request(token)); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("retried before the back-off: %v", err)
	}
	now = now.Add(retryEvery + time.Second)
	if _, err := o.Authenticate(request(token)); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

func TestLoopbackIssuersMayUsePlainHTTP(t *testing.T) {
	for _, issuer := range []string{"http://localhost:8180/realms/x", "http://127.0.0.1:5556", "http://keycloak.localhost:8180/realms/chronicle"} {
		if _, err := NewOIDC(Config{Issuer: issuer, ClientID: "c", AllowedGroups: []string{"g"}}); err != nil {
			t.Errorf("%s: %v", issuer, err)
		}
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("OIDC_ISSUER_URL", "")
	if _, configured := ConfigFromEnv(); configured {
		t.Fatal("configured without an issuer")
	}
	t.Setenv("OIDC_ISSUER_URL", "https://idp.example.com")
	t.Setenv("OIDC_CLIENT_ID", "chronicle")
	t.Setenv("OIDC_ALLOWED_GROUPS", "sre, platform ,")
	t.Setenv("OIDC_ALLOWED_EMAILS", "a@example.com")
	t.Setenv("OIDC_MAX_TOKEN_AGE", "15m")
	cfg, configured := ConfigFromEnv()
	if !configured || cfg.ClientID != "chronicle" || len(cfg.AllowedGroups) != 2 || cfg.AllowedGroups[1] != "platform" ||
		cfg.AllowedEmails[0] != "a@example.com" || cfg.MaxTokenAge != 15*time.Minute {
		t.Fatalf("unexpected config %+v", cfg)
	}
}
