// Package authtest is a minimal OpenID provider for tests: discovery, keys, and
// signed ID tokens. It is not for use outside tests.
package authtest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

type IDP struct {
	Server *httptest.Server
	Key    *rsa.PrivateKey // published
	Other  *rsa.PrivateKey // not published
	Down   atomic.Bool
}

// Claims overrides the defaults of Token; a nil value removes a claim.
type Claims map[string]any

func New(t *testing.T) *IDP {
	t.Helper()
	p := &IDP{}
	var err error
	if p.Key, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
		t.Fatal(err)
	}
	if p.Other, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		if p.Down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"issuer": p.Server.URL, "jwks_uri": p.Server.URL + "/keys",
			"authorization_endpoint": p.Server.URL + "/authorize", "token_endpoint": p.Server.URL + "/token",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
			{Key: &p.Key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}})
	})
	p.Server = httptest.NewServer(mux)
	t.Cleanup(p.Server.Close)
	return p
}

func (p *IDP) URL() string { return p.Server.URL }

// Token signs a token for audience "chronicle", subject "user-1", group "sre",
// valid for ten minutes, unless override says otherwise.
func (p *IDP) Token(t *testing.T, key *rsa.PrivateKey, override Claims) string {
	t.Helper()
	now := time.Now()
	c := Claims{
		"iss": p.Server.URL, "aud": "chronicle", "sub": "user-1",
		"iat": now.Unix(), "exp": now.Add(10 * time.Minute).Unix(),
		"email": "ana@example.com", "email_verified": true, "groups": []string{"sre"},
	}
	for k, v := range override {
		if v == nil {
			delete(c, k)
			continue
		}
		c[k] = v
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jwt.Signed(signer).Claims(map[string]any(c)).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// HMACToken is signed with a shared secret, the way an attacker would try to
// pass off a token using the provider's public key as the secret.
func (p *IDP) HMACToken(t *testing.T, secret []byte, c Claims) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.HS256, Key: secret},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	full := map[string]any{"iss": p.Server.URL, "aud": "chronicle", "sub": "user-1",
		"iat": now.Unix(), "exp": now.Add(time.Minute).Unix(), "groups": []string{"sre"}}
	for k, v := range c {
		full[k] = v
	}
	raw, err := jwt.Signed(signer).Claims(full).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// UnsignedToken has "alg":"none" and no signature.
func (p *IDP) UnsignedToken() string {
	enc := base64.RawURLEncoding.EncodeToString
	header := enc([]byte(`{"alg":"none","typ":"JWT"}`))
	body := enc([]byte(fmt.Sprintf(`{"iss":%q,"aud":"chronicle","sub":"user-1","iat":%d,"exp":%d,"groups":["sre"]}`,
		p.Server.URL, time.Now().Unix(), time.Now().Add(time.Minute).Unix())))
	return header + "." + body + "."
}
