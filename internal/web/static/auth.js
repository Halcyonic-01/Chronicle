// Approver sign-in: OpenID Connect authorization-code flow with PKCE, as a
// public client. The ID token is kept for this browser tab only (sessionStorage)
// and sent with approvals; the server verifies it, this file never trusts it.
(function (root) {
  const KEY = { token: 'chronicle_oidc_id_token', flow: 'chronicle_oidc_flow' };
  const store = () => { try { return root.sessionStorage; } catch (_) { return null; } };
  const read = k => { const s = store(); try { return s ? s.getItem(k) : null; } catch (_) { return null; } };
  const write = (k, v) => { const s = store(); try { if (s) s.setItem(k, v); } catch (_) {} };
  const drop = k => { const s = store(); try { if (s) s.removeItem(k); } catch (_) {} };

  const base64url = bytes => btoa(String.fromCharCode(...new Uint8Array(bytes)))
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  const random = (n = 32) => base64url(root.crypto.getRandomValues(new Uint8Array(n)));
  const challengeFor = async verifier =>
    base64url(await root.crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier)));

  // Reads a JWT's claims for display and expiry only.
  function claimsOf(jwt) {
    try {
      const part = jwt.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
      return JSON.parse(decodeURIComponent(escape(atob(part))));
    } catch (_) { return null; }
  }

  let configPromise = null;
  // fetcher is the console's api(), so an API token is still honoured.
  function config(fetcher) {
    if (!configPromise) {
      // Only a real answer is cached; a failed fetch is asked again next time.
      configPromise = fetcher('/api/auth/config').then(r => {
        if (r.ok) return r.json();
        configPromise = null;
        return { mode: 'shared_token' };
      }).catch(() => { configPromise = null; return { mode: 'shared_token' }; });
    }
    return configPromise;
  }

  const redirectUri = () => root.location.origin + '/';

  // A token near expiry is as good as none: the approval would be refused.
  function token() {
    const jwt = read(KEY.token);
    const claims = jwt && claimsOf(jwt);
    if (!claims || !claims.exp || claims.exp * 1000 < Date.now() + 30000) { drop(KEY.token); return ''; }
    return jwt;
  }

  function signedInAs() {
    const claims = claimsOf(token() || '');
    return claims ? (claims.email || claims.name || claims.sub) : '';
  }

  function signOut() { drop(KEY.token); }

  async function login(cfg) {
    if (!cfg.authorization_endpoint) throw new Error('The identity provider is unreachable');
    const flow = { verifier: random(48), state: random(), nonce: random(), hash: root.location.hash };
    write(KEY.flow, JSON.stringify(flow));
    const query = new URLSearchParams({
      response_type: 'code', client_id: cfg.client_id, redirect_uri: redirectUri(),
      scope: (cfg.scopes || ['openid']).join(' '), state: flow.state, nonce: flow.nonce,
      code_challenge: await challengeFor(flow.verifier), code_challenge_method: 'S256',
    });
    root.location.assign(cfg.authorization_endpoint + (cfg.authorization_endpoint.includes('?') ? '&' : '?') + query);
  }

  // Run once at start-up. Returns the signed-in name, '' when this page load is
  // not a sign-in return, and throws with a reason when the return is bad.
  async function finishLogin(fetcher) {
    const params = new URLSearchParams(root.location.search);
    if (!params.has('code') && !params.has('error')) return '';
    const flow = JSON.parse(read(KEY.flow) || 'null');
    drop(KEY.flow);
    root.history.replaceState(null, '', root.location.pathname + (flow && flow.hash ? flow.hash : ''));
    if (params.has('error')) throw new Error('Sign-in was refused: ' + params.get('error'));
    if (!flow || params.get('state') !== flow.state) throw new Error('Sign-in could not be matched to this browser tab');
    const cfg = await config(fetcher);
    if (!cfg.token_endpoint) throw new Error('The identity provider is unreachable');
    const response = await fetch(cfg.token_endpoint, {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ grant_type: 'authorization_code', code: params.get('code'),
        redirect_uri: redirectUri(), client_id: cfg.client_id, code_verifier: flow.verifier }),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || !body.id_token) throw new Error('Sign-in failed: ' + (body.error || response.status));
    const claims = claimsOf(body.id_token);
    if (!claims || claims.nonce !== flow.nonce) throw new Error('Sign-in response did not match its request');
    write(KEY.token, body.id_token);
    return signedInAs();
  }

  root.ChronicleAuth = { config, token, signedInAs, signOut, login, finishLogin, base64url, challengeFor, claimsOf };
})(typeof window !== 'undefined' ? window : globalThis);
