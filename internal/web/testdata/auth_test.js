// Run by auth_js_test.go: node auth_test.js <path to auth.js>
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const source = fs.readFileSync(process.argv[2], 'utf8');

function browser({ search = '', hash = '#incidents' } = {}) {
  const session = new Map(), calls = { assign: [], replace: [], fetch: [] };
  const window = {
    sessionStorage: { getItem: k => session.has(k) ? session.get(k) : null, setItem: (k, v) => session.set(k, String(v)), removeItem: k => session.delete(k) },
    location: { origin: 'https://chronicle.example', pathname: '/', search, hash, assign: u => calls.assign.push(u) },
    history: { replaceState: (_s, _t, url) => calls.replace.push(url) },
    crypto: globalThis.crypto,
  };
  const sandbox = { window, btoa, atob, escape, URLSearchParams, TextEncoder, URL, JSON, Date, Math, Uint8Array, Error, String, Promise, console,
    decodeURIComponent, fetch: undefined };
  vm.createContext(sandbox);
  vm.runInContext(source, sandbox);
  return { auth: window.ChronicleAuth, window, session, calls, sandbox };
}

const jwt = claims => 'e30.' + Buffer.from(JSON.stringify(claims)).toString('base64url') + '.sig';
const cfg = { mode: 'oidc', client_id: 'chronicle', scopes: ['openid', 'email'], authorization_endpoint: 'https://idp.example/authorize', token_endpoint: 'https://idp.example/token' };
const api = async () => ({ ok: true, json: async () => cfg });

(async () => {
  // RFC 7636 appendix B.
  {
    const { auth } = browser();
    assert.strictEqual(await auth.challengeFor('dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk'), 'E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM');
  }

  // The sign-in request is PKCE, carries state and nonce, and keeps the verifier private.
  {
    const { auth, calls, session } = browser();
    await auth.login(cfg);
    const url = new URL(calls.assign[0]);
    const q = url.searchParams;
    assert.strictEqual(url.origin + url.pathname, 'https://idp.example/authorize');
    assert.strictEqual(q.get('response_type'), 'code');
    assert.strictEqual(q.get('client_id'), 'chronicle');
    assert.strictEqual(q.get('redirect_uri'), 'https://chronicle.example/');
    assert.strictEqual(q.get('code_challenge_method'), 'S256');
    assert.ok(q.get('state') && q.get('nonce') && q.get('code_challenge'));
    const flow = JSON.parse(session.get('chronicle_oidc_flow'));
    assert.ok(!calls.assign[0].includes(flow.verifier), 'the verifier must not be sent to the provider');
    assert.strictEqual(q.get('code_challenge'), await auth.challengeFor(flow.verifier));
  }

  // A good return: code exchanged with the verifier, token stored, URL cleaned.
  {
    const first = browser();
    await first.auth.login(cfg);
    const flow = JSON.parse(first.session.get('chronicle_oidc_flow'));
    const b = browser({ search: `?code=abc&state=${flow.state}` });
    b.session.set('chronicle_oidc_flow', JSON.stringify(flow));
    const idToken = jwt({ email: 'ana@example.com', nonce: flow.nonce, exp: Math.floor(Date.now() / 1000) + 600 });
    b.sandbox.fetch = async (url, options) => { b.calls.fetch.push([url, options]); return { ok: true, json: async () => ({ id_token: idToken }) }; };
    assert.strictEqual(await b.auth.finishLogin(api), 'ana@example.com');
    const [url, options] = b.calls.fetch[0];
    assert.strictEqual(url, 'https://idp.example/token');
    const form = new URLSearchParams(options.body);
    assert.strictEqual(form.get('code_verifier'), flow.verifier);
    assert.strictEqual(form.get('code'), 'abc');
    assert.strictEqual(b.auth.token(), idToken);
    assert.deepStrictEqual(b.calls.replace, ['/#incidents']);
    assert.strictEqual(b.session.has('chronicle_oidc_flow'), false, 'the one-time flow state must be consumed');
  }

  // A return that does not match this tab is refused before any exchange.
  {
    const b = browser({ search: '?code=abc&state=forged' });
    b.session.set('chronicle_oidc_flow', JSON.stringify({ verifier: 'v', state: 'real', nonce: 'n', hash: '' }));
    b.sandbox.fetch = async () => { throw new Error('must not be called'); };
    await assert.rejects(() => b.auth.finishLogin(api), /could not be matched/);
    assert.strictEqual(b.auth.token(), '');
  }

  // A token whose nonce is not ours is not kept.
  {
    const b = browser({ search: '?code=abc&state=s' });
    b.session.set('chronicle_oidc_flow', JSON.stringify({ verifier: 'v', state: 's', nonce: 'mine', hash: '' }));
    b.sandbox.fetch = async () => ({ ok: true, json: async () => ({ id_token: jwt({ nonce: 'theirs', exp: Math.floor(Date.now() / 1000) + 600 }) }) });
    await assert.rejects(() => b.auth.finishLogin(api), /did not match/);
    assert.strictEqual(b.auth.token(), '');
  }

  // A refused sign-in and a plain page load.
  {
    const b = browser({ search: '?error=access_denied' });
    await assert.rejects(() => b.auth.finishLogin(api), /access_denied/);
    assert.strictEqual(await browser().auth.finishLogin(api), '');
  }

  // An expired (or nearly expired) token counts as signed out.
  {
    const b = browser();
    b.session.set('chronicle_oidc_id_token', jwt({ email: 'a@b.c', exp: Math.floor(Date.now() / 1000) - 5 }));
    assert.strictEqual(b.auth.token(), '');
    assert.strictEqual(b.session.has('chronicle_oidc_id_token'), false);
    b.session.set('chronicle_oidc_id_token', jwt({ email: 'a@b.c', exp: Math.floor(Date.now() / 1000) + 10 }));
    assert.strictEqual(b.auth.token(), '', 'a token about to expire would be refused by the server');
  }

  // The provider being down is an error, not a silent no-op.
  {
    const b = browser();
    await assert.rejects(() => b.auth.login({ ...cfg, authorization_endpoint: undefined }), /unreachable/);
  }
  console.log('auth.js ok');
})().catch(e => { console.error(e); process.exit(1); });
