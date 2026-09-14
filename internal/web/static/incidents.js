/* Incidents as an operator surface: triage on the left, evidence and root
   cause on the right, keyboard first. Loaded after app.js, which it overrides
   the same way replay.js and graph.js do. */
(function () {
  const state = window.chronicleState;
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const el = s => document.querySelector(s);

  // --- turning raw source text into something triageable ------------------
  // Log lines arrive as whole formatted records. Rendering one as a headline
  // is what broke this page; the readable part is extracted for the row and
  // the untouched line is kept as evidence.
  const GLOG = /^[WEIF]\d{4}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\d+\s+[^\]]+\]\s*/;
  const LOGFMT_MSG = /\bmsg="((?:[^"\\]|\\.)*)"/;

  function readable(e) {
    let text = String(e.title || e.type || '').trim();
    const structured = text.match(LOGFMT_MSG);
    if (structured) text = structured[1].replace(/\\"/g, '"');
    text = text.replace(GLOG, '').replace(/\s+/g, ' ').trim();
    return text || e.type || '—';
  }

  function payload(e) {
    if (e.payload == null) return {};
    if (typeof e.payload === 'string') { try { return JSON.parse(e.payload); } catch (_) { return {}; } }
    return typeof e.payload === 'object' ? e.payload : {};
  }

  // A short, stable descriptor built from structured fields when they exist,
  // so the row says "OOMKilled exit 137" rather than 400 characters of log.
  function descriptor(e) {
    const p = payload(e);
    const parts = [];
    if (p.reason) parts.push(String(p.reason));
    if (p.exit_code !== undefined && p.exit_code !== null && p.exit_code !== 0) parts.push('exit ' + p.exit_code);
    if (p.container) parts.push('container=' + p.container);
    if (p.new_image) parts.push(String(p.new_image).split('/').pop());
    if (p.health) parts.push(String(p.health));
    return parts.join(' · ');
  }

  const target = e => `${e.namespace || 'cluster'}/${e.entity_name || '—'}`;
  const isCritical = e => e.severity === 'critical';
  const ts = v => new Date(v).getTime();

  const clock = v => {
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? '—' : d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
  };
  const stamp = v => {
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? '—' : d.toLocaleString([], {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit'});
  };
  const since = v => {
    const s = Math.max(0, (Date.now() - ts(v)) / 1000);
    if (s < 60) return Math.round(s) + 's';
    if (s < 3600) return Math.round(s / 60) + 'm';
    if (s < 86400) return Math.round(s / 3600) + 'h';
    return Math.round(s / 86400) + 'd';
  };

  // --- grouping ------------------------------------------------------------
  // One misbehaving component emits the same line hundreds of times. Triage
  // needs one row per distinct problem, with the repeat count as a signal.
  function identity(e) {
    const shape = readable(e)
      .replace(/\b\d{1,3}(?:\.\d{1,3}){3}\b/g, '<ip>')
      .replace(/\b[0-9a-f]{8,}\b/gi, '<hash>')
      .replace(/#\d+/g, '#n')
      .replace(/\b\d+\b/g, '<n>');
    return [e.namespace, e.entity_kind, e.entity_name, e.type, shape].join('|');
  }

  function group(events) {
    const groups = new Map();
    events.forEach(e => {
      const id = identity(e);
      const found = groups.get(id);
      if (found) {
        found.events.push(e);
        if (ts(e.ingested_at) > ts(found.latest.ingested_at)) found.latest = e;
        if (ts(e.ingested_at) < ts(found.first.ingested_at)) found.first = e;
        if (isCritical(e)) found.critical = true;
      } else {
        groups.set(id, {id, latest: e, first: e, events: [e], critical: isCritical(e)});
      }
    });
    return [...groups.values()].sort((a, b) => {
      if (a.critical !== b.critical) return a.critical ? -1 : 1;
      return ts(b.latest.ingested_at) - ts(a.latest.ingested_at);
    });
  }

  const severityFilter = () => state.opsSeverity || '';
  const queryFilter = () => (state.opsQuery || '').toLowerCase();

  function signals() {
    return (state.events || []).filter(e => {
      if (e.severity !== 'warning' && e.severity !== 'critical') return false;
      if (severityFilter() && e.severity !== severityFilter()) return false;
      const q = queryFilter();
      if (!q) return true;
      return (e.title + ' ' + e.type + ' ' + target(e) + ' ' + e.source).toLowerCase().includes(q);
    });
  }

  function selected(groups) {
    return groups.find(g => g.events.some(e => e.id === state.opsSelected)) || groups[0] || null;
  }

  // --- command bar ---------------------------------------------------------
  // Signal volume over the loaded window: the page is about time, so the time
  // axis belongs in the chrome, not in a separate view.
  function histogram(events) {
    const buckets = 44;
    if (!events.length) return `<div class="hist">${'<i></i>'.repeat(buckets)}</div>`;
    const times = events.map(e => ts(e.ingested_at));
    const start = Math.min(...times);
    const end = Math.max(Math.max(...times), start + 1);
    const counts = new Array(buckets).fill(0);
    const crit = new Array(buckets).fill(0);
    events.forEach(e => {
      const slot = Math.min(buckets - 1, Math.floor(((ts(e.ingested_at) - start) / (end - start)) * buckets));
      counts[slot]++;
      if (isCritical(e)) crit[slot]++;
    });
    const peak = Math.max(...counts, 1);
    return `<div class="hist" role="img" aria-label="Signal volume across the loaded window">${counts.map((n, i) =>
      `<i class="${n ? (crit[i] ? 'c' : 'w') : ''}" style="height:${n ? Math.max(8, Math.round((n / peak) * 100)) : 0}%" title="${n} signal(s)"></i>`).join('')}</div>
      <span class="ops-span">${events.length ? `${clock(start)} → ${clock(end)}` : 'no signals'}</span>`;
  }

  function bar(all, groups) {
    const severity = severityFilter();
    const button = (value, label, cls) =>
      `<button type="button" class="${cls}" data-sev="${value}" aria-pressed="${severity === value}">${label}</button>`;
    return `<div class="ops-bar">
      <input class="ops-find" id="ops-find" placeholder="filter  /" value="${esc(state.opsQuery || '')}" autocomplete="off" spellcheck="false">
      <div class="seg">${button('', 'ALL', '')}${button('critical', 'CRIT', 'crit')}${button('warning', 'WARN', 'warn')}</div>
      ${histogram(all)}
      <span class="ops-span">${groups.length} GROUP${groups.length === 1 ? '' : 'S'} · ${all.length} SIGNAL${all.length === 1 ? '' : 'S'}</span>
    </div>`;
  }

  // --- list ----------------------------------------------------------------
  function row(g, active) {
    const e = g.latest;
    const detail = descriptor(e);
    return `<button type="button" class="sig${g.critical ? ' crit' : ''}${active ? ' on' : ''}" data-sig="${esc(e.id)}">
      <span class="sig-top"><span class="sig-type">${esc(e.type)}</span>${g.events.length > 1 ? `<span class="sig-n">×${g.events.length}</span>` : ''}</span>
      <div class="sig-msg">${esc(detail || readable(e))}</div>
      <div class="sig-meta"><b>${esc(target(e))}</b><span>${esc(e.source)}</span><span>${since(e.latest || e.ingested_at)} ago</span></div>
    </button>`;
  }

  // --- detail --------------------------------------------------------------
  function keyValues(g) {
    const e = g.latest;
    const p = payload(e);
    const cells = [
      ['Severity', e.severity],
      ['Source', e.source],
      ['Kind', e.entity_kind || '—'],
      ['Occurrences', g.events.length],
      ['First seen', stamp(g.first.ingested_at)],
      ['Last seen', stamp(g.latest.ingested_at)],
    ];
    if (p.reason) cells.push(['Reason', p.reason]);
    if (p.exit_code !== undefined && p.exit_code !== null) cells.push(['Exit code', p.exit_code]);
    if (p.owner) cells.push(['Owner', p.owner]);
    if (p.ready_count !== undefined) cells.push(['Ready', p.ready_count]);
    return `<dl class="kv">${cells.map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join('')}</dl>`;
  }

  function occurrences(g) {
    if (g.events.length < 2) return '';
    const shown = g.events.slice().sort((a, b) => ts(b.ingested_at) - ts(a.ingested_at)).slice(0, 40);
    return `<section class="sec"><div class="sec-h"><b>Occurrences</b><span>${g.events.length} IN WINDOW</span></div>
      <div class="sec-body"><div class="occ">${shown.map(e => `<span class="${isCritical(e) ? 'crit' : ''}">${clock(e.ingested_at)}</span>`).join('')}${g.events.length > shown.length ? `<span>+${g.events.length - shown.length} more</span>` : ''}</div></div></section>`;
  }

  function remediation(g) {
    const actions = (state.actions || []).filter(a => g.events.some(e => e.id === a.incident_id));
    if (!actions.length) return '';
    return `<section class="sec"><div class="sec-h"><b>Remediation</b><span>${esc(actions[0].dry_run ? 'DRY RUN' : 'LIVE')}</span></div>
      <div class="sec-body">${actions.map(a => `
        <div style="display:flex;align-items:flex-start;gap:12px;padding-bottom:8px">
          <div style="flex:1;min-width:0">
            <div style="font:11px var(--mono);color:var(--text)">${esc(a.rule || a.action_type || 'no rule matched')}${a.target ? ` → ${esc(a.namespace || 'default')}/${esc(a.target)}` : ''}</div>
            <div style="font:10px var(--mono);color:var(--dim);margin-top:4px">${esc(a.status)} · ${esc(a.result || '')}</div>
          </div>
          ${a.approval === 'pending'
            ? `<div class="det-act"><button class="k pri" data-approve="${esc(a.id)}">Approve</button><button class="k no" data-deny="${esc(a.id)}">Deny</button></div>`
            : `<span style="font:10px var(--mono);color:var(--dim)">${esc(a.approval)}</span>`}
        </div>`).join('')}</div></section>`;
  }

  function rootCause(g) {
    const result = (state.opsRCA || {})[g.id];
    const body = result === 'loading'
      ? `<div class="none">Scanning the causal window and walking the dependency graph…</div>`
      : result && result.error
        ? `<div class="none warnline">${esc(result.error)}</div>`
        : result
          ? rcaBody(result)
          : `<div class="none">Not analyzed. <kbd>R</kbd> ranks graph-reachable causes in the window before this signal.</div>`;
    return `<section class="sec"><div class="sec-h"><b>Root cause</b><span>${result && result.scanned !== undefined ? `${result.scanned} EVENTS SCANNED` : 'POST /api/analyze'}</span></div>${body}</section>`;
  }

  function rcaBody(r) {
    const confidence = Number(r.confidence || 0);
    const pct = Math.round(confidence * 100);
    if (!r.candidates || !r.candidates.length) {
      return `<div class="sec-body"><div class="conf low"><span class="conf-n">—</span><span class="conf-t"><i style="width:0"></i></span><span class="conf-l">NO REACHABLE CAUSE</span></div>
        <p class="note">${esc(r.narrative || 'No upstream cause was found in the dependency graph for this signal.')}</p></div>`;
    }
    const rows = r.candidates.map((c, i) => {
      const gap = Math.round((ts(r.symptom.ingested_at) - ts(c.event.ingested_at)));
      return `<tr class="${i === 0 ? 'top' : ''}">
        <td class="num">${i + 1}</td>
        <td><span class="bar"><i style="width:${Math.round((c.score || 0) * 100)}%"></i></span>${(c.score || 0).toFixed(3)}</td>
        <td>${esc(c.event.type)}<div style="color:var(--dim);margin-top:3px">${esc(target(c.event))}</div>
          ${i === 0 && c.reasons ? `<ul class="why">${c.reasons.map(x => `<li>${esc(x)}</li>`).join('')}</ul>` : ''}</td>
        <td class="num">${Math.round(gap / 1000)}s</td>
        <td class="num">${c.distance}</td>
        <td class="num">${c.affected_services || 0}</td>
      </tr>`;
    }).join('');
    return `<div class="sec-body">
      <div class="conf${confidence < 0.5 ? ' low' : ''}">
        <span class="conf-n">${pct}%</span>
        <span class="conf-t"><i style="width:${pct}%"></i></span>
        <span class="conf-l">${confidence < 0.5 ? 'INCONCLUSIVE' : 'CONFIDENCE'} · BLAST RADIUS ${r.blast_radius ? r.blast_radius.affected_services : 0} SVC / ${r.blast_radius ? r.blast_radius.affected_nodes : 0} NODES</span>
      </div>
      <table class="cand"><thead><tr><th>#</th><th>Score</th><th>Cause · target · why</th><th class="num">Gap</th><th class="num">Hops</th><th class="num">Svc</th></tr></thead><tbody>${rows}</tbody></table>
      ${r.narrative ? `<p class="note" style="margin-top:11px">${esc(r.narrative)}</p>` : ''}
    </div>`;
  }

  function detail(g) {
    if (!g) return `<div class="none">No signal selected.</div>`;
    const e = g.latest;
    const raw = String(e.title || '').trim();
    const short = readable(e);
    const running = (state.opsRCA || {})[g.id] === 'loading';
    return `<div class="det-head">
        <div class="det-id">
          <div class="det-type">${esc(e.type)}</div>
          <div class="det-target">${esc(target(e))} · ${esc(e.entity_kind || 'Resource')} · ${esc(descriptor(e) || short).slice(0, 160)}</div>
        </div>
        <div class="det-act"><button class="k pri" id="ops-rca" data-rca="${esc(e.id)}" ${running ? 'disabled' : ''}>${running ? 'Analyzing…' : 'Run RCA'}</button></div>
      </div>
      ${keyValues(g)}
      ${raw && raw !== short ? `<section class="sec"><div class="sec-h"><b>Raw record</b><span>${esc(e.source.toUpperCase())} · ${esc(e.id)}</span></div><pre class="raw">${esc(raw)}</pre></section>` : ''}
      ${rootCause(g)}
      ${remediation(g)}
      ${occurrences(g)}`;
  }

  function view() {
    const all = signals();
    const groups = group(all);
    const active = selected(groups);
    if (active) state.opsSelected = active.latest.id;
    return `<div class="ops">
      ${bar(all, groups)}
      <div class="ops-body">
        <div class="ops-list" id="ops-list">${groups.length ? groups.map(g => row(g, active && g.id === active.id)).join('') : `<div class="none">No warning or critical signals in the loaded window.</div>`}</div>
        <div class="ops-detail" id="ops-detail">${detail(active)}</div>
      </div>
      <div class="ops-foot">
        <span><kbd>j</kbd><kbd>k</kbd> move</span>
        <span><kbd>r</kbd> run RCA</span>
        <span><kbd>/</kbd> filter</span>
        <span><kbd>esc</kbd> clear</span>
        <span style="margin-left:auto">${esc(state.totalEvents || (state.events || []).length)} events loaded · last 24h</span>
      </div>
    </div>`;
  }

  // --- behaviour -----------------------------------------------------------
  async function analyze(id) {
    const groups = group(signals());
    const g = selected(groups);
    if (!g) return;
    state.opsRCA = state.opsRCA || {};
    state.opsRCA[g.id] = 'loading';
    window.render();
    try {
      const response = await window.chronicleAPI(`/api/analyze?event_id=${encodeURIComponent(id)}`, {method: 'POST'});
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Analysis failed');
      state.opsRCA[g.id] = data;
      if (data.action && window.loadActions) await window.loadActions();
    } catch (err) {
      state.opsRCA[g.id] = {error: err.message};
    }
    window.render();
  }

  function move(step) {
    const groups = group(signals());
    if (!groups.length) return;
    const current = groups.findIndex(x => x.events.some(e => e.id === state.opsSelected));
    const next = Math.max(0, Math.min(groups.length - 1, (current < 0 ? 0 : current) + step));
    state.opsSelected = groups[next].latest.id;
    window.render();
    const node = el('.sig.on');
    if (node) node.scrollIntoView({block: 'nearest'});
  }

  function bind() {
    const find = el('#ops-find');
    if (find) {
      find.oninput = () => {
        state.opsQuery = find.value;
        const at = find.selectionStart;
        window.render();
        const again = el('#ops-find');
        if (again) { again.focus(); again.setSelectionRange(at, at); }
      };
    }
    document.querySelectorAll('[data-sev]').forEach(button => button.onclick = () => {
      state.opsSeverity = button.dataset.sev;
      window.render();
    });
    document.querySelectorAll('[data-sig]').forEach(button => button.onclick = () => {
      state.opsSelected = button.dataset.sig;
      window.render();
    });
    const run = el('[data-rca]');
    if (run) run.onclick = () => analyze(run.dataset.rca);
  }

  if (!window.__opsKeys) {
    document.addEventListener('keydown', event => {
      if (state.page !== 'incidents') return;
      const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(event.target.tagName);
      if (event.key === 'Escape' && typing) { event.target.blur(); return; }
      if (typing || event.metaKey || event.ctrlKey || event.altKey) return;
      if (event.key === 'j') { event.preventDefault(); move(1); }
      else if (event.key === 'k') { event.preventDefault(); move(-1); }
      else if (event.key === 'r') { const run = el('[data-rca]'); if (run && !run.disabled) { event.preventDefault(); analyze(run.dataset.rca); } }
      else if (event.key === '/') { const find = el('#ops-find'); if (find) { event.preventDefault(); find.focus(); find.select(); } }
      else if (event.key === 'Escape') { state.opsQuery = ''; state.opsSeverity = ''; window.render(); }
    });
    window.__opsKeys = true;
  }

  window.incidents = view;
  const previousBind = window.bindView;
  window.bindView = function () {
    if (previousBind) previousBind();
    if (state.page === 'incidents') bind();
  };
  if (state.page === 'incidents' && window.render) window.render();
}());
