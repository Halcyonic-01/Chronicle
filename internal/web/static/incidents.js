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

  // Namespace classification is shared with the graph and posture views.
  const scopeOf = e => window.chronicleScopeOf(e.namespace);
  const triageScope = () => state.opsScope || 'application';

  const severityFilter = () => state.opsSeverity || '';
  const queryFilter = () => (state.opsQuery || '').toLowerCase();

  function signals() {
    return (state.events || []).filter(e => {
      if (e.severity !== 'warning' && e.severity !== 'critical') return false;
      if (triageScope() !== 'all' && scopeOf(e) !== triageScope()) return false;
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
      <div class="seg">${['application', 'platform', 'all'].map(scope =>
        `<button type="button" data-scope="${scope}" aria-pressed="${triageScope() === scope}">${scope.toUpperCase()}</button>`).join('')}</div>
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
          ${a.approval === 'pending' && a.status === 'would_run'
            ? `<div class="det-act"><button class="k pri" data-approve="${esc(a.id)}">Approve</button><button class="k no" data-deny="${esc(a.id)}">Deny</button></div>`
            : `<span class="gate">${esc(a.status === 'blocked' ? 'blocked by safety gate' : a.approval)}</span>`}
        </div>`).join('')}</div></section>`;
  }

  function rootCause(g) {
    const result = (state.opsRCA || {})[g.id];
    const body = result === 'loading'
      ? `<div class="none">Scanning the causal window and walking the dependency graph…</div>`
      : result && result.error
        ? `<div class="none warnline">${esc(result.error)}</div>`
        : result
          ? rcaBody(result, Math.min(focusIndex(g), (result.candidates || []).length - 1))
          : `<div class="none">Not analyzed. <kbd>R</kbd> ranks graph-reachable causes in the window before this signal.</div>`;
    return `<section class="sec"><div class="sec-h"><b>Root cause</b><span>${result && result.scanned !== undefined ? `${result.scanned} EVENTS SCANNED` : 'POST /api/analyze'}</span></div>${body}</section>`;
  }

  function rcaBody(r, focus) {
    const confidence = Number(r.confidence || 0);
    const pct = Math.round(confidence * 100);
    if (!r.candidates || !r.candidates.length) {
      return `<div class="sec-body"><div class="conf low"><span class="conf-n">—</span><span class="conf-t"><i style="width:0"></i></span><span class="conf-l">NO REACHABLE CAUSE</span></div>
        <p class="note">${esc(r.narrative || 'No upstream cause was found in the dependency graph for this signal.')}</p></div>`;
    }
    const rows = r.candidates.map((c, i) => {
      const gap = Math.round((ts(r.symptom.ingested_at) - ts(c.event.ingested_at)));
      return `<tr class="${i === focus ? 'top' : ''}" data-cand="${i}">
        <td class="num">${i + 1}</td>
        <td><span class="bar"><i style="width:${Math.round((c.score || 0) * 100)}%"></i></span>${(c.score || 0).toFixed(3)}</td>
        <td>${esc(c.event.type)}<div style="color:var(--dim);margin-top:3px">${esc(target(c.event))}</div></td>
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
      <table class="cand"><thead><tr><th>#</th><th>Score</th><th>Cause · target</th><th class="num">Gap</th><th class="num">Hops</th><th class="num">Svc</th></tr></thead><tbody>${rows}</tbody></table>
      ${derivation(r.candidates[focus] || r.candidates[0])}
      ${r.narrative ? `<div class="narr"><span class="narr-l">Narrative</span><p class="note">${esc(r.narrative)}</p></div>` : ''}
    </div>`;
  }

  // The score is a product of one base weight and three multipliers. Showing it
  // as an aligned derivation makes it obvious which factor moved the number —
  // a sentence per step cannot do that.
  function derivation(top) {
    if (!top) return '';
    const factors = top.factors || [];
    if (!factors.length) {
      return top.reasons && top.reasons.length
        ? `<div class="deriv"><div class="deriv-h">Score derivation</div><div class="deriv-fallback">${top.reasons.map(x => `<div>${esc(x)}</div>`).join('')}</div></div>`
        : '';
    }
    // 1.5 is the widest multiplier the scoring can produce, so the tick at
    // two thirds marks "no effect" on every row.
    const scale = m => Math.max(0, Math.min(100, (m / 1.5) * 100));
    const rows = factors.map(f => {
      const direction = f.base ? 'base' : (f.multiplier > 1.001 ? 'up' : (f.multiplier < 0.999 ? 'down' : 'flat'));
      return `<tr>
        <td class="lbl">${esc(f.label)}</td>
        <td class="det">${esc(f.detail)}</td>
        <td class="mul ${direction}"><span class="meter"><i style="width:${scale(f.multiplier)}%"></i></span>${f.base ? '' : '×'}${Number(f.multiplier).toFixed(2)}</td>
      </tr>`;
    }).join('');
    return `<div class="deriv">
      <div class="deriv-h">Score derivation<span>TOP CANDIDATE · ${esc(top.event.type)}</span></div>
      <table><tbody>${rows}
        <tr class="total"><td class="lbl">Score</td><td class="det">base × each factor</td><td class="mul">${Number(top.score || 0).toFixed(3)}</td></tr>
      </tbody></table>
    </div>`;
  }

  /* --- impact: who is affected, and how the cause reaches them ----------- */

  const nodeKey = n => `${n?.Namespace ?? n?.namespace ?? ''}/${n?.Kind ?? n?.kind ?? ''}/${n?.Name ?? n?.name ?? ''}`;
  const shortName = (name, max = 22) => name.length > max ? name.slice(0, max - 1) + '\u2026' : name;
  const parseKey = k => { const parts = String(k).split('/'); return {namespace: parts[0] || '', kind: parts[1] || '', name: parts.slice(2).join('/') || ''}; };
  const focusIndex = g => Number((state.opsCandidateBy || {})[g.id] || 0);

  // Upstream hop distance from the symptom, over the evidence subgraph.
  // Anything the walk never reaches sits downstream of the symptom.
  // Causality runs backwards along ownership and forwards along dependencies —
  // the same rule the analyzer walks. Anything a step never reaches sits
  // downstream of the symptom rather than upstream of it.
  const CAUSAL_WITH_EDGE = new Set(['owns']);

  function causalSteps(edges) {
    const step = new Map();
    const add = (node, next, kind) => { if (!step.has(node)) step.set(node, []); step.get(node).push({next, kind}); };
    edges.forEach(e => {
      const from = nodeKey(e.From), to = nodeKey(e.To);
      if (CAUSAL_WITH_EDGE.has(e.Kind)) add(to, from, e.Kind);
      else add(from, to, e.Kind);
    });
    return step;
  }

  function layersFrom(edges, symptomKey) {
    const step = causalSteps(edges);
    const layer = new Map([[symptomKey, 0]]);
    let frontier = [symptomKey];
    for (let depth = 1; depth <= 6 && frontier.length; depth++) {
      const next = [];
      frontier.forEach(node => (step.get(node) || []).forEach(({next: to}) => { if (!layer.has(to)) { layer.set(to, depth); next.push(to); } }));
      frontier = next;
    }
    return layer;
  }

  // Shortest chain of edges from the cause to the symptom, so the hop count in
  // the ranking table can be read as an actual route.
  function causalRoute(edges, symptomKey, causeKey) {
    if (symptomKey === causeKey) return [];
    const step = causalSteps(edges);
    const queue = [[symptomKey, []]];
    const seen = new Set([symptomKey]);
    while (queue.length) {
      const [node, trail] = queue.shift();
      for (const {next, kind} of step.get(node) || []) {
        if (seen.has(next)) continue;
        const extended = trail.concat([{to: next, kind, reversed: !CAUSAL_WITH_EDGE.has(kind)}]);
        if (next === causeKey) {
          // Collected symptom-first; read it back cause-first.
          const nodes = [symptomKey, ...extended.map(x => x.to)].reverse();
          const steps = extended.map(x => ({kind: x.kind, reversed: x.reversed})).reverse();
          return nodes.slice(1).map((to, i) => ({to, kind: steps[i].kind, reversed: steps[i].reversed}));
        }
        seen.add(next);
        queue.push([next, extended]);
      }
    }
    return null;
  }

  function causalPath(result, candidate) {
    const symptomKey = nodeKey({Namespace: result.symptom.namespace, Kind: result.symptom.entity_kind, Name: result.symptom.entity_name});
    const causeKey = nodeKey({Namespace: candidate.event.namespace, Kind: candidate.event.entity_kind, Name: candidate.event.entity_name});
    if (causeKey === symptomKey) {
      const self = parseKey(causeKey);
      return `<div class="path"><span class="hop self"><b>${esc(shortName(self.name, 30))}</b><small>${esc(self.kind)}</small></span><span class="path-note">cause and symptom are the same resource \u2014 no hop between them</span></div>`;
    }
    const route = causalRoute(result.evidence || [], symptomKey, causeKey);
    const start = parseKey(causeKey);
    if (!route || !route.length) {
      const end = parseKey(symptomKey);
      return `<div class="path"><span class="hop cause"><b>${esc(shortName(start.name, 28))}</b><small>${esc(start.kind)}</small></span><span class="link">${candidate.distance} hop(s)</span><span class="hop sym"><b>${esc(shortName(end.name, 28))}</b><small>${esc(end.kind)}</small></span><span class="path-note">exact route is outside the retained evidence</span></div>`;
    }
    return `<div class="path">
      <span class="hop cause"><b>${esc(shortName(start.name, 28))}</b><small>${esc(start.kind)}</small></span>
      ${route.map(hop => { const to = parseKey(hop.to); const last = hop.to === symptomKey;
        return `<span class="link${hop.reversed ? ' rev' : ''}">${esc(hop.kind)}</span><span class="hop ${last ? 'sym' : ''}"><b>${esc(shortName(to.name, 28))}</b><small>${esc(to.kind)}</small></span>`; }).join('')}
    </div>`;
  }

  // A small layered picture of the retained evidence: upstream on the left,
  // the symptom in its own column, anything downstream on the right.
  function evidenceGraph(result, candidate) {
    const edges = result.evidence || [];
    if (!edges.length) return `<div class="none">No dependency edges were retained for this analysis.</div>`;
    const symptomKey = nodeKey({Namespace: result.symptom.namespace, Kind: result.symptom.entity_kind, Name: result.symptom.entity_name});
    const causeKey = candidate ? nodeKey({Namespace: candidate.event.namespace, Kind: candidate.event.entity_kind, Name: candidate.event.entity_name}) : '';
    const layer = layersFrom(edges, symptomKey);

    const nodes = new Map();
    edges.forEach(e => [e.From, e.To].forEach(n => { const k = nodeKey(n); if (!nodes.has(k)) nodes.set(k, {key: k, ...parseKey(k)}); }));
    if (!nodes.has(symptomKey)) nodes.set(symptomKey, {key: symptomKey, ...parseKey(symptomKey)});

    const columns = [...new Set([...nodes.keys()].map(k => layer.has(k) ? layer.get(k) : -1))].sort((a, b) => b - a);
    const order = new Map(columns.map((c, i) => [c, i]));
    const W = 172, H = 40, GAPX = 74, GAPY = 12, PAD = 12;
    const counters = new Map();
    const placed = new Map();
    [...nodes.values()].sort((a, b) => a.key.localeCompare(b.key)).forEach(n => {
      const column = layer.has(n.key) ? layer.get(n.key) : -1;
      const row = counters.get(column) || 0;
      counters.set(column, row + 1);
      placed.set(n.key, Object.assign({}, n, {x: PAD + order.get(column) * (W + GAPX), y: PAD + row * (H + GAPY)}));
    });
    const width = PAD * 2 + columns.length * W + Math.max(0, columns.length - 1) * GAPX;
    const height = PAD * 2 + Math.max(...counters.values(), 1) * (H + GAPY);
    const showLabels = edges.length <= 14;

    const lines = edges.map(e => {
      const a = placed.get(nodeKey(e.From)), b = placed.get(nodeKey(e.To));
      if (!a || !b) return '';
      const x1 = a.x + W, y1 = a.y + H / 2, x2 = b.x, y2 = b.y + H / 2;
      const midX = (x1 + x2) / 2;
      const onPath = nodeKey(e.From) === causeKey || nodeKey(e.To) === symptomKey;
      return `<path d="M${x1} ${y1} C${midX} ${y1} ${midX} ${y2} ${x2} ${y2}" class="ev-edge${onPath ? ' on' : ''}" marker-end="url(#ev-arrow)"/>` +
        (showLabels ? `<text class="ev-kind" x="${midX}" y="${(y1 + y2) / 2 - 4}" text-anchor="middle">${esc(e.Kind)}</text>` : '');
    }).join('');

    const boxes = [...placed.values()].map(n => {
      const role = n.key === symptomKey ? 'sym' : (n.key === causeKey ? 'cause' : '');
      return `<g class="ev-node ${role}" transform="translate(${n.x},${n.y})">
        <rect width="${W}" height="${H}" rx="2"/>
        <text class="ev-name" x="9" y="17">${esc(shortName(n.name, 24))}</text>
        <text class="ev-meta" x="9" y="30">${esc(n.kind)} \u00b7 ${esc(n.namespace || 'cluster')}</text>
      </g>`;
    }).join('');

    return `<div class="ev-wrap"><svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}">
      <defs><marker id="ev-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0 L7 3.5 L0 7 z" class="ev-head"/></marker></defs>
      ${lines}${boxes}
    </svg></div>
    <div class="ev-key"><span class="sw sym"></span><span>symptom</span><span class="sw cause"></span><span>selected cause</span><span class="sw"></span><span>dependency</span><span class="ev-count">${edges.length} edge(s) \u00b7 ${placed.size} node(s)${showLabels ? '' : ' \u00b7 labels hidden'}</span></div>`;
  }

  function blastRadius(result) {
    const radius = result.blast_radius || {};
    const services = radius.services || [];
    if (!services.length) return `<div class="none">Nothing downstream of this resource is reachable in the graph.</div>`;
    const byKind = new Map();
    services.forEach(key => { const parsed = parseKey(key); if (!byKind.has(parsed.kind)) byKind.set(parsed.kind, []); byKind.get(parsed.kind).push(parsed); });
    return `<div class="blast">${[...byKind.entries()].map(([kind, items]) => `
      <div class="blast-group"><div class="blast-k">${esc(kind)}<span>${items.length}</span></div>
      <div class="blast-items">${items.map(i => `<span class="blast-item"><b>${esc(i.name)}</b><small>${esc(i.namespace)}</small></span>`).join('')}</div></div>`).join('')}
    </div>`;
  }

  function impactSections(g) {
    const result = (state.opsRCA || {})[g.id];
    if (!result || result === 'loading' || result.error || !result.candidates || !result.candidates.length) return '';
    const candidate = result.candidates[Math.min(focusIndex(g), result.candidates.length - 1)];
    const radius = result.blast_radius || {};
    return `<section class="sec"><div class="sec-h"><b>Causal path</b><span>SELECTED CANDIDATE \u2192 SYMPTOM</span></div>
        <div class="sec-body">${causalPath(result, candidate)}</div>
        ${evidenceGraph(result, candidate)}
      </section>
      <section class="sec"><div class="sec-h"><b>Blast radius</b><span>${radius.affected_services || 0} SERVICES \u00b7 ${radius.affected_nodes || 0} NODES REACHABLE</span></div>
        <div class="sec-body">${blastRadius(result)}</div>
      </section>`;
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
      ${impactSections(g)}
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
    document.querySelectorAll('[data-scope]').forEach(button => button.onclick = () => {
      state.opsScope = button.dataset.scope;
      window.render();
    });
    document.querySelectorAll('[data-sev]').forEach(button => button.onclick = () => {
      state.opsSeverity = button.dataset.sev;
      window.render();
    });
    document.querySelectorAll('[data-sig]').forEach(button => button.onclick = () => {
      state.opsSelected = button.dataset.sig;
      window.render();
    });
    document.querySelectorAll('[data-cand]').forEach(row => row.onclick = () => {
      const active = selected(group(signals()));
      if (!active) return;
      state.opsCandidateBy = state.opsCandidateBy || {};
      state.opsCandidateBy[active.id] = Number(row.dataset.cand);
      window.render();
    });
    const evidence = el('.ev-wrap');
    if (evidence && evidence.scrollWidth > evidence.clientWidth) {
      const symptom = evidence.querySelector('.ev-node.sym');
      evidence.scrollLeft = symptom
        ? Math.max(0, symptom.getBoundingClientRect().left - evidence.getBoundingClientRect().left + evidence.scrollLeft - 24)
        : evidence.scrollWidth;
    }
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
