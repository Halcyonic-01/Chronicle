/* Chronicle operations console.
   Shell, shared helpers, data loading, and the views that are not owned by a
   dedicated file. incidents.js, replay.js and graph.js replace their own view
   after this file loads; everything here is resolved by name at render time so
   those overrides always win. */

// Old links used the phase-era page names.
const pageAliases = {overview: 'now', timeline: 'events', services: 'events', rca: 'incidents'};
const hashPage = () => { const raw = location.hash.slice(1); return pageAliases[raw] || raw || 'now'; };

const state = {
  page: hashPage(),
  events: [], totalEvents: 0, eventError: '', loading: true,
  postureLoading: true, postureError: '', posture: {resources: [], summary: {total: 0, healthy: 0, warning: 0, critical: 0}},
  filterSeverity: '', filterSource: '', filterQuery: '', selectedEvent: null, selectedIncident: null,
  replayAt: new Date().toISOString(), replayPosition: 60, replayLive: false,
  graph: {nodes: [], edges: []}, actions: []
};
window.chronicleState = state;

// Seven destinations, down from ten: root cause is a step inside an incident,
// and timeline/services were the event table wearing different hats.
const navItems = [
  ['now', 'Now', '◉'], ['incidents', 'Incidents', '!'], ['events', 'Events', '≡'],
  ['graph', 'Graph', '◇'], ['replay', 'Replay', '◷'], ['healing', 'Healing', '↺'],
  ['settings', 'Settings', '⚙']
];

const $ = (s) => document.querySelector(s);
const esc = (v) => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmtTime = (v, withDate=false) => { const d = new Date(v); if (Number.isNaN(d.getTime())) return '—'; return d.toLocaleString([], withDate ? {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'} : {hour:'2-digit',minute:'2-digit',second:'2-digit'}); };
const ago = (v) => { const s = Math.max(0, (Date.now()-new Date(v).getTime())/1000); if(s<60)return `${Math.round(s)}s`; if(s<3600)return `${Math.round(s/60)}m`; if(s<86400)return `${Math.round(s/3600)}h`; return `${Math.round(s/86400)}d`; };
const severity = (v='info') => `<span class="severity ${esc(v)}">${esc(v)}</span>`;
const empty = (title, copy) => `<div class="none">${esc(title)}${copy ? ' — ' + esc(copy) : ''}</div>`;
// Collect everything, triage what you own. These namespaces stay in the event
// store and in the dependency graph; they are simply not what you are on call
// for. Shared so incidents, the graph and the posture list never drift apart.
const PLATFORM_NAMESPACES = new Set(['kube-system','kube-public','kube-node-lease','monitoring','linkerd','local-path-storage','chronicle','argocd','github','terraform']);
const scopeOf = (namespace) => PLATFORM_NAMESPACES.has(namespace) ? 'platform' : 'application';
window.chronicleScopeOf = scopeOf;
function scopeSegments(id, current){
  return `<div class="seg">${['application','platform','all'].map(scope =>
    `<button type="button" data-${id}="${scope}" aria-pressed="${current===scope}">${scope.toUpperCase()}</button>`).join('')}</div>`;
}

const targetOf = (e) => `${e.namespace || 'cluster'}/${e.entity_name || '—'}`;

// Every API call goes through api() so an optional CHRONICLE_API_TOKEN can be
// supplied once and reused. The token is held only for the browser session.
const apiToken = () => { try { return sessionStorage.getItem('chronicle_api_token')||''; } catch (_) { return ''; } };
const withToken = (options,token) => Object.assign({},options,{headers:Object.assign({},options&&options.headers,token?{Authorization:`Bearer ${token}`}:{})});
async function api(path, options){
  let response = await fetch(path, withToken(options, apiToken()));
  if(response.status!==401) return response;
  const entered = window.prompt('Chronicle API token');
  if(!entered) return response;
  try { sessionStorage.setItem('chronicle_api_token',entered); } catch (_) {}
  return fetch(path, withToken(options, entered));
}
window.chronicleAPI = api;

function renderNav(){ $('#nav').innerHTML = navItems.map(([id,label,icon]) => `<div class="nav-item ${state.page===id?'active':''}" data-page="${id}"><span class="nav-icon">${icon}</span><span>${label}</span></div>`).join(''); document.querySelectorAll('.nav-item').forEach(el=>el.onclick=()=>navigate(el.dataset.page)); }
function navigate(page){ state.page=page; location.hash=page; render(); }

// Compact header. graph.js and replay.js render through this, so keeping the
// signature means they inherit the denser chrome without changes.
function pageHeading(kicker,title,subtitle,action=''){
  return `<div class="ph"><h1>${esc(title)}</h1><p>${esc(subtitle||'')}</p>${action?`<div class="act">${action}</div>`:''}</div>`;
}

/* ---------------------------------------------------------------- shared */

function histogram(events){
  const buckets = 40;
  if(!events.length) return `<div class="hist">${'<i></i>'.repeat(buckets)}</div>`;
  const times = events.map(e=>new Date(e.ingested_at).getTime());
  const start = Math.min(...times), end = Math.max(Math.max(...times), Math.min(...times)+1);
  const counts = new Array(buckets).fill(0), crit = new Array(buckets).fill(0);
  events.forEach(e=>{ const slot = Math.min(buckets-1, Math.floor(((new Date(e.ingested_at).getTime()-start)/(end-start))*buckets)); counts[slot]++; if(e.severity==='critical')crit[slot]++; });
  const peak = Math.max(...counts,1);
  return `<div class="hist" role="img" aria-label="Event volume">${counts.map((n,i)=>`<i class="${n?(crit[i]?'c':'w'):''}" style="height:${n?Math.max(8,Math.round(n/peak*100)):0}%" title="${n} event(s)"></i>`).join('')}</div>`;
}

function severitySegments(){
  const current = state.filterSeverity;
  const button = (v,l,c) => `<button type="button" class="${c}" data-severity-filter="${v}" aria-pressed="${current===v}">${l}</button>`;
  return `<div class="seg">${button('','ALL','')}${button('critical','CRIT','crit')}${button('warning','WARN','warn')}${button('info','INFO','')}</div>`;
}

function filteredEvents(){
  const q = (state.filterQuery||'').toLowerCase();
  return (state.events||[]).filter(e =>
    (!state.filterSeverity || e.severity === state.filterSeverity) &&
    (!state.filterSource || e.source === state.filterSource) &&
    (!q || `${e.title} ${e.type} ${targetOf(e)} ${e.source}`.toLowerCase().includes(q)));
}

function eventTable(items){
  if(!items.length) return empty('No matching events','widen the window or clear the filters');
  return `<table class="dt"><thead><tr><th>Time</th><th>Age</th><th>Source</th><th>Type</th><th>Target</th><th>Sev</th><th>Title</th></tr></thead><tbody>${items.map(e=>`
    <tr data-event="${esc(e.id)}">
      <td class="dim">${fmtTime(e.ingested_at)}</td>
      <td class="dim num">${ago(e.ingested_at)}</td>
      <td>${esc(e.source)}</td>
      <td>${esc(e.type)}</td>
      <td class="dim">${esc(targetOf(e))}</td>
      <td class="sev-${esc(e.severity)}">${esc((e.severity||'info').slice(0,4))}</td>
      <td class="t">${esc(e.title||e.type)}</td>
    </tr>`).join('')}</tbody></table>`;
}

function eventRows(items){
  if(!items.length) return empty('No events in the loaded window');
  return items.map(e=>`<button type="button" class="lr" data-event="${esc(e.id)}">
    <span class="lr-top"><span class="lr-t">${esc(e.type)}</span><span class="lr-b">${esc(e.source)}</span><span class="lr-m">${ago(e.ingested_at)} ago</span></span>
    <div class="lr-s">${esc(e.title||e.type)}</div>
    <div class="lr-s" style="color:var(--dim)">${esc(targetOf(e))}</div>
  </button>`).join('');
}

/* ------------------------------------------------------------------ now */

function now(){
  const s = state.posture.summary || {total:0,healthy:0,warning:0,critical:0};
  const signals = (state.events||[]).filter(e=>e.severity==='warning'||e.severity==='critical');
  const unhealthy = (state.posture.resources||[]).filter(r=>r.status!=='healthy');
  const pending = (state.actions||[]).filter(a=>a.approval==='pending');
  const cell = (label,value,cls,note) => `<div><dt>${label}</dt><dd class="${cls||''}">${value}${note?`<small>${note}</small>`:''}</dd></div>`;
  return `<div class="ops">
    <div class="strip">
      ${cell('Resources', state.postureLoading ? '—' : `${s.healthy}/${s.total}`, s.critical?'crit':(s.warning?'warn':'ok'), 'ready')}
      ${cell('Degraded', state.postureLoading ? '—' : (s.warning+s.critical), (s.warning+s.critical)?'warn':'ok')}
      ${cell('Signals', signals.length, signals.some(e=>e.severity==='critical')?'crit':(signals.length?'warn':'ok'), '24h')}
      ${cell('Events', state.totalEvents||state.events.length, '', '24h')}
      ${cell('Graph', `${(state.graph.nodes||[]).length}`, '', `nodes · ${(state.graph.edges||[]).length} edges`)}
      ${cell('Approvals', pending.length, pending.length?'warn':'ok', 'pending')}
    </div>
    <div class="duo">
      <section>
        <div class="sec-h"><b>Needs attention</b><span>${unhealthy.length} OF ${s.total} RESOURCES</span></div>
        ${state.postureLoading ? empty('Reading live Kubernetes state')
          : state.postureError ? empty('Live posture unavailable', state.postureError)
          : unhealthy.length ? `<table class="dt"><thead><tr><th>Kind</th><th>Resource</th><th>Ready</th><th class="num">Restarts</th><th>Detail</th></tr></thead><tbody>${unhealthy.map(r=>`
              <tr style="cursor:default"><td>${esc(r.kind)}</td><td>${esc(r.namespace)}/${esc(r.name)}</td>
              <td class="sev-${r.status==='critical'?'critical':'warning'}">${r.ready}/${r.desired}</td>
              <td class="num ${r.restarts?'sev-warning':'dim'}">${r.restarts||0}</td>
              <td class="t dim">${esc(r.message||'')}</td></tr>`).join('')}</tbody></table>`
          : empty(`All ${s.total} resources ready`, 'no pod or deployment is reporting a readiness problem')}
      </section>
      <section>
        <div class="sec-h"><b>Active signals</b><span>WARNING AND CRITICAL</span></div>
        ${signals.length ? eventRows(signals.slice(0,40)) : empty('No warning or critical signals')}
      </section>
    </div>
    <div class="ops-foot">
      <span>${esc(state.eventError || 'collectors online')}</span>
      <span style="margin-left:auto">${state.totalEvents||state.events.length} events loaded · last 24h</span>
    </div>
  </div>`;
}

/* --------------------------------------------------------------- events */

function events(){
  const items = filteredEvents();
  const sources = [...new Set((state.events||[]).map(e=>e.source))].sort();
  return `<div class="ops">
    <div class="ops-bar">
      <input class="ops-find" id="event-search" placeholder="filter  /" value="${esc(state.filterQuery||'')}" autocomplete="off" spellcheck="false">
      ${severitySegments()}
      <select id="source-filter" style="font:10px var(--mono);background:var(--bg);border:1px solid var(--line);color:var(--text);padding:4px 6px">
        <option value="">all sources</option>
        ${sources.map(s=>`<option value="${esc(s)}" ${state.filterSource===s?'selected':''}>${esc(s)}</option>`).join('')}
      </select>
      ${histogram(items)}
      <span class="ops-span">${items.length} SHOWN · ${state.totalEvents||state.events.length} LOADED</span>
    </div>
    <div class="scroll" id="events-table">${eventTable(items)}</div>
    <div class="ops-foot">
      ${state.totalEvents > state.events.length ? `<button id="load-more-events" style="background:none;border:0;color:var(--muted);font:10px var(--mono);cursor:pointer;padding:0">load ${Math.min(100, state.totalEvents-state.events.length)} older →</button>` : '<span>all loaded events shown</span>'}
      <span style="margin-left:auto">click a row to inspect the raw record</span>
    </div>
  </div>`;
}

/* -------------------------------------------------------------- healing */

function healing(){
  const actions = state.actions||[];
  const pending = actions.filter(a=>a.approval==='pending');
  const rules = [
    ['restart-deadlocked-pod','became_unready','restart_pod','0.80','3/h','approval required'],
    ['bump-memory-on-oom','oom_kill','bump_memory','0.85','2/h','automatic when live'],
    ['rollback-bad-deploy','deploy','rollback_deployment','0.90','1/h','approval required']
  ];
  return `<div class="ops">
    <div class="mode"><span>●</span><span>DRY RUN · execution disabled · every decision is recorded before anything runs</span></div>
    <div class="strip">
      <div><dt>Queued</dt><dd>${actions.length}</dd></div>
      <div><dt>Pending approval</dt><dd class="${pending.length?'warn':'ok'}">${pending.length}</dd></div>
      <div><dt>Executed</dt><dd>${actions.filter(a=>a.status==='succeeded').length}</dd></div>
      <div><dt>Blocked</dt><dd>${actions.filter(a=>a.status==='blocked').length}</dd></div>
    </div>
    <div class="scroll">
      <div class="sec-h"><b>Remediation queue</b><span>AUDIT STORE</span></div>
      ${actions.length ? `<table class="dt"><thead><tr><th>Created</th><th>Rule</th><th>Action</th><th>Target</th><th class="num">Conf</th><th>Status</th><th>Approval</th><th>Review</th></tr></thead><tbody>${actions.map(a=>`
        <tr style="cursor:default">
          <td class="dim">${fmtTime(a.created_at,true)}</td>
          <td>${esc(a.rule||'—')}</td>
          <td>${esc(a.action_type||'—')}</td>
          <td class="dim">${a.target?`${esc(a.namespace||'default')}/${esc(a.target)}`:'—'}</td>
          <td class="num">${(a.confidence||0).toFixed(2)}</td>
          <td class="${a.status==='blocked'||a.status==='failed'?'sev-warning':''}">${esc(a.status)}</td>
          <td class="dim">${esc(a.approval)}</td>
          <td>${a.approval==='pending' && a.status==='would_run'
            ? `<button class="k pri" data-approve="${esc(a.id)}">approve</button> <button class="k no" data-deny="${esc(a.id)}">deny</button>`
            : `<span class="dim">${esc(a.result||a.approval||'')}</span>`}</td>
        </tr>`).join('')}</tbody></table>`
        : empty('No remediation actions recorded','run RCA on a signal; a matched rule appears here as an audited decision')}
      <div class="sec-h" style="border-top:1px solid var(--line2)"><b>Rules</b><span>CONFIDENCE FLOOR · RATE LIMIT</span></div>
      <table class="dt"><thead><tr><th>Rule</th><th>Trigger</th><th>Action</th><th class="num">Min conf</th><th>Limit</th><th>Mode</th></tr></thead><tbody>
        ${rules.map(([n,t,a,c,l,m])=>`<tr style="cursor:default"><td>${n}</td><td class="dim">${t}</td><td>${a}</td><td class="num">${c}</td><td class="dim">${l}</td><td class="sev-warning">${m}</td></tr>`).join('')}
      </tbody></table>
    </div>
    <div class="ops-foot"><span>live execution also requires: live mode on · kill switch off · 30-day observation elapsed · namespace, action and target allowlisted</span></div>
  </div>`;
}

/* ------------------------------------------------------------- settings */

function settings(){
  const row = (k,v,note) => `<tr style="cursor:default"><td>${esc(k)}</td><td>${esc(v)}</td><td class="t dim">${esc(note||'')}</td></tr>`;
  return `<div class="ops">
    <div class="scroll">
      <div class="sec-h"><b>Collection</b><span>READ-ONLY</span></div>
      <table class="dt spec"><thead><tr><th>Setting</th><th>Value</th><th>Note</th></tr></thead><tbody>
        ${row('Environment','local / kind','cluster this console is served from')}
        ${row('Collectors','k8s · github · prometheus · loki','argocd and terraform activate when their URL is set')}
        ${row('Event bus','kafka → postgres','offsets commit only after a batch is persisted')}
        ${row('Recent cache','redis','serves the default event view; postgres is the fallback')}
        ${row('Snapshots','every 5 min','retained: all 7d · hourly 30d · daily 1y')}
        ${row('Graph sync','every 30 s','diffed against open edges; only real changes are versioned')}
      </tbody></table>
      <div class="sec-h" style="border-top:1px solid var(--line2)"><b>Safety</b><span>DEFAULT DENY</span></div>
      <table class="dt spec"><thead><tr><th>Gate</th><th>Value</th><th>Note</th></tr></thead><tbody>
        ${row('Healing mode','dry run','plans and audits actions, never executes')}
        ${row('Kill switch','enabled','blocks execution regardless of every other gate')}
        ${row('Observation period','30 days','must elapse before any live action')}
        ${row('Approval','token required','sent as X-Chronicle-Heal-Token')}
        ${row('API auth','set CHRONICLE_API_TOKEN','unset means anything reaching the Service can read')}
        ${row('Secrets','not readable','Chronicle has no cluster-wide Secret access')}
      </tbody></table>
    </div>
    <div class="ops-foot"><span>configuration is supplied by environment; this view reports it, it does not change it</span></div>
  </div>`;
}

/* ------------------------------------------------------- event inspector */

function payloadText(e){ try { const value=typeof e.payload==='string'?JSON.parse(e.payload):e.payload; if(value==null||value===''||(typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===0))return 'No structured payload was recorded for this event.'; return JSON.stringify(value,null,2); } catch (_) { return String(e.payload||''); } }

function eventInspector(){
  const e=state.events.find(x=>x.id===state.selectedEvent);
  if(!e)return '';
  return `<div class="event-inspector-backdrop" id="event-inspector" role="presentation"><section class="event-inspector" role="dialog" aria-modal="true" aria-labelledby="event-inspector-title">
    <div class="det-head"><div class="det-id"><div class="det-type" id="event-inspector-title">${esc(e.type)}</div><div class="det-target">${esc(targetOf(e))} · ${esc(e.entity_kind||'Resource')} · ${esc(e.source)}</div></div>
    <div class="det-act"><button class="k" id="close-event">close</button></div></div>
    <dl class="kv">
      <div><dt>Severity</dt><dd>${esc(e.severity)}</dd></div>
      <div><dt>Ingested</dt><dd>${fmtTime(e.ingested_at,true)}</dd></div>
      <div><dt>Occurred</dt><dd>${fmtTime(e.occurred_at,true)}</dd></div>
      <div><dt>Event ID</dt><dd>${esc(e.id)}</dd></div>
      <div><dt>Correlation</dt><dd>${esc(e.correlation_key||'—')}</dd></div>
      <div><dt>Trace</dt><dd>${esc(e.trace_id||'—')}</dd></div>
    </dl>
    <div class="sec-h"><b>Title</b></div><pre class="raw">${esc(e.title||'')}</pre>
    <div class="sec-h"><b>Payload</b></div><pre class="raw">${esc(payloadText(e))}</pre>
  </section></div>`;
}
function openEvent(id){ if(state.events.some(e=>e.id===id)){state.selectedEvent=id;render();} }
function closeEvent(){state.selectedEvent=null;render();}

if(!window.__chronicleEventClickHandler){
  document.addEventListener('click', event=>{
    const row=event.target.closest?.('[data-event]');
    if(!row || row.closest('#event-inspector') || row.classList.contains('replay-event') || row.classList.contains('sig'))return;
    const id=row.dataset.event;
    if(state.events.some(e=>e.id===id)){event.preventDefault();openEvent(id);}
  });
  document.addEventListener('keydown', event=>{ if(event.key==='Escape' && state.selectedEvent) closeEvent(); });
  window.__chronicleEventClickHandler=true;
}

/* --------------------------------------------------------------- render */

const pageIDs = new Set(navItems.map(item => item[0]));
function render(){
  renderNav();
  const page = pageIDs.has(state.page) ? state.page : 'now';
  $('#page-label').textContent = (navItems.find(x=>x[0]===page)||navItems[0])[1];
  const view = window[page] || now;
  $('#app').innerHTML = view() + eventInspector();
  bindView();
}

function bindView(){
  const content = $('#app');
  if(content) content.classList.add('bleed');
  const search=$('#event-search'), source=$('#source-filter');
  if(search) search.oninput=()=>{ const at=search.selectionStart; state.filterQuery=search.value; render(); const again=$('#event-search'); if(again){again.focus();again.setSelectionRange(at,at);} };
  if(source) source.onchange=()=>{ state.filterSource=source.value; render(); };
  document.querySelectorAll('[data-severity-filter]').forEach(button=>button.onclick=()=>{ state.filterSeverity=button.dataset.severityFilter||''; render(); });
  if($('#load-more-events'))$('#load-more-events').onclick=loadMoreEvents;
  document.querySelectorAll('[data-approve]').forEach(b=>b.onclick=()=>decide(b.dataset.approve,true));
  document.querySelectorAll('[data-deny]').forEach(b=>b.onclick=()=>decide(b.dataset.deny,false));
  if($('#close-event'))$('#close-event').onclick=closeEvent;
  if($('#event-inspector'))$('#event-inspector').onclick=(e)=>{if(e.target.id==='event-inspector')closeEvent();};
}

/* ----------------------------------------------------------------- data */

async function loadEvents(){
  state.loading=true; render();
  try{
    const r=await api('/api/events?limit=100');
    const d=await r.json();
    if(!r.ok)throw new Error(d.error||'Events unavailable');
    state.events=d.events||[];
    state.totalEvents=Number.isFinite(d.total)?d.total:state.events.length;
    state.eventError='';
  }catch(e){ state.events=[]; state.totalEvents=0; state.eventError=e.message; }
  await Promise.all([loadGraph(),loadActions(),loadPosture()]);
  state.loading=false; render();
}
async function loadGraph(){ try { const r=await api('/api/graph'); state.graph=r.ok?await r.json():{nodes:[],edges:[]}; } catch (_) { state.graph={nodes:[],edges:[]}; } }
async function loadPosture(){ state.postureLoading=true; try { const r=await api('/api/posture'); const d=await r.json(); if(!r.ok)throw new Error(d.error||'Live posture unavailable'); state.posture=d; state.postureError=''; } catch (e) { state.posture={resources:[],summary:{total:0,healthy:0,warning:0,critical:0}}; state.postureError=e.message; } state.postureLoading=false; }
async function loadActions(){ try { const r=await api('/api/heal/actions'); state.actions=r.ok?(await r.json()).actions||[]:[]; } catch (_) { state.actions=[]; } }
async function loadMoreEvents(){
  const button=$('#load-more-events');
  if(button){button.disabled=true;button.textContent='loading…';}
  try{
    const r=await api(`/api/events?limit=100&offset=${state.events.length}`);
    const d=await r.json();
    if(!r.ok)throw new Error(d.error||'Unable to load older events');
    state.events=state.events.concat(d.events||[]);
    if(Number.isFinite(d.total))state.totalEvents=d.total;
  }catch(e){ showToast(e.message); }
  render();
}

async function decide(id,approved){
  try{
    let token=sessionStorage.getItem('chronicle_heal_token')||window.prompt('Healing approval token');
    if(!token)return;
    sessionStorage.setItem('chronicle_heal_token',token);
    const r=await api(`/api/heal/actions/${encodeURIComponent(id)}/${approved?'approve':'deny'}`,{method:'POST',headers:{'Content-Type':'application/json','X-Chronicle-Heal-Token':token},body:JSON.stringify({by:'local-reviewer'})});
    if(r.status===401){sessionStorage.removeItem('chronicle_heal_token');throw new Error('Invalid approval token');}
    if(!r.ok)throw new Error('Action is no longer pending');
    await loadActions(); render();
    showToast(approved?'Approved and recorded':'Denied and recorded');
  }catch(e){ showToast(e.message); }
}

function showToast(msg){ const t=$('#toast'); t.textContent=msg; t.classList.add('show'); setTimeout(()=>t.classList.remove('show'),2200); }

// Navigation collapse persists per browser: an operator who wants the width
// back should not have to reclaim it on every page load.
const navKey = 'chronicle.nav.collapsed';
function setNavCollapsed(collapsed){
  document.querySelector('.app-shell').classList.toggle('nav-collapsed', collapsed);
  const toggle = $('#nav-toggle');
  if(toggle) toggle.setAttribute('aria-expanded', String(!collapsed));
  try { localStorage.setItem(navKey, collapsed ? '1' : '0'); } catch (_) {}
}
function toggleNav(){ setNavCollapsed(!document.querySelector('.app-shell').classList.contains('nav-collapsed')); }
try { setNavCollapsed(localStorage.getItem(navKey) === '1'); } catch (_) {}
$('#nav-toggle').onclick = toggleNav;
document.addEventListener('keydown', event => {
  if(event.key !== '[' || event.metaKey || event.ctrlKey || event.altKey) return;
  if(/^(INPUT|TEXTAREA|SELECT)$/.test(event.target.tagName)) return;
  event.preventDefault();
  toggleNav();
});

$('#refresh').onclick=()=>loadEvents();
window.addEventListener('hashchange',()=>{ state.page=hashPage(); render(); });
// The first render waits for incidents.js, replay.js and graph.js to install
// their views, otherwise a deep link renders this file's fallback.
document.addEventListener('DOMContentLoaded',()=>loadEvents());
