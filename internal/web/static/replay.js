/* Phase 3 replay UI: time, graph, state colours, and playback. */
(function () {
  const stateRef = window.__chronicleState || null;
  // app.js keeps state private in the original build, so the helpers below
  // attach through the existing global functions and DOM state.
  const getState = () => window.chronicleState || stateRef;
  const escValue = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const classFor = phase => { const value=String(phase||'').trim().toLowerCase(); if(!value)return 'referenced'; return ['failed','degraded','restarting','notready','unhealthy'].includes(value) ? 'critical' : ['pending','changed','unknown'].includes(value) ? 'warning' : ['referenced','present','bound'].includes(value) ? 'referenced' : 'healthy'; };
  const key = n => `${n?.Namespace ?? n?.namespace ?? ''}/${n?.Kind ?? n?.kind ?? ''}/${n?.Name ?? n?.name ?? ''}`;
  const namespaceFor = n => n?.Namespace ?? n?.namespace ?? '';
  const nameFor = n => n?.Name ?? n?.name ?? '';
  const kindFor = n => n?.Kind ?? n?.kind ?? 'Resource';
  const platformNamespaces = new Set(['kube-system','kube-public','kube-node-lease','monitoring','linkerd','local-path-storage']);
  const categoryFor = n => kindFor(n)==='Node' || platformNamespaces.has(namespaceFor(n)) ? 'platform' : 'application';
  const graphScope = () => state.replayGraphScope || 'application';
  const graphNamespace = () => state.replayGraphNamespace || '';
  const matchesGraphFilter = n => (graphScope()==='all' || categoryFor(n)===graphScope()) && (!graphNamespace() || namespaceFor(n)===graphNamespace());
  const timeFor = value => new Date(Date.now() - (60 - Number(value)) * 60000).toISOString();
  const formatTime = value => new Date(value).toLocaleString([], {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'});

  // This file is loaded after app.js; replacing the view functions keeps the
  // embedded frontend dependency-free while making replay fully interactive.
  const renderReplayGraph = data => {
    const host = document.querySelector('#replay-graph'); if (!host) return;
    const objects = Object.entries(data.objects || {}).map(([id, o]) => ({id, ...o})).filter(matchesGraphFilter);
    const edges = (data.edges || []).filter(e => e.From && e.To && matchesGraphFilter(e.From) && matchesGraphFilter(e.To));
    const nodes = new Map(objects.map(o => [key(o) || o.id, o]));
    edges.forEach(e => [e.From,e.To].forEach(n => { if (n && !nodes.has(key(n))) nodes.set(key(n), {...n, Phase:'Referenced'}); }));
    const list = [...nodes.values()];
    if (!list.length) {
      host.innerHTML=emptyView('No resources in this view','Try the All resources scope or choose another namespace.');
      return;
    }
    const columns = Math.min(5, Math.max(1, list.length));
    const rows = Math.max(1, Math.ceil(list.length / columns));
    const canvasWidth = Math.max(1200, 25 + (columns - 1) * 230 + 210);
    const canvasHeight = Math.max(430, 25 + (rows - 1) * 105 + 100);
    const positions = new Map(list.map((n,i) => [key(n)||n.id, {x:25+(i%columns)*230,y:25+Math.floor(i/columns)*105}]));
    const lines = edges.map(e => { const a=positions.get(key(e.From)), b=positions.get(key(e.To)); if(!a||!b)return ''; const ca=classFor(nodes.get(key(e.From))?.phase ?? nodes.get(key(e.From))?.Phase), cb=classFor(nodes.get(key(e.To))?.phase ?? nodes.get(key(e.To))?.Phase); const tone=ca==='critical'||cb==='critical'?'critical':ca==='warning'||cb==='warning'?'warning':''; return `<line class="replay-edge edge-${tone}" x1="${a.x+85}" y1="${a.y+31}" x2="${b.x+85}" y2="${b.y+31}" marker-end="url(#replay-arrow)"></line>`; }).join('');
    const markup = list.map(n => { const p=positions.get(key(n)||n.id); const kind=kindFor(n); const rawPhase=n.phase ?? n.Phase; const phase=rawPhase || (['ConfigMap','Secret','Service','PersistentVolumeClaim'].includes(kind)?'Present':'Unknown'); const status=classFor(phase); const reason=n.status_reason ?? n.StatusReason ?? ''; const message=n.status_message ?? n.StatusMessage ?? ''; return `<div class="replay-node state-${status}" title="${escValue(message)}" style="left:${p.x}px;top:${p.y}px"><h3>${escValue(n.name ?? n.Name ?? n.id)}</h3><small>${escValue(kind)} · ${escValue(n.namespace ?? n.Namespace ?? 'cluster')} · ${escValue(phase)}${reason?` · ${escValue(reason)}`:''}</small></div>`; }).join('');
    host.innerHTML=`<div class="replay-graph-canvas" style="width:${canvasWidth}px;height:${canvasHeight}px"><svg viewBox="0 0 ${canvasWidth} ${canvasHeight}"><defs><marker id="replay-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0,0 L7,3.5 L0,7 z" fill="#536b7f"></path></marker></defs>${lines}</svg>${markup}</div><div class="replay-legend"><span style="color:#65c18c">● healthy</span><span style="color:#a8b7c4">● referenced / present</span><span style="color:#e0ad61">● warning / changed</span><span style="color:#e07070">● degraded / failed</span><span>${list.length}/${Object.keys(data.objects||{}).length} nodes · ${edges.length}/${(data.edges||[]).length} edges · ${graphScope()} view</span></div>`;
  };

  // These functions use the existing app.js lexical state through the global
  // bridge installed below by app.js's final render call.
  window.chronicleReplay = {
    render: renderReplayGraph,
    timeFor,
    formatTime,
    classFor
  };

  const state = window.chronicleState;
  const emptyView = (title, copy) => `<div class="empty"><strong>${escValue(title)}</strong><p>${escValue(copy)}</p></div>`;
  const eventSeverity = value => `<span class="replay-severity ${escValue(value||'info')}">${escValue(value||'info')}</span>`;
  const replayPayloadText = event => {
    if (event?.payload == null || event.payload === '') return 'No payload was recorded for this event.';
    if (typeof event.payload === 'string') {
      try { return JSON.stringify(JSON.parse(event.payload), null, 2); } catch (_) { return event.payload; }
    }
    try { return JSON.stringify(event.payload, null, 2); } catch (_) { return String(event.payload); }
  };
  const events = items => items.length ? items.map(e => `<div class="replay-event" data-event="${escValue(e.id)}" role="button" tabindex="0"><div class="replay-event-head"><span class="event-time">${formatTime(e.ingested_at)}</span><span class="event-source-badge source-${escValue((e.source||'system').toLowerCase())}">${escValue(e.source||'system')}</span>${eventSeverity(e.severity)}</div><strong>${escValue(e.title||e.type)}</strong><div class="replay-event-meta"><span>${escValue(e.type||'event')}</span><span>${escValue(e.namespace||'—')}/${escValue(e.entity_kind||'resource')}/${escValue(e.entity_name||'—')}</span></div></div>`).join('') : emptyView('No events before this point','No normalized events were recorded before the selected time.');
  const replayEventInspector = e => `<div class="event-inspector-backdrop" id="replay-event-inspector" role="presentation"><section class="event-inspector" role="dialog" aria-modal="true" aria-labelledby="replay-event-inspector-title"><div class="event-inspector-head"><div><div class="eyebrow">EVENT INSPECTOR · ${escValue(e.source||'system')}</div><h2 id="replay-event-inspector-title">${escValue(e.title||e.type)}</h2><div class="event-inspector-sub">${eventSeverity(e.severity)} <span>${escValue(e.namespace||'—')}/${escValue(e.entity_kind||'resource')}/${escValue(e.entity_name||'—')}</span></div></div><button class="icon-button" id="replay-event-close" aria-label="Close event inspector">×</button></div><div class="event-inspector-grid"><div><span>Event type</span><strong>${escValue(e.type||'event')}</strong></div><div><span>Source</span><strong>${escValue(e.source||'system')}</strong></div><div><span>Occurred</span><strong>${formatTime(e.occurred_at)}</strong></div><div><span>Ingested</span><strong>${formatTime(e.ingested_at)}</strong></div></div><div class="event-inspector-section"><h3>Event payload</h3><pre>${escValue(replayPayloadText(e))}</pre></div>${e.trace_id||e.correlation_key?`<div class="event-inspector-links">${e.trace_id?`<span>Trace <code>${escValue(e.trace_id)}</code></span>`:''}${e.correlation_key?`<span>Correlation <code>${escValue(e.correlation_key)}</code></span>`:''}</div>`:''}</section></div>`;
  const closeReplayEvent = () => document.querySelector('#replay-event-inspector')?.remove();
  const openReplayEvent = id => {
    const e = (state.replayEvents||[]).find(item => item.id === id) || (state.events||[]).find(item => item.id === id);
    if (!e) return;
    closeReplayEvent();
    document.body.insertAdjacentHTML('beforeend', replayEventInspector(e));
    document.querySelector('#replay-event-close')?.addEventListener('click', closeReplayEvent);
    document.querySelector('#replay-event-inspector')?.addEventListener('click', event => { if (event.target.id === 'replay-event-inspector') closeReplayEvent(); });
  };
  const heading = (title, subtitle, action) => `<div class="ph"><h1>${title}</h1><p>${subtitle}</p>${action?`<div class="act">${action}</div>`:''}</div>`;
  const position = () => Number(state.replayPosition ?? 30);
  const setPosition = value => { state.replayPosition=Math.max(0,Math.min(60,Number(value))); state.replayAt=timeFor(state.replayPosition); state.replayLive=false; state.replayData=null; state.replayEvents=[]; state.replayDataLive=false; };
  const replayDataMatchesSelection = data => data && Number.isFinite(new Date(data.taken_at).getTime()) && new Date(data.taken_at).getTime()===new Date(state.replayAt).getTime() && Boolean(state.replayDataLive)===Boolean(state.replayLive);
  const replayControls = data => { const namespaces=[...new Set(Object.values(data?.objects||{}).map(namespaceFor).filter(Boolean))].sort(); const scope=graphScope(); const selected=graphNamespace()||'All namespaces'; return `<div class="replay-graph-controls" id="replay-graph-controls"><span class="replay-control-label">VIEW</span><button class="replay-scope ${scope==='application'?'active':''}" data-replay-scope="application">Application</button><button class="replay-scope ${scope==='platform'?'active':''}" data-replay-scope="platform">Platform</button><button class="replay-scope ${scope==='all'?'active':''}" data-replay-scope="all">All resources</button><details class="replay-namespace-menu" id="replay-namespace-menu"><summary>Namespace: ${escValue(selected)} <span>⌄</span></summary><div class="replay-namespace-options"><button type="button" data-replay-namespace="">All namespaces</button>${namespaces.map(ns=>`<button type="button" data-replay-namespace="${escValue(ns)}">${escValue(ns)}</button>`).join('')}</div></details></div>`; };
  const replayView = () => { const at=new Date(state.replayAt); const replayEvents=state.replayEvents||[]; return heading('Historical infrastructure state','Scrub through time to reconstruct the resources, dependencies, and evidence that existed at that moment.',`<button class="button" id="replay-play">${state.replayPlaying?'Pause':'Play'}</button> <button class="button" id="replay-now">Use current time</button>`)+`<section class="panel"><div class="timeline"><div class="replay-toolbar"><button class="button" id="replay-step-back">−1m</button><input id="replay-range" type="range" min="0" max="60" value="${position()}"><button class="button" id="replay-step-forward">+1m</button><span class="mono" id="replay-time">${formatTime(at)}</span></div><div class="timeline-line"><div class="timeline-point" style="left:${position()/60*100}%"></div></div><div class="timeline-labels"><span>1 hour ago</span><span>Graph and state update together</span><span>now</span></div></div></section><section class="panel" style="margin-top:14px"><div class="panel-head"><span class="panel-title">Dependency graph at ${formatTime(at)}</span><span class="panel-meta">RECONSTRUCTED FROM SNAPSHOT</span></div>${replayControls(state.replayData)}<div id="replay-graph" class="replay-graph">${emptyView('Loading replay graph','Reconstructing the selected point in time…')}</div></section><div class="grid replay-state" style="margin-top:14px"><section class="panel"><div class="panel-head"><span class="panel-title">Reconstructed state</span><span class="panel-meta" id="replay-object-count">${Object.keys(state.replayData?.objects||{}).length} OBJECTS</span></div><div id="replay-result">${emptyView('Choose a moment to replay','Move the scrubber to load historical state.')}</div></section><section class="panel"><div class="panel-head"><span class="panel-title">Events before this point</span><span class="panel-meta" id="replay-events-meta">${replayEvents.length} EVENTS · BEFORE SELECTED TIME</span></div><div class="replay-event-list" id="replay-events">${events(replayEvents)}</div></section></div>`; };
  function renderReplayData(data) {
    const box=document.querySelector('#replay-result');
    if(!box || !data)return;
    renderReplayControls(data);
    renderReplayGraph(data);
    const objects=Object.values(data.objects||{});
    const objectCount=document.querySelector('#replay-object-count'); if(objectCount)objectCount.textContent=`${objects.length} OBJECTS`;
    const counts=objects.reduce((acc,o)=>{const status=classFor(o.phase);acc.total++;acc[status]=(acc[status]||0)+1;return acc;},{total:0,healthy:0,warning:0,critical:0});
    box.innerHTML=objects.length?`<div class="replay-object-summary"><div><strong>${counts.total}</strong><span>objects</span></div><div class="healthy"><strong>${counts.healthy||0}</strong><span>healthy</span></div><div class="warning"><strong>${counts.warning||0}</strong><span>changed</span></div><div class="critical"><strong>${counts.critical||0}</strong><span>degraded</span></div></div><div class="replay-object-list">${objects.map(o=>{const kind=kindFor(o);const phase=o.phase||(['ConfigMap','Secret','Service','PersistentVolumeClaim'].includes(kind)?'Present':'Unknown');const detail=o.status_reason||o.status_message;return `<div class="setting" title="${escValue(o.status_message||'')}"><div><h3>${escValue(o.name)}</h3><p>${escValue(o.namespace||'cluster')} · ${escValue(kind)}${kind==='Deployment'?` · ${o.ready_count||0}/${o.replicas||0} ready`:''}${o.restarts?` · ${o.restarts} restarts`:''}${detail?` · ${escValue(detail)}`:''}</p></div><span class="status ${classFor(phase)==='healthy'?'healthy':classFor(phase)==='critical'?'failed':'pending'}">${escValue(phase)}</span></div>`;}).join('')}</div>`:emptyView('No objects in snapshot','No reconstructed objects were returned.');
    const eventBox=document.querySelector('#replay-events'); if(eventBox)eventBox.innerHTML=events(state.replayEvents||[]);
    const eventMeta=document.querySelector('#replay-events-meta'); if(eventMeta)eventMeta.textContent=`${(state.replayEvents||[]).length} EVENTS · BEFORE SELECTED TIME`;
  }
  function renderReplayControls(data) {
    const host=document.querySelector('#replay-graph-controls');
    if(host)host.outerHTML=replayControls(data);
  }
  function applyReplayGraphFilter(scope, namespace) {
    if(!state.replayData)return;
    if(scope!==undefined)state.replayGraphScope=scope||'application';
    if(namespace!==undefined)state.replayGraphNamespace=namespace||'';
    renderReplayControls(state.replayData);
    renderReplayGraph(state.replayData);
  }
  if(!window.__chronicleReplayFilterEvents){
    document.addEventListener('click', event=>{
      const scope=event.target.closest?.('[data-replay-scope]');
      if(scope){event.preventDefault();applyReplayGraphFilter(scope.dataset.replayScope);return;}
      const namespace=event.target.closest?.('[data-replay-namespace]');
      if(namespace){event.preventDefault();applyReplayGraphFilter(undefined,namespace.dataset.replayNamespace);const menu=document.querySelector('#replay-namespace-menu');if(menu)menu.removeAttribute('open');}
    });
    window.__chronicleReplayFilterEvents=true;
  }
  if(!window.__chronicleReplayEventEvents){
    document.addEventListener('click', event => { const row=event.target.closest?.('.replay-event'); if(row) { event.preventDefault(); openReplayEvent(row.dataset.event); } });
    document.addEventListener('keydown', event => { const row=event.target.closest?.('.replay-event'); if(row && (event.key==='Enter'||event.key===' ')) { event.preventDefault(); openReplayEvent(row.dataset.event); } });
    window.__chronicleReplayEventEvents=true;
  }
  function bindReplayGraphFilters() {}

  async function loadReplayView() {
    const box=document.querySelector('#replay-result'); if(!box)return;
    const requestID=Number(state.replayRequestID||0)+1;
    state.replayRequestID=requestID;
    if(state.replayAbortController)state.replayAbortController.abort();
    const controller=new AbortController();
    state.replayAbortController=controller;
    state.replayLoading=true;
    box.innerHTML=emptyView('Loading historical state','Reconstructing objects and dependency graph…');
    try {
      const from=new Date(new Date(state.replayAt).getTime()-24*60*60*1000).toISOString();
      const live=state.replayLive?'&live=1':'';
      const [response,eventResponse]=await Promise.all([api(`/api/replay?t=${encodeURIComponent(state.replayAt)}${live}`,{signal:controller.signal}),api(`/api/events?from=${encodeURIComponent(from)}&to=${encodeURIComponent(state.replayAt)}&limit=30`,{signal:controller.signal})]);
      const data=await response.json(); if(!response.ok)throw new Error(data.error||'Replay unavailable');
      const eventData=eventResponse.ok?await eventResponse.json():{events:[]};
      if(requestID!==state.replayRequestID)return;
      state.replayData=data; state.replayEvents=eventData.events||[];
      state.replayDataLive=Boolean(state.replayLive);
      renderReplayData(data);
    } catch(err) {
      if(err.name==='AbortError')return;
      if(requestID!==state.replayRequestID)return;
      box.innerHTML=emptyView('Replay unavailable',err.message);
    } finally {
      if(requestID===state.replayRequestID){
        state.replayLoading=false;
        state.replayAbortController=null;
      }
    }
  }
  function bindReplay() {
    const slider=document.querySelector('#replay-range');
    if(slider){
      slider.oninput=()=>{setPosition(slider.value); const label=document.querySelector('#replay-time'); if(label)label.textContent=formatTime(state.replayAt);};
      slider.onchange=()=>window.render();
    }
    document.querySelector('#replay-step-back')?.addEventListener('click',()=>{setPosition(position()-1); window.render();});
    document.querySelector('#replay-step-forward')?.addEventListener('click',()=>{setPosition(position()+1); window.render();});
    const nowButton=document.querySelector('#replay-now');
    if(nowButton){nowButton.onclick=null;nowButton.addEventListener('click',()=>{setPosition(60); state.replayLive=true; window.render();});}
    document.querySelector('#replay-play')?.addEventListener('click',()=>{state.replayPlaying=!state.replayPlaying; if(state.replayPlaying){window.__replayTimer=setInterval(()=>{if(position()>=60){clearInterval(window.__replayTimer);state.replayPlaying=false;window.render();return;}setPosition(position()+1);window.render();},900);}else{clearInterval(window.__replayTimer);window.__replayTimer=null;} window.render();});
    if(replayDataMatchesSelection(state.replayData))renderReplayData(state.replayData);
    else if(document.querySelector('#replay-result'))loadReplayView();
    bindReplayGraphFilters();
  }
  window.replay = replayView;
  window.loadReplay = loadReplayView;
  const originalBind = window.bindView;
  window.bindView = function(){ if(originalBind)originalBind(); if(state.page==='replay')bindReplay(); };
  if(state.page==='replay' && window.render) window.render();
}());
