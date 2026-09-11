/* Phase 3 replay UI: time, graph, state colours, and playback. */
(function () {
  const stateRef = window.__chronicleState || null;
  // app.js keeps state private in the original build, so the helpers below
  // attach through the existing global functions and DOM state.
  const getState = () => window.chronicleState || stateRef;
  const escValue = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const classFor = phase => ['failed','degraded','restarting','notready','unhealthy'].includes(String(phase||'').toLowerCase()) ? 'critical' : ['pending','changed','unknown'].includes(String(phase||'').toLowerCase()) ? 'warning' : 'healthy';
  const key = n => `${n?.Namespace ?? n?.namespace ?? ''}/${n?.Kind ?? n?.kind ?? ''}/${n?.Name ?? n?.name ?? ''}`;
  const timeFor = value => new Date(Date.now() - (60 - Number(value)) * 60000).toISOString();
  const formatTime = value => new Date(value).toLocaleString([], {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'});

  // This file is loaded after app.js; replacing the view functions keeps the
  // embedded frontend dependency-free while making replay fully interactive.
  const renderReplayGraph = data => {
    const host = document.querySelector('#replay-graph'); if (!host) return;
    const objects = Object.entries(data.objects || {}).map(([id, o]) => ({id, ...o}));
    const nodes = new Map(objects.map(o => [key(o) || o.id, o]));
    (data.edges || []).forEach(e => [e.From,e.To].forEach(n => { if (n && !nodes.has(key(n))) nodes.set(key(n), {...n, Phase:'Unknown'}); }));
    const list = [...nodes.values()], positions = new Map(list.map((n,i) => [key(n)||n.id, {x:25+(i%5)*230,y:25+Math.floor(i/5)*105}]));
    const lines = (data.edges || []).map(e => { const a=positions.get(key(e.From)), b=positions.get(key(e.To)); if(!a||!b)return ''; const ca=classFor(nodes.get(key(e.From))?.phase ?? nodes.get(key(e.From))?.Phase), cb=classFor(nodes.get(key(e.To))?.phase ?? nodes.get(key(e.To))?.Phase); const tone=ca==='critical'||cb==='critical'?'critical':ca==='warning'||cb==='warning'?'warning':''; return `<line class="replay-edge edge-${tone}" x1="${a.x+85}" y1="${a.y+31}" x2="${b.x+85}" y2="${b.y+31}" marker-end="url(#replay-arrow)"></line>`; }).join('');
    const markup = list.map(n => { const p=positions.get(key(n)||n.id); const status=classFor(n.phase ?? n.Phase); return `<div class="replay-node state-${status}" style="left:${p.x}px;top:${p.y}px"><h3>${escValue(n.name ?? n.Name ?? n.id)}</h3><small>${escValue(n.kind ?? n.Kind ?? 'Resource')} · ${escValue(n.namespace ?? n.Namespace ?? 'cluster')} · ${escValue(n.phase ?? n.Phase ?? 'Unknown')}</small></div>`; }).join('');
    host.innerHTML=`<div class="replay-graph-canvas"><svg><defs><marker id="replay-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0,0 L7,3.5 L0,7 z" fill="#536b7f"></path></marker></defs>${lines}</svg>${markup}</div><div class="replay-legend"><span style="color:#65c18c">● healthy</span><span style="color:#e0ad61">● warning / changed</span><span style="color:#e07070">● degraded / failed</span><span>${list.length} nodes · ${(data.edges||[]).length} edges</span></div>`;
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
  const events = items => items.length ? items.map(e => `<div class="event-row"><div class="event-time">${formatTime(e.ingested_at)}</div><div class="event-source">${escValue(e.source||'system')}</div><div><p>${escValue(e.title||e.type)}</p><small>${escValue(e.namespace)}/${escValue(e.entity_kind)}/${escValue(e.entity_name)}</small></div></div>`).join('') : emptyView('No events before this point','No normalized events were recorded before the selected time.');
  const heading = (title, subtitle, action) => `<div class="page-heading"><div><div class="eyebrow">REPLAY</div><h1 class="page-title">${title}</h1><p class="page-subtitle">${subtitle}</p></div>${action||''}</div>`;
  const position = () => Number(state.replayPosition ?? 30);
  const setPosition = value => { state.replayPosition=Math.max(0,Math.min(60,Number(value))); state.replayAt=timeFor(state.replayPosition); };
  const replayView = () => { const at=new Date(state.replayAt); return heading('Historical infrastructure state','Scrub through time to see graph relationships and health propagate.',`<button class="button" id="replay-play">${state.replayPlaying?'Pause':'Play'}</button> <button class="button" id="replay-now">Use current time</button>`)+`<section class="panel"><div class="timeline"><div class="replay-toolbar"><button class="button" id="replay-step-back">−1m</button><input id="replay-range" type="range" min="0" max="60" value="${position()}"><button class="button" id="replay-step-forward">+1m</button><span class="mono" id="replay-time">${formatTime(at)}</span></div><div class="timeline-line"><div class="timeline-point" style="left:${position()/60*100}%"></div></div><div class="timeline-labels"><span>1 hour ago</span><span>Graph and state update together</span><span>now</span></div></div></section><section class="panel" style="margin-top:14px"><div class="panel-head"><span class="panel-title">Dependency graph at ${formatTime(at)}</span><span class="panel-meta">HISTORICAL SNAPSHOT</span></div><div id="replay-graph" class="replay-graph">${emptyView('Loading replay graph','Reconstructing the selected point in time…')}</div></section><div class="grid replay-state" style="margin-top:14px"><section class="panel"><div class="panel-head"><span class="panel-title">Reconstructed state</span><span class="panel-meta">${Object.keys(state.replayData?.objects||{}).length} OBJECTS</span></div><div id="replay-result">${emptyView('Choose a moment to replay','Move the scrubber to load historical state.')}</div></section><section class="panel"><div class="panel-head"><span class="panel-title">Events before this point</span><span class="panel-meta">EVENT STORE</span></div><div class="event-list">${events(state.events.filter(e=>new Date(e.ingested_at)<=at).slice(0,8))}</div></section></div>`; };
  async function loadReplayView() {
    const box=document.querySelector('#replay-result'); if(!box)return;
    box.innerHTML=emptyView('Loading historical state','Reconstructing objects and dependency graph…');
    try { const response=await fetch(`/api/replay?t=${encodeURIComponent(state.replayAt)}`), data=await response.json(); if(!response.ok)throw new Error(data.error||'Replay unavailable'); state.replayData=data; renderReplayGraph(data); const objects=Object.values(data.objects||{}); box.innerHTML=objects.length?`<div class="object-card">${objects.slice(0,20).map(o=>`<div class="setting"><div><h3>${escValue(o.name)}</h3><p>${escValue(o.namespace||'cluster')} · ${escValue(o.kind)}</p></div><span class="status ${classFor(o.phase)==='healthy'?'healthy':classFor(o.phase)==='critical'?'failed':'pending'}">${escValue(o.phase||'UNKNOWN')}</span></div>`).join('')}</div>`:emptyView('No objects in snapshot','No reconstructed objects were returned.'); } catch(err) { box.innerHTML=emptyView('Replay unavailable',err.message); }
  }
  function bindReplay() {
    const slider=document.querySelector('#replay-range');
    if(slider){slider.oninput=()=>{setPosition(slider.value); const label=document.querySelector('#replay-time'); if(label)label.textContent=formatTime(state.replayAt); loadReplayView();};}
    document.querySelector('#replay-step-back')?.addEventListener('click',()=>{setPosition(position()-1); window.render(); loadReplayView();});
    document.querySelector('#replay-step-forward')?.addEventListener('click',()=>{setPosition(position()+1); window.render(); loadReplayView();});
    document.querySelector('#replay-now')?.addEventListener('click',()=>{setPosition(60); window.render(); loadReplayView();});
    document.querySelector('#replay-play')?.addEventListener('click',()=>{state.replayPlaying=!state.replayPlaying; if(state.replayPlaying){window.__replayTimer=setInterval(()=>{if(position()>=60){clearInterval(window.__replayTimer);state.replayPlaying=false;window.render();return;}setPosition(position()+1);window.render();loadReplayView();},900);}else{clearInterval(window.__replayTimer);window.__replayTimer=null;} window.render(); loadReplayView();});
    if(document.querySelector('#replay-result')&&!state.replayData)loadReplayView();
  }
  window.replay = replayView;
  window.loadReplay = loadReplayView;
  const originalBind = window.bindView;
  window.bindView = function(){ if(originalBind)originalBind(); if(state.page==='replay')bindReplay(); };
  if(state.page==='replay' && window.render) window.render();
}());
