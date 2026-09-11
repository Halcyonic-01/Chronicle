(function () {
  const layoutKey = 'chronicle.graph.layout.v2';
  const nodeWidth = 176, nodeHeight = 82;
  const keyOf = n => `${n.Namespace}/${n.Kind}/${n.Name}`;
  const readLayout = () => { try { return JSON.parse(localStorage.getItem(layoutKey) || '{}'); } catch (_) { return {}; } };
  const saveLayout = layout => localStorage.setItem(layoutKey, JSON.stringify(layout));
  const autoLayout = (nodes, edges) => {
    const byKey = new Map(nodes.map(n => [keyOf(n), n]));
    const incoming = new Map(nodes.map(n => [keyOf(n), 0]));
    const outgoing = new Map(nodes.map(n => [keyOf(n), []]));
    edges.forEach(e => { const from = keyOf(e.From), to = keyOf(e.To); if (!byKey.has(from) || !byKey.has(to)) return; outgoing.get(from).push(to); incoming.set(to, (incoming.get(to) || 0) + 1); });
    const roots = nodes.map(keyOf).filter(k => (incoming.get(k) || 0) === 0);
    const layer = new Map(), queue = roots.length ? roots : nodes.map(keyOf);
    queue.forEach(k => layer.set(k, 0));
    for (let i = 0; i < queue.length; i++) (outgoing.get(queue[i]) || []).forEach(next => { const depth = (layer.get(queue[i]) || 0) + 1; if (!layer.has(next)) { layer.set(next, depth); queue.push(next); } });
    nodes.forEach(n => { if (!layer.has(keyOf(n))) layer.set(keyOf(n), 0); });
    const groups = new Map(); nodes.forEach(n => { const l = layer.get(keyOf(n)); if (!groups.has(l)) groups.set(l, []); groups.get(l).push(n); });
    const positions = {};
    for (const [l, group] of groups) { group.sort((a,b) => keyOf(a).localeCompare(keyOf(b))); group.forEach((n, i) => { positions[keyOf(n)] = { x: 70 + l * 285, y: 70 + i * 125 }; }); }
    return positions;
  };

  window.graph = function () {
    const g = state.graph || { nodes: [], edges: [] }, layout = readLayout();
    const nodes = g.nodes || [], edges = g.edges || [];
    const automatic = autoLayout(nodes, edges);
    const maxX = Math.max(1500, ...nodes.map(n => (automatic[keyOf(n)] || {x:0}).x + 300));
    const maxY = Math.max(900, ...nodes.map(n => (automatic[keyOf(n)] || {y:0}).y + 180));
    const nodeMarkup = nodes.length ? nodes.map((n, i) => {
      const p = layout[keyOf(n)] || automatic[keyOf(n)];
      return `<div class="graph-node draggable" data-node="${esc(keyOf(n))}" data-x="${p.x}" data-y="${p.y}" style="left:${p.x}px;top:${p.y}px"><h3>${esc(n.Name)}</h3><small><span class="dot dot-green"></span> ${esc(n.Kind)} · ${esc(n.Namespace)}</small></div>`;
    }).join('') : empty('No dependency edges', 'The graph collector has not stored active dependencies yet.');
    return pageHeading('PHASE 2 / DEPENDENCY GRAPH','Service relationships','Connected resources are arranged from upstream to downstream. Drag nodes to refine the layout.',`<button class="button" id="graph-auto">Auto layout</button>`)+`<section class="panel"><div class="graph-toolbar"><input class="input graph-search" id="graph-search" placeholder="Find a service, pod, or namespace…"><button class="button" id="graph-zoom-out">−</button><input class="graph-zoom" id="graph-zoom" type="range" min="35" max="140" value="100" title="Zoom graph"><button class="button" id="graph-zoom-in">+</button><button class="button" id="graph-fit">Fit all</button><button class="button" id="graph-reset">Reset</button><span class="graph-help">DRAG NODES · DRAG BACKGROUND TO PAN · CLICK A NODE FOR DETAILS</span></div><div class="graph-viewport" id="graph-viewport"><div class="graph-canvas" id="graph-canvas" style="width:${maxX}px;height:${maxY}px"><svg class="graph-edges" id="graph-edges"></svg>${nodeMarkup}</div></div><div class="graph-legend"><span><span class="dot dot-green"></span> observed resource</span><span>${nodes.length} NODES</span><span>${edges.length} EDGES</span><span>FLOW: LEFT → RIGHT</span></div></section><p class="page-subtitle" style="margin-top:12px">All live resources are included, including services without a dependency edge. Search narrows the view without deleting resources.</p>`;
  };

  function setupGraph() {
    const viewport = document.querySelector('#graph-viewport'), canvas = document.querySelector('#graph-canvas');
    if (!viewport || !canvas) return;
    const g = state.graph || { nodes: [], edges: [] }, nodes = g.nodes || [], edges = g.edges || [];
    let zoom = 1, panX = 0, panY = 0, selected = null, dragging = null, panning = null;
    const nodeMap = new Map(nodes.map(n => [keyOf(n), n]));
    const nodeEl = key => canvas.querySelector(`[data-node="${CSS.escape(key)}"]`);
    const updateTransform = () => { canvas.style.transform = `translate(${panX}px,${panY}px) scale(${zoom})`; };
    const edgePoint = (from, to) => {
      const dx = to.x - from.x, dy = to.y - from.y;
      if (!dx && !dy) return from;
      const scale = Math.min((nodeWidth / 2) / Math.max(Math.abs(dx), 0.001), (nodeHeight / 2) / Math.max(Math.abs(dy), 0.001));
      return { x: from.x + dx * scale, y: from.y + dy * scale };
    };
    const drawEdges = () => {
      const svg = document.querySelector('#graph-edges'); if (!svg) return;
      svg.innerHTML = edges.map(e => {
        const from = nodeEl(keyOf(e.From)), to = nodeEl(keyOf(e.To)); if (!from || !to || from.style.display === 'none' || to.style.display === 'none') return '';
        const fromCenter = { x: Number(from.dataset.x) + nodeWidth / 2, y: Number(from.dataset.y) + nodeHeight / 2 };
        const toCenter = { x: Number(to.dataset.x) + nodeWidth / 2, y: Number(to.dataset.y) + nodeHeight / 2 };
        const start = edgePoint(fromCenter, toCenter), end = edgePoint(toCenter, fromCenter);
        const evidence = (state.graphEvidence||[]).some(x => keyOf(x.From)===keyOf(e.From) && keyOf(x.To)===keyOf(e.To) && x.Kind===e.Kind);
        const active = selected && (selected === keyOf(e.From) || selected === keyOf(e.To));
        return `<line class="graph-edge ${evidence ? 'edge-evidence' : active ? 'edge-selected' : ''}" x1="${start.x}" y1="${start.y}" x2="${end.x}" y2="${end.y}" marker-end="url(#arrow)"></line>`;
      }).join('') + `<defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0,0 L7,3.5 L0,7 z" fill="#3b5164"></path></marker></defs>`;
    };
    const inspect = (key) => {
      document.querySelector('.graph-inspector')?.remove(); selected = key; drawEdges();
      const n = nodeMap.get(key); if (!n) return;
      const incoming = edges.filter(e => keyOf(e.To) === key).length, outgoing = edges.filter(e => keyOf(e.From) === key).length;
      viewport.insertAdjacentHTML('beforeend', `<div class="graph-inspector"><button class="close-inspector">×</button><h3>${esc(n.Name)}</h3><p>${esc(n.Kind)} · ${esc(n.Namespace)}</p><p>${incoming} upstream · ${outgoing} downstream</p></div>`);
      viewport.querySelector('.close-inspector').onclick = () => { selected = null; viewport.querySelector('.graph-inspector')?.remove(); drawEdges(); };
      canvas.querySelectorAll('.graph-node').forEach(el => el.classList.toggle('selected', el.dataset.node === key));
    };
    canvas.querySelectorAll('.graph-node').forEach(el => {
      el.addEventListener('pointerdown', ev => { ev.stopPropagation(); el.setPointerCapture(ev.pointerId); dragging = { el, key: el.dataset.node, startX: ev.clientX, startY: ev.clientY, x: Number(el.dataset.x), y: Number(el.dataset.y) }; });
      el.addEventListener('pointermove', ev => { if (!dragging || dragging.el !== el) return; el.dataset.x = dragging.x + (ev.clientX - dragging.startX) / zoom; el.dataset.y = dragging.y + (ev.clientY - dragging.startY) / zoom; el.style.left = `${el.dataset.x}px`; el.style.top = `${el.dataset.y}px`; drawEdges(); });
      el.addEventListener('pointerup', () => { if (!dragging || dragging.el !== el) return; const l = readLayout(); l[dragging.key] = { x: Number(el.dataset.x), y: Number(el.dataset.y) }; saveLayout(l); dragging = null; });
      el.addEventListener('click', ev => { ev.stopPropagation(); inspect(el.dataset.node); });
    });
    viewport.addEventListener('pointerdown', ev => { if (ev.target.closest('.graph-node') || ev.target.closest('.graph-inspector')) return; viewport.classList.add('is-panning'); panning = { x: ev.clientX, y: ev.clientY, px: panX, py: panY }; });
    viewport.addEventListener('pointermove', ev => { if (!panning) return; panX = panning.px + ev.clientX - panning.x; panY = panning.py + ev.clientY - panning.y; updateTransform(); });
    viewport.addEventListener('pointerup', () => { panning = null; viewport.classList.remove('is-panning'); });
    document.querySelector('#graph-zoom-in')?.addEventListener('click', () => { zoom = Math.min(1.4, zoom + .1); const slider = document.querySelector('#graph-zoom'); if (slider) slider.value = Math.round(zoom * 100); updateTransform(); });
    document.querySelector('#graph-zoom-out')?.addEventListener('click', () => { zoom = Math.max(.35, zoom - .1); const slider = document.querySelector('#graph-zoom'); if (slider) slider.value = Math.round(zoom * 100); updateTransform(); });
    document.querySelector('#graph-reset')?.addEventListener('click', () => { localStorage.removeItem(layoutKey); render(); });
    document.querySelector('#graph-auto')?.addEventListener('click', () => { localStorage.removeItem(layoutKey); render(); });
    document.querySelector('#graph-fit')?.addEventListener('click', () => { const scaleX = (viewport.clientWidth - 40) / canvas.offsetWidth, scaleY = (viewport.clientHeight - 40) / canvas.offsetHeight; zoom = Math.max(.35, Math.min(1, scaleX, scaleY)); const slider = document.querySelector('#graph-zoom'); if (slider) slider.value = Math.round(zoom * 100); panX = 20; panY = 20; updateTransform(); });
    document.querySelector('#graph-zoom')?.addEventListener('input', ev => { zoom = Number(ev.target.value) / 100; updateTransform(); });
    document.querySelector('#graph-search')?.addEventListener('input', ev => { const query = ev.target.value.toLowerCase().trim(); canvas.querySelectorAll('.graph-node').forEach(el => { const visible = !query || el.dataset.node.toLowerCase().includes(query) || el.textContent.toLowerCase().includes(query); el.style.display = visible ? 'block' : 'none'; }); drawEdges(); });
    drawEdges(); updateTransform();
  }
  const previousBind = window.bindView;
  window.bindView = function () { previousBind(); setupGraph(); };
  if (state.page === 'graph') render();
}());
