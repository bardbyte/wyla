/* <synapse-cosmos> — three.js knowledge-graph cosmos.
   Attributes: theme="paper|navy", drift="on|off", edges="joins|membership|computed-from|all".
   Emits window CustomEvent 'cosmos-pick' {detail:{node}} on click. */
(() => {
  const TIER = {
    ha: { color: 0x0e7a55, glow: 0.9 }, gr: { color: 0x1f9e78, glow: 0.55 },
    in: { color: 0xc9962e, glow: 0.45 }, gu: { color: 0x8b98b0, glow: 0.15 },
    de: { color: 0xbe3a48, glow: 0.5 },
  };
  const DOMAINS = [
    { id: 'credit_risk', label: 'CREDIT RISK', sub: '41 tables · 62% grounded', center: [-16, 4, -4] },
    { id: 'merchant_services', label: 'MERCHANT SERVICES', sub: '33 tables', center: [14, 6, 2] },
    { id: 'deposits', label: 'DEPOSITS', sub: '27 tables', center: [-2, -10, 8] },
  ];
  function mulberry(seed) { return () => { seed |= 0; seed = seed + 0x6D2B79F5 | 0; let t = Math.imul(seed ^ seed >>> 15, 1 | seed); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }
  function makeGraph() {
    const rnd = mulberry(42), nodes = [], edges = [];
    const tiers = ['ha', 'gr', 'gr', 'in', 'in', 'in', 'gu', 'gu'];
    DOMAINS.forEach((d, di) => {
      const n = 16;
      for (let i = 0; i < n; i++) {
        const t = tiers[Math.floor(rnd() * tiers.length)];
        const r = 4.5 + rnd() * 4.5, th = rnd() * Math.PI * 2, ph = Math.acos(2 * rnd() - 1);
        nodes.push({
          id: d.id + '_t' + i, name: ['auth_events', 'fx_rates', 'stmt_daily', 'limits', 'chargebacks', 'merch_dim', 'settle_log', 'accounts'][Math.floor(rnd() * 8)] + '_' + di + i,
          domain: d.id, domains: [d.label], tier: t, usage: Math.floor(rnd() * 4000) + 50,
          kind: rnd() < 0.85 ? 'table' : 'metric',
          pos: [d.center[0] + r * Math.sin(ph) * Math.cos(th), d.center[1] + r * Math.sin(ph) * Math.sin(th), d.center[2] + r * Math.cos(ph)],
        });
      }
    });
    // the shared star: held between Credit Risk and Merchant Services
    nodes.push({ id: 'gcs', name: 'gcs_transactions', domain: 'shared', domains: ['Credit Risk', 'Merchant Services'], tier: 'gr', usage: 4200, kind: 'table', pos: [-1, 6, -1], star: true });
    const byDomain = (id) => nodes.filter((x) => x.domain === id);
    nodes.forEach((a, i) => {
      if (a.star) return;
      const peers = byDomain(a.domain);
      for (let k = 0; k < 2; k++) {
        const b = peers[Math.floor(rnd() * peers.length)];
        if (b && b !== a) edges.push({ a: a.id, b: b.id, kind: rnd() < 0.6 ? 'joins' : (rnd() < 0.5 ? 'membership' : 'computed-from') });
      }
      if (rnd() < 0.22) edges.push({ a: a.id, b: 'gcs', kind: 'joins' });
    });
    return { nodes, edges };
  }

  class Cosmos extends HTMLElement {
    static observedAttributes = ['theme', 'drift', 'edges'];
    connectedCallback() { if (!this._init) { this._init = true; this.boot(); } }
    attributeChangedCallback() { if (this._ready) this.applyAttrs(); }
    disconnectedCallback() { cancelAnimationFrame(this._raf); }
    async boot() {
      this.style.cssText += ';display:block;position:relative;overflow:hidden;border-radius:14px;';
      const THREE = await import('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.module.js');
      this.THREE = THREE;
      const w = this.clientWidth || 900, h = this.clientHeight || 520;
      const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
      renderer.setPixelRatio(Math.min(devicePixelRatio, 2)); renderer.setSize(w, h);
      this.appendChild(renderer.domElement);
      const scene = new THREE.Scene();
      const camera = new THREE.PerspectiveCamera(50, w / h, 0.1, 500);
      camera.position.set(0, 4, 46);
      scene.add(new THREE.AmbientLight(0xffffff, 1.6));
      const dir = new THREE.DirectionalLight(0xffffff, 1.2); dir.position.set(10, 20, 10); scene.add(dir);
      const root = new THREE.Group(); scene.add(root);
      const { nodes, edges } = makeGraph();
      this.graph = { nodes, edges };
      const sph = new THREE.SphereGeometry(1, 20, 20);
      this.meshes = nodes.map((nd) => {
        const t = TIER[nd.tier];
        const m = new THREE.Mesh(sph, new THREE.MeshStandardMaterial({ color: t.color, emissive: t.color, emissiveIntensity: t.glow * 0.6, roughness: 0.4 }));
        const s = nd.star ? 1.15 : 0.3 + Math.sqrt(nd.usage) / 90;
        m.scale.setScalar(s); m.position.set(...nd.pos); m.userData = nd;
        if (nd.star) { const halo = new THREE.Mesh(new THREE.SphereGeometry(1.7, 20, 20), new THREE.MeshBasicMaterial({ color: 0xe8c98a, transparent: true, opacity: 0.18 })); m.add(halo); m.material.color.set(0xe8c98a); m.material.emissive.set(0xe8c98a); }
        root.add(m); return m;
      });
      this.edgeGroups = {};
      ['joins', 'membership', 'computed-from'].forEach((kind) => {
        const pts = [];
        edges.filter((e) => e.kind === kind).forEach((e) => {
          const A = nodes.find((n) => n.id === e.a), B = nodes.find((n) => n.id === e.b);
          pts.push(new THREE.Vector3(...A.pos), new THREE.Vector3(...B.pos));
        });
        const g = new THREE.BufferGeometry().setFromPoints(pts);
        const line = new THREE.LineSegments(g, new THREE.LineBasicMaterial({ transparent: true, opacity: 0.28 }));
        this.edgeGroups[kind] = line; root.add(line);
      });
      // labels
      this.labels = DOMAINS.map((d) => {
        const el = document.createElement('div');
        el.style.cssText = 'position:absolute;pointer-events:none;font:600 11px "IBM Plex Mono",monospace;letter-spacing:.06em;white-space:nowrap';
        el.innerHTML = d.label + '<br><span style="font-weight:400;font-size:9.5px;opacity:.65">' + d.sub + '</span>';
        this.appendChild(el); return { el, pos: new THREE.Vector3(...d.center).add(new THREE.Vector3(0, 9, 0)) };
      });
      const gcsEl = document.createElement('div');
      gcsEl.style.cssText = 'position:absolute;pointer-events:none;font:600 10.5px "IBM Plex Mono",monospace;white-space:nowrap';
      gcsEl.innerHTML = 'gcs_transactions<br><span style="font-weight:400;font-size:9.5px;opacity:.65">shared — held between both wells</span>';
      this.appendChild(gcsEl);
      this.labels.push({ el: gcsEl, pos: new THREE.Vector3(-1, 8.4, -1) });
      // interaction: drag-rotate, wheel-zoom, click-pick
      this.rot = { x: -0.12, y: 0.3 }; this.dist = 46; let drag = null;
      renderer.domElement.style.cursor = 'grab';
      renderer.domElement.addEventListener('pointerdown', (e) => { drag = { x: e.clientX, y: e.clientY, moved: false }; });
      window.addEventListener('pointermove', (e) => { if (!drag) return; const dx = e.clientX - drag.x, dy = e.clientY - drag.y; if (Math.abs(dx) + Math.abs(dy) > 3) drag.moved = true; this.rot.y += dx * 0.005; this.rot.x = Math.max(-1.2, Math.min(1.2, this.rot.x + dy * 0.004)); drag.x = e.clientX; drag.y = e.clientY; });
      window.addEventListener('pointerup', () => { drag = null; });
      renderer.domElement.addEventListener('wheel', (e) => { e.preventDefault(); this.dist = Math.max(18, Math.min(90, this.dist + e.deltaY * 0.04)); }, { passive: false });
      const ray = new THREE.Raycaster(), v2 = new THREE.Vector2();
      renderer.domElement.addEventListener('click', (e) => {
        if (drag && drag.moved) return;
        const r = renderer.domElement.getBoundingClientRect();
        v2.set(((e.clientX - r.left) / r.width) * 2 - 1, -((e.clientY - r.top) / r.height) * 2 + 1);
        ray.setFromCamera(v2, camera);
        const hit = ray.intersectObjects(this.meshes, false)[0];
        if (hit) {
          if (this.sel) this.sel.material.emissiveIntensity = TIER[this.sel.userData.tier].glow * 0.6;
          this.sel = hit.object; this.sel.material.emissiveIntensity = 1.4;
          window.dispatchEvent(new CustomEvent('cosmos-pick', { detail: { node: hit.object.userData } }));
        }
      });
      this.renderer = renderer; this.scene = scene; this.camera = camera; this.root = root;
      this._ready = true; this.applyAttrs();
      const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
      const tick = () => {
        this._raf = requestAnimationFrame(tick);
        if (this.driftOn && !reduced && !drag) this.rot.y += 0.0008;
        camera.position.set(Math.sin(this.rot.y) * this.dist * Math.cos(this.rot.x), Math.sin(this.rot.x) * this.dist + 4, Math.cos(this.rot.y) * this.dist * Math.cos(this.rot.x));
        camera.lookAt(0, 2, 0);
        renderer.render(scene, camera);
        for (const L of this.labels) {
          const p = L.pos.clone().project(camera);
          L.el.style.left = ((p.x * 0.5 + 0.5) * renderer.domElement.clientWidth) + 'px';
          L.el.style.top = ((-p.y * 0.5 + 0.5) * renderer.domElement.clientHeight) + 'px';
          L.el.style.opacity = p.z < 1 ? 1 : 0;
        }
      };
      tick();
      new ResizeObserver(() => { const W = this.clientWidth, H = this.clientHeight; if (!W || !H) return; renderer.setSize(W, H); camera.aspect = W / H; camera.updateProjectionMatrix(); }).observe(this);
    }
    applyAttrs() {
      const navy = this.getAttribute('theme') === 'navy';
      this.style.background = navy ? 'radial-gradient(ellipse at 50% 40%, #0a2472 0%, #00175a 70%)' : 'radial-gradient(ellipse at 50% 40%, #ffffff 0%, #eef1f6 75%)';
      const ink = navy ? '#cdd9f0' : '#4a5568';
      this.labels?.forEach((L) => { L.el.style.color = ink; });
      Object.values(this.edgeGroups || {}).forEach((l) => l.material.color.set(navy ? 0x8fa8d8 : 0x9aa8c0));
      this.driftOn = this.getAttribute('drift') !== 'off';
      const want = this.getAttribute('edges') || 'joins';
      Object.entries(this.edgeGroups || {}).forEach(([k, l]) => { l.visible = want === 'all' || want === k; });
    }
  }
  customElements.define('synapse-cosmos', Cosmos);
})();
