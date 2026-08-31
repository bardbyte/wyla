/** Cosmos: the sky, from the compiler's graph_map.json (positions
 * baked at compile; this page only renders). three.js is vendored
 * locally: no CDN, works offline. Encoding: size = usage, glow =
 * trust tier, gold star = held by multiple domains. Click a body →
 * the rail, with a real link to its profile. */

import { api } from "../api.js";
import { card, esc, loading, tierChip, unavailable } from "../ui.js";

const TIER_COLOR = { ha: 0x0e7a55, gr: 0x1f9e78, in: 0xc9962e, gu: 0x8b98b0 };
const TIER_GLOW = { ha: 0.9, gr: 0.55, in: 0.45, gu: 0.15 };
const KINDS = ["joins", "membership", "computed-from", "all"];

export async function renderCosmos(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Understand · Graph Cosmos</span>
      <span class="spacer"></span><span class="muted" id="cosmos-note"></span>
    </div>
    <div id="cosmos-body">${loading()}</div>`;

  const [payload, three] = await Promise.all([
    api.graphMap(),
    import("../../vendor/three.module.min.js"),
  ]);
  const body = outlet.querySelector("#cosmos-body");
  if (!body) return;
  if (!payload.available) { body.innerHTML = unavailable(payload.reason); return; }
  const THREE = three;
  outlet.querySelector("#cosmos-note").textContent =
    `${payload.nodes.length} bodies · ${
      payload.meta.truncated.mined_metrics} mined metrics counted, not drawn`;

  body.innerHTML = `
    <div class="cosmos-row">
      <div class="cosmos-wrap">
        <div class="pills" id="edge-pills"></div>
        <div class="cosmos-stage" id="stage"></div>
      </div>
      <div class="cosmos-rail" id="rail"></div>
    </div>`;
  const rail = body.querySelector("#rail");
  const drawRail = (node) => {
    if (!node) {
      rail.innerHTML = card("ENCODING", `
        <p class="muted">${esc(payload.meta.encoding)}</p>
        <p class="muted">drag to orbit · wheel to zoom · click a body
          to read it</p>`);
      return;
    }
    if (node.kind === "domain") {
      rail.innerHTML = card("DOMAIN", `
        <div class="profile-title mono">${esc(node.label)}</div>
        ${tierChip(node.tier, node.tier === "ha"
          ? "steward-mapped" : "unmapped")}
        <div class="muted">${esc(node.sub ?? `${node.usage} tables`)}</div>
        <div class="muted">every dashed tether is a table this domain
          claims; a gold star carries two</div>
        <a class="btn" href="#/semantics">browse its tables →</a>`);
      return;
    }
    rail.innerHTML = card(node.kind.toUpperCase(), `
      <div class="profile-title mono">${esc(node.label)}</div>
      ${tierChip(node.tier)}
      <div class="muted">well ${esc(node.well)}${
        node.star ? " · ★ held by multiple domains" : ""}</div>
      <div class="muted">usage <span class="mono">${node.usage}</span>${
        node.kind === "table" && node.metrics_here !== undefined
          ? ` · ${node.metrics_here} metrics here` : ""}${
        node.kind === "metric" && node.status
          ? ` · ${esc(node.status)}` : ""}</div>
      <a class="btn" href="#/${node.kind === "table"
        ? `table/${encodeURIComponent(node.id.replace(/^table:/, ""))}`
        : `metric/${encodeURIComponent(node.id)}`}">open profile →</a>`);
  };
  drawRail(null);

  const host = body.querySelector("#stage");
  const dark =
    document.documentElement.dataset.theme === "dark" ||
    (!document.documentElement.dataset.theme &&
      matchMedia("(prefers-color-scheme: dark)").matches);
  const width = host.clientWidth || 900;
  const height = host.clientHeight || 560;
  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
  renderer.setSize(width, height);
  host.appendChild(renderer.domElement);
  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(50, width / height, 0.1, 500);
  scene.add(new THREE.AmbientLight(0xffffff, 1.6));
  const dirLight = new THREE.DirectionalLight(0xffffff, 1.2);
  dirLight.position.set(10, 20, 10);
  scene.add(dirLight);
  const root = new THREE.Group();
  scene.add(root);

  // shape = kind: domains are octahedral hubs at their well centers,
  // tables spheres, metrics small orbiting spheres: one constellation
  const sphere = new THREE.SphereGeometry(1, 20, 20);
  const octa = new THREE.OctahedronGeometry(1, 0);
  const meshes = payload.nodes.map((node) => {
    const color = node.star ? 0xe8c98a : TIER_COLOR[node.tier] ?? 0x8b98b0;
    const mesh = new THREE.Mesh(
      node.kind === "domain" ? octa : sphere,
      new THREE.MeshStandardMaterial({
        color, emissive: color,
        emissiveIntensity: (TIER_GLOW[node.tier] ?? 0.2)
          * (node.kind === "domain" ? 0.35 : 0.6),
        roughness: 0.4,
        transparent: node.kind === "domain",
        opacity: node.kind === "domain" ? 0.85 : 1,
      }));
    const scale = node.kind === "domain"
      ? Math.min(1.0 + Math.sqrt(Math.max(node.usage, 1)) / 4, 2.2)
      : node.star ? 1.15
        : Math.min(0.3 + Math.sqrt(Math.max(node.usage, 1)) / 30, 1.6);
    mesh.scale.setScalar(node.kind === "metric" ? scale * 0.6 : scale);
    mesh.position.set(...node.pos);
    mesh.userData = node;
    if (node.star) {
      mesh.add(new THREE.Mesh(new THREE.SphereGeometry(1.7, 20, 20),
        new THREE.MeshBasicMaterial({
          color: 0xe8c98a, transparent: true, opacity: 0.18 })));
    }
    root.add(mesh);
    return mesh;
  });

  const positions = new Map(payload.nodes.map((n) =>
    [n.id, new THREE.Vector3(...n.pos)]));
  const edgeGroups = {};
  for (const kind of ["joins", "membership", "computed-from"]) {
    const points = [];
    for (const edge of payload.edges) {
      if (edge.kind !== kind) continue;
      const a = positions.get(edge.a);
      const b = positions.get(edge.b);
      if (a && b) points.push(a, b);
    }
    // each kind gets its own hue so the toggle visibly changes the
    // sky: joins in the accent blue, membership as slate dashed
    // tethers, computed-from in the data hue
    const KIND_STYLE = {
      "joins": { color: dark ? 0x57a9f2 : 0x006fcf, opacity: 0.55 },
      "membership": { color: dark ? 0x9db4e6 : 0x5b6b8f, opacity: 0.6 },
      "computed-from": { color: dark ? 0x5e8df0 : 0x3b6fe0,
                         opacity: 0.4 },
    };
    const style = KIND_STYLE[kind];
    const line = new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints(points),
      kind === "membership"
        ? new THREE.LineDashedMaterial({
            transparent: true, opacity: style.opacity,
            color: style.color, dashSize: 0.7, gapSize: 0.5 })
        : new THREE.LineBasicMaterial({
            transparent: true, opacity: style.opacity,
            color: style.color }));
    if (kind === "membership") line.computeLineDistances();
    line.visible = kind === "joins";
    edgeGroups[kind] = line;
    root.add(line);
  }
  // adjacency for pick-highlight: click a body and its neighborhood
  // lights up while the rest of the sky dims
  const neighbors = new Map();
  for (const edge of payload.edges) {
    if (!neighbors.has(edge.a)) neighbors.set(edge.a, new Set());
    if (!neighbors.has(edge.b)) neighbors.set(edge.b, new Set());
    neighbors.get(edge.a).add(edge.b);
    neighbors.get(edge.b).add(edge.a);
  }
  let highlight = null;
  const clearHighlight = () => {
    if (highlight) { root.remove(highlight); highlight = null; }
    for (const mesh of meshes) {
      const node = mesh.userData;
      mesh.material.opacity = node.kind === "domain" ? 0.85 : 1;
      mesh.material.transparent = node.kind === "domain";
      mesh.material.emissiveIntensity =
        (TIER_GLOW[node.tier] ?? 0.2)
        * (node.kind === "domain" ? 0.35 : 0.6);
    }
  };
  const highlightNode = (node) => {
    clearHighlight();
    const hood = neighbors.get(node.id) ?? new Set();
    for (const mesh of meshes) {
      const other = mesh.userData;
      if (other.id === node.id) {
        mesh.material.emissiveIntensity = 1.4;
        continue;
      }
      if (!hood.has(other.id)) {
        mesh.material.transparent = true;
        mesh.material.opacity = 0.15;
      }
    }
    const points = [];
    for (const edge of payload.edges) {
      if (edge.a !== node.id && edge.b !== node.id) continue;
      const a = positions.get(edge.a);
      const b = positions.get(edge.b);
      if (a && b) points.push(a, b);
    }
    highlight = new THREE.LineSegments(
      new THREE.BufferGeometry().setFromPoints(points),
      new THREE.LineBasicMaterial({
        transparent: true, opacity: 0.95, color: 0xe8c98a }));
    root.add(highlight);
  };
  let edgeKind = "joins";
  const pills = body.querySelector("#edge-pills");
  const edgeCounts = {};
  for (const edge of payload.edges)
    edgeCounts[edge.kind] = (edgeCounts[edge.kind] ?? 0) + 1;
  edgeCounts.all = payload.edges.length;
  const drawPills = () => {
    pills.innerHTML = KINDS.map((k) =>
      `<button class="pill ${edgeKind === k ? "on" : ""}"
        data-k="${k}">${k}</button>`).join("")
      + `<span class="muted" id="edge-count">${
        edgeCounts[edgeKind] ?? 0} edges lit</span>`;
  };
  pills.addEventListener("click", (e) => {
    const k = e.target?.dataset?.k;
    if (!k) return;
    edgeKind = k;
    drawPills();
    for (const [kind, line] of Object.entries(edgeGroups))
      line.visible = edgeKind === "all" || edgeKind === kind;
  });
  drawPills();

  const labels = payload.wells.map((well) => {
    const el = document.createElement("div");
    el.className = "cosmos-label";
    el.style.color = dark ? "#cdd9f0" : "#4a5568";
    el.innerHTML = `${esc(well.label)}<br><span>${esc(well.sub)}</span>`;
    host.appendChild(el);
    return {
      el,
      pos: new THREE.Vector3(...well.center)
        .add(new THREE.Vector3(0, 10, 0)),
    };
  });

  const rot = { x: -0.12, y: 0.3 };
  let dist = 46;
  let drag = null;
  let dragJustEnded = false;         // pointerup precedes click
  renderer.domElement.style.cursor = "grab";
  const onDown = (e) => { drag = { x: e.clientX, y: e.clientY, moved: false }; };
  const ray = new THREE.Raycaster();
  const pointer = new THREE.Vector2();
  const castAt = (e) => {
    const rect = renderer.domElement.getBoundingClientRect();
    pointer.set(((e.clientX - rect.left) / rect.width) * 2 - 1,
      -((e.clientY - rect.top) / rect.height) * 2 + 1);
    ray.setFromCamera(pointer, camera);
    return ray.intersectObjects(meshes, false)[0];
  };
  // hover: the sky answers the pointer: cursor, a name chip, a glow
  const hoverChip = document.createElement("div");
  hoverChip.className = "cosmos-hover";
  hoverChip.hidden = true;
  host.appendChild(hoverChip);
  let hovered = null;
  const unhover = () => {
    if (!hovered) return;
    hovered.scale.multiplyScalar(1 / 1.25);
    hovered = null;
    hoverChip.hidden = true;
    if (!drag) renderer.domElement.style.cursor = "grab";
  };
  const onMove = (e) => {
    if (drag) {
      const dx = e.clientX - drag.x;
      const dy = e.clientY - drag.y;
      if (Math.abs(dx) + Math.abs(dy) > 3) drag.moved = true;
      rot.y += dx * 0.005;
      rot.x = Math.max(-1.2, Math.min(1.2, rot.x + dy * 0.004));
      drag.x = e.clientX;
      drag.y = e.clientY;
      return;
    }
    if (e.target !== renderer.domElement) { unhover(); return; }
    const hit = castAt(e);
    if (hit?.object !== hovered) {
      unhover();
      if (hit) {
        hovered = hit.object;
        hovered.scale.multiplyScalar(1.25);
        renderer.domElement.style.cursor = "pointer";
      }
    }
    if (hovered) {
      const node = hovered.userData;
      const rect = host.getBoundingClientRect();
      hoverChip.textContent = `${node.label} · ${node.kind}`;
      hoverChip.style.left = `${e.clientX - rect.left + 14}px`;
      hoverChip.style.top = `${e.clientY - rect.top + 10}px`;
      hoverChip.hidden = false;
    }
  };
  const onUp = () => {
    dragJustEnded = Boolean(drag?.moved);
    drag = null;
  };
  const onWheel = (e) => {
    e.preventDefault();
    dist = Math.max(18, Math.min(90, dist + e.deltaY * 0.04));
  };
  renderer.domElement.addEventListener("pointerdown", onDown);
  window.addEventListener("pointermove", onMove);
  window.addEventListener("pointerup", onUp);
  renderer.domElement.addEventListener("wheel", onWheel, { passive: false });
  const onClick = (e) => {
    if (dragJustEnded) { dragJustEnded = false; return; }
    const hit = castAt(e);
    if (!hit) {
      // empty space resets the sky
      clearHighlight();
      drawRail(null);
      return;
    }
    highlightNode(hit.object.userData);
    drawRail(hit.object.userData);
  };
  renderer.domElement.addEventListener("click", onClick);

  const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;
  let raf = 0;
  const tick = () => {
    raf = requestAnimationFrame(tick);
    if (!reduced && !drag) rot.y += 0.0008;
    camera.position.set(
      Math.sin(rot.y) * dist * Math.cos(rot.x),
      Math.sin(rot.x) * dist + 4,
      Math.cos(rot.y) * dist * Math.cos(rot.x));
    camera.lookAt(0, 2, 0);
    renderer.render(scene, camera);
    for (const label of labels) {
      const projected = label.pos.clone().project(camera);
      label.el.style.left = `${(projected.x * 0.5 + 0.5)
        * renderer.domElement.clientWidth}px`;
      label.el.style.top = `${(-projected.y * 0.5 + 0.5)
        * renderer.domElement.clientHeight}px`;
      label.el.style.opacity = projected.z < 1 ? "1" : "0";
    }
  };
  tick();
  const resize = new ResizeObserver(() => {
    const w = host.clientWidth;
    const h = host.clientHeight;
    if (!w || !h) return;
    renderer.setSize(w, h);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
  });
  resize.observe(host);

  return () => {
    cancelAnimationFrame(raf);
    resize.disconnect();
    window.removeEventListener("pointermove", onMove);
    window.removeEventListener("pointerup", onUp);
    renderer.dispose();
  };
}
