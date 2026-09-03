/** Cosmos — the knowledge-graph sky (F6 artboard + cosmos.js
 * encoding, fed by the compiler's graph_map.json — positions are
 * baked at compile time, this component only renders). Encoding:
 * size = usage · glow = trust tier · gold star = held by multiple
 * domains · edges toggle by kind. Drag orbits, wheel zooms, click
 * picks into the side rail. Reduced-motion disables drift. */

import { useEffect, useRef, useState } from "react";
import * as THREE from "three";
import type { Unavailable } from "../lib/meridian";
import { TIER_GLYPH } from "../lib/meridian";

interface MapNode {
  id: string; label: string; kind: "table" | "metric";
  tier: "ha" | "gr" | "in" | "gu"; usage: number; well: string;
  star: boolean; pos: [number, number, number];
  columns?: number | null; metrics_here?: number; status?: string;
  business_name?: string; business_unit?: string; lifecycle?: string;
  pii?: boolean; description?: string;
}
interface MapEdge {
  a: string; b: string; kind: string; source?: string; scope?: string;
}
interface Well {
  id: string; label: string; sub: string;
  center: [number, number, number];
}
interface GraphMapPayload {
  available: true; build_id: string;
  nodes: MapNode[]; edges: MapEdge[]; wells: Well[];
  meta: { truncated: Record<string, number>; encoding: string };
}

const TIER_COLOR: Record<MapNode["tier"], number> = {
  ha: 0x0e7a55, gr: 0x1f9e78, in: 0xc9962e, gu: 0x8b98b0,
};
const TIER_GLOW: Record<MapNode["tier"], number> = {
  ha: 0.9, gr: 0.55, in: 0.45, gu: 0.15,
};
const EDGE_KINDS = ["joins", "computed-from", "all"] as const;

export function CosmosTab() {
  const stage = useRef<HTMLDivElement>(null);
  const [payload, setPayload] =
    useState<GraphMapPayload | Unavailable | null>(null);
  const [picked, setPicked] = useState<MapNode | null>(null);
  const [edgeKind, setEdgeKind] =
    useState<(typeof EDGE_KINDS)[number]>("joins");
  const edgeGroups = useRef<Record<string, THREE.LineSegments>>({});

  useEffect(() => {
    fetch("/api/meridian/graph_map")
      .then((r) => r.json())
      .then(setPayload)
      .catch(() => setPayload(
        { available: false, reason: "console unreachable" }));
  }, []);

  useEffect(() => {
    const host = stage.current;
    if (!host || !payload || !payload.available) return;
    const dark =
      document.documentElement.dataset.theme === "dark" ||
      (!document.documentElement.dataset.theme &&
        matchMedia("(prefers-color-scheme: dark)").matches);
    const width = host.clientWidth || 900;
    const height = host.clientHeight || 540;
    const renderer = new THREE.WebGLRenderer(
      { antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
    renderer.setSize(width, height);
    host.appendChild(renderer.domElement);
    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(
      50, width / height, 0.1, 500);
    scene.add(new THREE.AmbientLight(0xffffff, 1.6));
    const dir = new THREE.DirectionalLight(0xffffff, 1.2);
    dir.position.set(10, 20, 10);
    scene.add(dir);
    const root = new THREE.Group();
    scene.add(root);

    const sphere = new THREE.SphereGeometry(1, 20, 20);
    const meshes = payload.nodes.map((node) => {
      const color = node.star ? 0xe8c98a : TIER_COLOR[node.tier];
      const mesh = new THREE.Mesh(sphere,
        new THREE.MeshStandardMaterial({
          color, emissive: color,
          emissiveIntensity: TIER_GLOW[node.tier] * 0.6,
          roughness: 0.4,
        }));
      const scale = node.star
        ? 1.15
        : Math.min(0.3 + Math.sqrt(Math.max(node.usage, 1)) / 30, 1.6);
      mesh.scale.setScalar(node.kind === "metric" ? scale * 0.6 : scale);
      mesh.position.set(...node.pos);
      mesh.userData = node;
      if (node.star) {
        const halo = new THREE.Mesh(
          new THREE.SphereGeometry(1.7, 20, 20),
          new THREE.MeshBasicMaterial({
            color: 0xe8c98a, transparent: true, opacity: 0.18 }));
        mesh.add(halo);
      }
      root.add(mesh);
      return mesh;
    });

    const positions = new Map(
      payload.nodes.map((n) => [n.id, new THREE.Vector3(...n.pos)]));
    edgeGroups.current = {};
    for (const kind of ["joins", "computed-from"]) {
      const points: THREE.Vector3[] = [];
      for (const edge of payload.edges) {
        if (edge.kind !== kind) continue;
        const a = positions.get(edge.a);
        const b = positions.get(edge.b);
        if (a && b) points.push(a, b);
      }
      const geometry = new THREE.BufferGeometry().setFromPoints(points);
      const line = new THREE.LineSegments(geometry,
        new THREE.LineBasicMaterial({
          transparent: true, opacity: kind === "joins" ? 0.35 : 0.16,
          color: dark ? 0x8fa8d8 : 0x9aa8c0,
        }));
      edgeGroups.current[kind] = line;
      root.add(line);
    }

    const labels = payload.wells.map((well) => {
      const el = document.createElement("div");
      el.className = "m-cosmos-label";
      el.style.color = dark ? "#cdd9f0" : "#4a5568";
      el.innerHTML = `${well.label}<br><span>${well.sub}</span>`;
      host.appendChild(el);
      return {
        el,
        pos: new THREE.Vector3(...well.center)
          .add(new THREE.Vector3(0, 10, 0)),
      };
    });

    const rot = { x: -0.12, y: 0.3 };
    let dist = 46;
    let drag: { x: number; y: number; moved: boolean } | null = null;
    renderer.domElement.style.cursor = "grab";
    const onDown = (e: PointerEvent) => {
      drag = { x: e.clientX, y: e.clientY, moved: false };
    };
    const onMove = (e: PointerEvent) => {
      if (!drag) return;
      const dx = e.clientX - drag.x;
      const dy = e.clientY - drag.y;
      if (Math.abs(dx) + Math.abs(dy) > 3) drag.moved = true;
      rot.y += dx * 0.005;
      rot.x = Math.max(-1.2, Math.min(1.2, rot.x + dy * 0.004));
      drag.x = e.clientX;
      drag.y = e.clientY;
    };
    const onUp = () => { drag = null; };
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      dist = Math.max(18, Math.min(90, dist + e.deltaY * 0.04));
    };
    renderer.domElement.addEventListener("pointerdown", onDown);
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    renderer.domElement.addEventListener("wheel", onWheel,
      { passive: false });
    const ray = new THREE.Raycaster();
    const pointer = new THREE.Vector2();
    let selected: THREE.Mesh | null = null;
    const onClick = (e: MouseEvent) => {
      if (drag && drag.moved) return;
      const rect = renderer.domElement.getBoundingClientRect();
      pointer.set(
        ((e.clientX - rect.left) / rect.width) * 2 - 1,
        -((e.clientY - rect.top) / rect.height) * 2 + 1);
      ray.setFromCamera(pointer, camera);
      const hit = ray.intersectObjects(meshes, false)[0];
      if (!hit) return;
      if (selected) {
        const prev = selected.userData as MapNode;
        (selected.material as THREE.MeshStandardMaterial)
          .emissiveIntensity = TIER_GLOW[prev.tier] * 0.6;
      }
      selected = hit.object as THREE.Mesh;
      (selected.material as THREE.MeshStandardMaterial)
        .emissiveIntensity = 1.4;
      setPicked(selected.userData as MapNode);
    };
    renderer.domElement.addEventListener("click", onClick);

    const reduced =
      matchMedia("(prefers-reduced-motion: reduce)").matches;
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
        label.el.style.left =
          `${(projected.x * 0.5 + 0.5) * renderer.domElement.clientWidth}px`;
        label.el.style.top =
          `${(-projected.y * 0.5 + 0.5) * renderer.domElement.clientHeight}px`;
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
      renderer.domElement.removeEventListener("pointerdown", onDown);
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      renderer.domElement.removeEventListener("wheel", onWheel);
      renderer.domElement.removeEventListener("click", onClick);
      labels.forEach((l) => l.el.remove());
      renderer.dispose();
      host.innerHTML = "";
    };
  }, [payload]);

  useEffect(() => {
    for (const [kind, line] of Object.entries(edgeGroups.current)) {
      line.visible = edgeKind === "all" || edgeKind === kind;
    }
  }, [edgeKind, payload]);

  return (
    <div className="m-page m-page-wide">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">Understand · Graph Cosmos</span>
        <span className="m-spacer" />
        {payload?.available && (
          <span className="m-muted">
            {payload.nodes.length} bodies ·{" "}
            {payload.meta.truncated.mined_metrics} mined metrics
            counted, not drawn
          </span>
        )}
      </div>

      {payload && !payload.available && (
        <div className="m-card m-empty">
          <div className="m-card-label">NO SKY TO DRAW</div>
          <p>{payload.reason}</p>
        </div>
      )}

      <div className="m-cosmos-row">
        <div className="m-cosmos-stage-wrap">
          <div className="m-pills m-cosmos-pills">
            {EDGE_KINDS.map((k) => (
              <button key={k}
                className={`m-pill ${edgeKind === k ? "m-pill-on" : ""}`}
                onClick={() => setEdgeKind(k)}>{k}</button>
            ))}
          </div>
          <div ref={stage} className="m-cosmos-stage" />
        </div>

        <div className="m-cosmos-rail">
          {picked ? (
            <div className="m-card">
              <div className="m-card-label">
                {picked.kind.toUpperCase()}
              </div>
              <div className="m-profile-title m-mono">
                {picked.label}
              </div>
              {picked.business_name && (
                <div>{picked.business_name}</div>
              )}
              {picked.description && (
                <div className="m-muted">{picked.description}</div>
              )}
              <span className={`m-chip m-t-${picked.tier}`}>
                {TIER_GLYPH[picked.tier].glyph}{" "}
                {TIER_GLYPH[picked.tier].word}
              </span>
              <div className="m-muted">
                well {picked.well}
                {picked.business_unit && ` · MDM unit ${picked.business_unit}`}
                {picked.lifecycle && ` · ${picked.lifecycle}`}
                {picked.pii && " · ⊘ PII"}
                {picked.star && " · ★ held by multiple domains"}
              </div>
              <div className="m-muted">
                usage <span className="m-mono">{picked.usage}</span>
                {picked.kind === "table" && picked.metrics_here !==
                  undefined &&
                  ` · ${picked.metrics_here} metrics here`}
                {picked.kind === "metric" && picked.status &&
                  ` · ${picked.status}`}
              </div>
              <div className="m-muted">
                open its profile from the Semantics tab
              </div>
            </div>
          ) : (
            <div className="m-card">
              <div className="m-card-label">ENCODING</div>
              <p className="m-muted">
                {payload?.available ? payload.meta.encoding
                  : "size = usage · glow = trust tier · gold star = "
                    + "held by multiple domains"}
              </p>
              <p className="m-muted">
                drag to orbit · wheel to zoom · click a body to read it
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
