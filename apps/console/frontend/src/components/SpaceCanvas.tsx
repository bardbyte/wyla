/** Synapse space — the graph as a living cosmos. Always dark, whatever
 * the app theme: this is the map view, and space is dark.
 *
 * What you see:
 *  - background stardust (deterministic, twinkling),
 *  - every table a sun ORBITED by tiny stars — its real column count,
 *  - color = evidence tier (signed gold · corroborated teal · inferred
 *    amber · unverified dim): the sky literally shows how settled your
 *    knowledge is, and steward work turns it gold,
 *  - shape = kind (circle table · diamond entity · square metric ·
 *    triangle playbook),
 *  - a soft breathing core at the centre of the synapse.
 *
 * When the agent works: touched nodes IGNITE in traversal order (bloom
 * pop), edges between touched nodes fire as animated beams, everything
 * else recedes, and the agent's current verb floats in the corner. The
 * lit traversal lingers after the answer for show-and-tell; the next
 * question clears it.
 */

import { useMemo, useState } from "react";
import type { ReactNode } from "react";
import type { MouseEvent as ReactMouseEvent } from "react";
import {
  LANE_ORDER, SPACE_H, SPACE_W, STRIP_H, STRIP_W, TIER_ORBIT, hash01,
  isActive, layoutLanes, layoutOrbits, layoutSpine, shortName,
} from "../lib/graphLayout";
import type { GraphMap, GraphMapNode } from "../lib/types";

/* the fixed space palette — independent of the app theme */
const TIER_COLOR: Record<string, string> = {
  human_asserted: "#F2C94C",   // signed — gold
  grounded: "#37D3B2",         // corroborated — teal
  inferred: "#E0A63E",         // inferred — amber
  guessed: "#7E93AC",          // unverified — dim slate
  deprecated: "#5A6B7E",
};

const N_DUST = 110;

function Glyph({ kind, x, y, r, fill, cls, onClick }: {
  kind: string; x: number; y: number; r: number; fill: string;
  cls: string; onClick?: (e: ReactMouseEvent) => void;
}) {
  const common = {
    className: cls, onClick, fill,
    style: onClick ? ({ cursor: "pointer" } as const) : undefined,
  };
  if (kind === "metric")
    return <rect x={x - r} y={y - r} width={2 * r} height={2 * r}
      rx={2} {...common} />;
  if (kind === "entity")
    return <polygon
      points={`${x},${y - r} ${x + r},${y} ${x},${y + r} ${x - r},${y}`}
      {...common} />;
  if (kind === "skill")
    return <polygon
      points={`${x},${y - r} ${x + r * 0.92},${y + r * 0.6} ${x - r * 0.92},${y + r * 0.6}`}
      {...common} />;
  return <circle cx={x} cy={y} r={r} {...common} />;
}

export function SpaceCanvas({
  map, activity = {}, selected = null, onSelect, backdrop = false,
  portrait = false, verb = "", listening = [], variant = "constellation",
  intro = false, introLine = "", finding = null, ceremonyRef = null,
  overlay = null,
}: {
  map: GraphMap;
  activity?: Record<string, number>;
  selected?: string | null;
  onSelect?: (id: string | null) => void;
  /** backdrop mode: non-interactive, crops to fill its container */
  backdrop?: boolean;
  /** portrait mode: tall frame for the Ask side strip */
  portrait?: boolean;
  /** the agent's current tool verb, floated in-space while it works */
  verb?: string;
  /** anticipation: node ids that match the question being TYPED */
  listening?: string[];
  /** orbits = trust as radius; lanes = the sky organized by kind */
  variant?: "constellation" | "orbits" | "lanes";
  /** first light (mockup 1a): staggered arrival + a serif line, once */
  intro?: boolean;
  introLine?: string;
  /** condensation (mockup 1d): the finding as a paper plate in-space,
   *  citation threads drawn to the exact nodes that produced it */
  finding?: { text: string;
              citations: { label: string; ref: string }[] } | null;
  /** the signature ceremony (mockup 1e): this ref ignites gold */
  ceremonyRef?: string | null;
  /** extra in-space chrome (the ask bar, the seal) rendered by the host */
  overlay?: ReactNode;
}) {
  const [localSel, setLocalSel] = useState<string | null>(null);
  const sel = onSelect ? selected : localSel;
  const setSel = onSelect ?? setLocalSel;

  const W = portrait ? STRIP_W : SPACE_W;
  const H = portrait ? STRIP_H : SPACE_H;
  const sig = map.nodes.map((n) => n.id).join("|") + "#" + map.edges.length
    + `@${W}x${H}#${variant}`;
  /* eslint-disable react-hooks/exhaustive-deps */
  const pos = useMemo(
    () => (variant === "orbits" ? layoutOrbits(map.nodes, W, H)
      : variant === "lanes" ? layoutLanes(map.nodes, W, H)
      : layoutSpine(map.nodes, map.edges, W, H)), [sig]);
  const idx = useMemo(
    () => new Map(map.nodes.map((n, i) => [n.id, i])), [sig]);
  const degree = useMemo(() => {
    const d = new Map<string, number>();
    for (const e of map.edges) {
      d.set(e.source, (d.get(e.source) ?? 0) + 1);
      d.set(e.target, (d.get(e.target) ?? 0) + 1);
    }
    return d;
  }, [sig]);
  const dust = useMemo(() =>
    Array.from({ length: N_DUST }, (_, i) => ({
      x: hash01(i * 3 + 1) * W,
      y: hash01(i * 3 + 2) * H,
      r: 0.7 + hash01(i * 3 + 3) * 1.1,
      delay: hash01(i * 7 + 5) * 6,
    })), [W, H]);
  /* eslint-enable react-hooks/exhaustive-deps */

  /* when two nodes sit label-to-label, flip the right-hand one's label
   * above its glyph so the names never collide */
  const labelUp = useMemo(() => {
    const up = new Set<string>();
    for (let i = 0; i < map.nodes.length; i++) {
      for (let j = i + 1; j < map.nodes.length; j++) {
        const a = pos[i], b = pos[j];
        if (!a || !b) continue;
        if (Math.abs(a.y - b.y) < 36 && Math.abs(a.x - b.x) < 150) {
          up.add((a.x <= b.x ? map.nodes[j] : map.nodes[i]).id);
        }
      }
    }
    return up;
  }, [sig, pos]);

  const activeIds = useMemo(() => {
    const s = new Set<string>();
    for (const n of map.nodes) if (isActive(n, activity)) s.add(n.id);
    return s;
  }, [map, activity]);
  const traversing = activeIds.size > 0;
  const listenIds = useMemo(() => new Set(listening), [listening]);

  /* first light: bigger bodies arrive first (mockup 1a) */
  const arriveRank = useMemo(() => {
    const order = map.nodes.map((_, i) => i).sort((a, b) =>
      ((map.nodes[b].columns ?? 0) - (map.nodes[a].columns ?? 0))
      || map.nodes[a].id.localeCompare(map.nodes[b].id));
    return new Map(order.map((idx, k) => [map.nodes[idx].id, k]));
    /* eslint-disable-next-line react-hooks/exhaustive-deps */
  }, [sig]);

  /* the verb whispers NEXT TO the node the agent touched last (1c) */
  const lastTouched = useMemo(() => {
    let bestKey = ""; let bestAt = -1;
    for (const [k, at] of Object.entries(activity))
      if (at > bestAt) { bestAt = at; bestKey = k; }
    if (!bestKey) return null;
    for (const n of map.nodes) {
      const label = n.label.toLowerCase();
      const bare = label.split(/[./]/).pop() ?? label;
      if (n.id.toLowerCase() === bestKey || label === bestKey
          || bare === bestKey) {
        const i = idx.get(n.id);
        return i == null ? null : pos[i];
      }
    }
    return null;
  }, [activity, map, idx, pos]);

  /* lane captions (1g): one label per present kind, at its lane */
  const laneLabels = useMemo(() => {
    if (variant !== "lanes") return [];
    return LANE_ORDER.flatMap((k) => {
      const ys = map.nodes.flatMap((n, i) =>
        n.kind === k && pos[i] ? [pos[i].y] : []);
      if (!ys.length) return [];
      return [{ kind: k,
                y: ys.reduce((a, b) => a + b, 0) / ys.length }];
    });
    /* eslint-disable-next-line react-hooks/exhaustive-deps */
  }, [variant, sig, pos]);

  /* condensation threads (1d): plate → the nodes it cites */
  const PLATE = { w: Math.min(560, W - 160), h: 108 };
  const plateX = (W - PLATE.w) / 2, plateY = H - PLATE.h - 26;
  const threadTargets = useMemo(() => {
    if (!finding) return [];
    const out: { x: number; y: number; fromX: number }[] = [];
    finding.citations.forEach((c, k) => {
      const node = map.nodes.find((n) =>
        n.id === c.ref
        || c.ref.toLowerCase().endsWith("/" + n.label.toLowerCase()));
      if (!node) return;
      const i = idx.get(node.id);
      if (i == null || !pos[i]) return;
      out.push({ x: pos[i].x, y: pos[i].y,
                 fromX: plateX + 40 + k * 90 });
    });
    return out;
    /* eslint-disable-next-line react-hooks/exhaustive-deps */
  }, [finding, sig, pos]);

  const nodeR = (n: GraphMapNode) =>
    (n.kind === "table" ? 9 : 7)
    + Math.min(9, (degree.get(n.id) ?? 0) * 1.5);

  return (
    <div className={`space-wrap ${backdrop ? "backdrop" : ""}`}>
      <svg className="space-canvas"
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio={backdrop ? "xMidYMid slice" : "xMidYMid meet"}
        role="img" aria-label="Synapse space — the knowledge graph"
        onClick={backdrop ? undefined : () => setSel(null)}>
        <defs>
          <radialGradient id="sp-core" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="#39D98A" stopOpacity="0.5" />
            <stop offset="45%" stopColor="#2AA7C7" stopOpacity="0.14" />
            <stop offset="100%" stopColor="#0A1626" stopOpacity="0" />
          </radialGradient>
          <radialGradient id="sp-vignette" cx="50%" cy="42%" r="75%">
            <stop offset="0%" stopColor="#0E1B30" />
            <stop offset="70%" stopColor="#081020" />
            <stop offset="100%" stopColor="#05090F" />
          </radialGradient>
        </defs>

        {/* deep space */}
        <rect x="0" y="0" width={W} height={H}
          fill="url(#sp-vignette)" />
        {/* stardust */}
        <g className="sp-dust">
          {dust.map((d, i) => (
            <circle key={i} cx={d.x} cy={d.y} r={d.r} fill="#B9CBE4"
              className="sp-star"
              style={{ animationDelay: `${d.delay}s` }} />
          ))}
        </g>
        {/* the breathing core of the synapse */}
        <circle cx={W / 2} cy={H / 2} r={Math.min(W, H) * 0.235}
          fill="url(#sp-core)" className="sp-corepulse" />
        {/* trust orbits: signed at the centre, vapour at the rim */}
        {variant === "orbits" && (
          <g aria-hidden>
            {Object.entries(TIER_ORBIT)
              .filter(([t]) => t !== "deprecated")
              .map(([t, f]) => (
                <ellipse key={t} cx={W / 2} cy={H / 2}
                  rx={(W / 2 - 130) * f} ry={(H / 2 - 64) * f}
                  className={`sp-orbit ${t === "human_asserted" ? "gold" : ""}`} />
              ))}
          </g>
        )}

        {/* constellation edges */}
        <g>
          {map.edges.map((e, i) => {
            const a = idx.get(e.source); const b = idx.get(e.target);
            if (a == null || b == null) return null;
            const hot = activeIds.has(e.source) && activeIds.has(e.target);
            const selHot = sel != null
              && (e.source === sel || e.target === sel);
            const dim = (traversing && !hot) || (sel != null && !selHot
                                                 && !hot);
            const x1 = pos[a].x, y1 = pos[a].y,
                  x2 = pos[b].x, y2 = pos[b].y;
            const len = Math.hypot(x2 - x1, y2 - y1);
            return (
              <g key={i}>
                <line x1={x1} y1={y1} x2={x2} y2={y2}
                  className={`sp-edge ${selHot ? "selhot" : ""} ${dim ? "dim" : ""}`} />
                {hot && (
                  <line x1={x1} y1={y1} x2={x2} y2={y2}
                    className="sp-beam"
                    style={{ strokeDasharray: len,
                             strokeDashoffset: len } as never} />
                )}
              </g>
            );
          })}
        </g>

        {laneLabels.map((l) => (
          <text key={l.kind} x={46} y={l.y + 4} className="sp-lane">
            {({ table: "TABLES", metric: "METRICS", entity: "ENTITIES",
                skill: "PLAYBOOKS" } as Record<string, string>)[l.kind]
              ?? l.kind.toUpperCase()}
          </text>
        ))}

        {/* condensation: citation threads first, under the suns */}
        {finding && threadTargets.map((t, i) => (
          <line key={i} x1={t.fromX} y1={plateY + 8} x2={t.x} y2={t.y}
            className="sp-thread" />
        ))}

        {/* suns + their column satellites */}
        <g>
          {map.nodes.map((n) => {
            const p = pos[idx.get(n.id)!];
            const r = nodeR(n);
            const color = TIER_COLOR[n.tier] ?? TIER_COLOR.guessed;
            const act = activeIds.has(n.id);
            const isSel = n.id === sel;
            const dim = (traversing && !act && !isSel)
              || (sel != null && !isSel && !act);
            const sats = n.kind === "table"
              ? Math.min(n.columns ?? 0, 18) : 0;
            const listen = listenIds.has(n.id) && !act;
            const showLabel = !backdrop || act || isSel || listen;
            const ceremonial = ceremonyRef != null
              && (n.id === ceremonyRef
                  || ceremonyRef.toLowerCase().endsWith(
                       "/" + n.label.toLowerCase()));
            return (
              <g key={n.id}
                className={`sp-node ${dim ? "dim" : ""} ${intro ? "sp-arrive" : ""}`}
                style={intro ? { animationDelay:
                  `${Math.min(2.2, (arriveRank.get(n.id) ?? 0) * 0.11)}s` }
                  : undefined}>
                {ceremonial && (
                  <>
                    <circle cx={p.x} cy={p.y} r={r + 10}
                      className="sp-ceremony" />
                    <circle cx={p.x} cy={p.y} r={r + 22}
                      className="sp-ceremony-after" />
                  </>
                )}
                {/* orbit satellites: the table's columns, as tiny stars */}
                {sats > 0 && (
                  <g className={act ? "sp-sats hot" : "sp-sats"}>
                    {Array.from({ length: sats }, (_, i) => {
                      const ang = i * 2.39996 + hash01(i + r) * 0.4;
                      const rad = r + 7 + (i % 3) * 5;
                      return (
                        <circle key={i}
                          cx={p.x + Math.cos(ang) * rad * 1.15}
                          cy={p.y + Math.sin(ang) * rad * 0.85}
                          r={i % 4 === 0 ? 1.8 : 1.2}
                          fill={color} />
                      );
                    })}
                  </g>
                )}
                {listen && (
                  <circle cx={p.x} cy={p.y} r={r + 7}
                    className="sp-listen" />
                )}
                {act && (
                  <>
                    <circle cx={p.x} cy={p.y} r={r + 9}
                      className="sp-ignite" style={{ stroke: color }} />
                    <circle cx={p.x} cy={p.y} r={r + 9}
                      className="sp-halo" style={{ fill: color }} />
                  </>
                )}
                {isSel && !act && (
                  <circle cx={p.x} cy={p.y} r={r + 8}
                    className="sp-selring" />
                )}
                <Glyph kind={n.kind} x={p.x} y={p.y} r={r} fill={color}
                  cls={`sp-body ${act ? "hot" : ""}`}
                  onClick={backdrop ? undefined : (e) => {
                    e.stopPropagation();
                    setSel(isSel ? null : n.id);
                  }} />
                {showLabel && (
                  <text x={p.x}
                    y={labelUp.has(n.id) ? p.y - r - 10 : p.y + r + 16}
                    className={`sp-label ${act || isSel ? "hot" : ""} ${listen ? "listen" : ""}`}
                    textAnchor="middle">{shortName(n.label)}</text>
                )}
              </g>
            );
          })}
        </g>
        {/* condensation: the finding as a paper plate in-space (1d) */}
        {finding && (
          <g className="sp-plate">
            <rect x={plateX} y={plateY} width={PLATE.w} height={PLATE.h}
              rx={9} className="sp-plate-paper" />
            <text x={plateX + 26} y={plateY + 30}
              className="sp-plate-eyebrow">FINDING</text>
            <text x={plateX + 26} y={plateY + 62}
              className="sp-plate-title">
              {finding.text.length > 52
                ? finding.text.slice(0, 51) + "…" : finding.text}
            </text>
            <text x={plateX + 26} y={plateY + 88}
              className="sp-plate-cites">
              {finding.citations.map((c) => c.label).join("   ·   ")}
            </text>
          </g>
        )}
      </svg>

      {/* the agent's current move, floated in space */}
      {verb && traversing && (
        <div className="sp-verb"
          style={lastTouched && !backdrop ? {
            left: `${Math.min(78, Math.max(6, (lastTouched.x / W) * 100))}%`,
            right: "auto",
            top: `${Math.min(88, Math.max(4, (lastTouched.y / H) * 100 + 5))}%`,
            bottom: "auto",
          } : undefined}>
          <span className="sp-verb-dot" aria-hidden />
          {verb}
        </div>
      )}

      {intro && introLine && (
        <div className="sp-firstlight" aria-hidden>{introLine}</div>
      )}

      {overlay}

      {!backdrop && (
        <div className="sp-legend" aria-hidden>
          <span><i style={{ background: TIER_COLOR.human_asserted }} /> Signed</span>
          <span><i style={{ background: TIER_COLOR.grounded }} /> Corroborated</span>
          <span><i style={{ background: TIER_COLOR.inferred }} /> Inferred</span>
          <span><i style={{ background: TIER_COLOR.guessed }} /> Unverified</span>
          <span className="sp-legend-note">
            circle table · diamond entity · square metric · triangle playbook
          </span>
        </div>
      )}
    </div>
  );
}
