/** Deterministic graph geometry shared by every canvas.
 *
 * Fruchterman–Reingold with mild centre gravity, clamped to a padded
 * frame. Initial positions come from the node index (a ring), never
 * Math.random, so a snapshot always renders the same picture. */

import type { GraphMapNode } from "./types";

export const SPACE_W = 1200;
export const SPACE_H = 640;
/* portrait frame for tall side-strip canvases (the Ask panel) */
export const STRIP_W = 560;
export const STRIP_H = 900;

export interface Pt { x: number; y: number }

export function layoutSpine(nodes: GraphMapNode[],
                            edges: { source: string; target: string }[],
                            W: number = SPACE_W,
                            H: number = SPACE_H,
                            ): Pt[] {
  const n = nodes.length;
  if (!n) return [];
  /* keep labels inside the frame: wide pads on wide frames, tighter
   * on narrow ones (labels are centred, ~120px wide at most) */
  const PAD_X = Math.min(140, Math.max(70, W * 0.117));
  const PAD_Y = Math.min(80, Math.max(48, H * 0.125));
  const idx = new Map(nodes.map((nd, i) => [nd.id, i]));
  const pos: Pt[] = nodes.map((_, i) => {
    const a = (i / n) * 2 * Math.PI;
    return { x: W / 2 + Math.cos(a) * W * 0.22,
             y: H / 2 + Math.sin(a) * H * 0.22 };
  });
  const links = edges
    .map((e) => [idx.get(e.source), idx.get(e.target)] as [number?, number?])
    .filter((l): l is [number, number] => l[0] != null && l[1] != null);
  const k = Math.sqrt((W * H) / Math.max(1, n)) * 0.9;
  const iters = 400;
  for (let it = 0; it < iters; it++) {
    const disp: Pt[] = pos.map(() => ({ x: 0, y: 0 }));
    for (let a = 0; a < n; a++) {
      for (let b = a + 1; b < n; b++) {
        let dx = pos[a].x - pos[b].x;
        let dy = pos[a].y - pos[b].y;
        const d = Math.hypot(dx, dy) || 0.01;
        const f = (k * k) / d;
        dx /= d; dy /= d;
        disp[a].x += dx * f; disp[a].y += dy * f;
        disp[b].x -= dx * f; disp[b].y -= dy * f;
      }
    }
    for (const [a, b] of links) {
      let dx = pos[a].x - pos[b].x;
      let dy = pos[a].y - pos[b].y;
      const d = Math.hypot(dx, dy) || 0.01;
      const f = (d * d) / k;
      dx /= d; dy /= d;
      disp[a].x -= dx * f; disp[a].y -= dy * f;
      disp[b].x += dx * f; disp[b].y += dy * f;
    }
    const temp = (1 - it / iters) * (W * 0.045);
    for (let i = 0; i < n; i++) {
      disp[i].x += (W / 2 - pos[i].x) * 0.035;
      disp[i].y += (H / 2 - pos[i].y) * 0.035;
      const d = Math.hypot(disp[i].x, disp[i].y) || 0.01;
      pos[i].x += (disp[i].x / d) * Math.min(d, temp);
      pos[i].y += (disp[i].y / d) * Math.min(d, temp);
      pos[i].x = Math.max(PAD_X, Math.min(W - PAD_X, pos[i].x));
      pos[i].y = Math.max(PAD_Y, Math.min(H - PAD_Y, pos[i].y));
    }
  }
  return pos;
}

/* trust-as-radius (mockup 1f): signed at the centre, vapour at the
 * rim — the sky's goldenness becomes spatial. Deterministic polar
 * placement: radius from tier, angle golden-stepped by sort order. */
export const TIER_ORBIT: Record<string, number> = {
  human_asserted: 0.22, grounded: 0.44, inferred: 0.66,
  guessed: 0.88, deprecated: 0.97,
};

export function layoutOrbits(nodes: GraphMapNode[],
                             W: number = SPACE_W,
                             H: number = SPACE_H): Pt[] {
  const cx = W / 2, cy = H / 2;
  const RX = W / 2 - 130, RY = H / 2 - 64;
  const order = nodes.map((_, i) => i)
    .sort((a, b) => nodes[a].id.localeCompare(nodes[b].id));
  const rank = new Map(order.map((idx, k) => [idx, k]));
  return nodes.map((n, i) => {
    const k = rank.get(i) ?? i;
    const f = TIER_ORBIT[n.tier] ?? 0.88;
    const ang = k * 2.39996 + hash01(k * 13 + 7) * 0.5;
    return { x: cx + Math.cos(ang) * RX * f,
             y: cy + Math.sin(ang) * RY * f };
  });
}

export const shortName = (s: string): string => {
  const tail = s.split(/[./]/).pop() ?? s;
  return tail.length > 24 ? tail.slice(0, 23) + "…" : tail;
};

/** Is this node "touched" by the current turn's traversal? Matched by
 * full synapse:// id or by bare table/metric name (case-insensitive).
 * Presence-based, per-turn — no wall-clock arithmetic. */
export function isActive(n: GraphMapNode,
                         activity: Record<string, number>): boolean {
  const label = n.label.toLowerCase();
  const bare = label.split(/[./]/).pop() ?? label;
  return Boolean(activity[n.id.toLowerCase()] ?? activity[label]
                 ?? activity[bare]);
}

/** Cheap deterministic hash → [0, 1). Positions the background dust
 * and the per-table satellite stars without Math.random. */
export function hash01(seed: number): number {
  const x = Math.sin(seed * 127.1 + 311.7) * 43758.5453;
  return x - Math.floor(x);
}
