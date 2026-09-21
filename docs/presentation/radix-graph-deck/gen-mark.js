// Radix Graph mark: the Radix bubble, with the graph inside it instead of the sparkle.
// Bubble geometry traced from glyphs/radix.png — body 26,42 → 229,192, corner radius 43,
// tail anchored 112→142 on the bottom edge, tip at 88,229. Same silhouette, new contents.
const sharp = require("sharp");

const BUBBLE =
  "M69 42 H186 A43 43 0 0 1 229 85 V149 A43 43 0 0 1 186 192 " +
  "H142 L88 229 L112 192 H69 A43 43 0 0 1 26 149 V85 A43 43 0 0 1 69 42 Z";

// three nodes and the three edges between them, centred in the body
const N = { a: [129, 81], b: [87, 147], c: [171, 147] };
const R = 14, EDGE = 9;

const mark = (bubble, graph) => `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" width="256" height="256">
  <path d="${BUBBLE}" fill="${bubble}"/>
  <g stroke="${graph}" stroke-width="${EDGE}" stroke-linecap="round" fill="${graph}">
    <line x1="${N.a[0]}" y1="${N.a[1]}" x2="${N.b[0]}" y2="${N.b[1]}"/>
    <line x1="${N.a[0]}" y1="${N.a[1]}" x2="${N.c[0]}" y2="${N.c[1]}"/>
    <line x1="${N.b[0]}" y1="${N.b[1]}" x2="${N.c[0]}" y2="${N.c[1]}"/>
    <circle cx="${N.a[0]}" cy="${N.a[1]}" r="${R}"/>
    <circle cx="${N.b[0]}" cy="${N.b[1]}" r="${R}"/>
    <circle cx="${N.c[0]}" cy="${N.c[1]}" r="${R}"/>
  </g>
</svg>`;

const NAVY = "#002663", WHITE = "#FFFFFF";

(async () => {
  // dark mark for light grounds, white mark for dark grounds — mirrors radix.png / radixW.png
  const fs = require("fs");
  // scalable masters — the asset to reuse anywhere else
  fs.writeFileSync("mark/radix-graph.svg", mark(NAVY, WHITE).trim() + "\n");
  fs.writeFileSync("mark/radix-graph-white.svg", mark(WHITE, NAVY).trim() + "\n");
  await sharp(Buffer.from(mark(NAVY, WHITE))).png().toFile("glyphs/radixgraph.png");
  await sharp(Buffer.from(mark(WHITE, NAVY))).png().toFile("glyphs/radixgraphW.png");
  // and PNGs at the sizes a deck or a favicon actually needs
  for (const px of [512, 256, 64]) {
    await sharp(Buffer.from(mark(NAVY, WHITE))).resize(px, px).png()
      .toFile(`mark/radix-graph-${px}.png`);
    await sharp(Buffer.from(mark(WHITE, NAVY))).resize(px, px).png()
      .toFile(`mark/radix-graph-white-${px}.png`);
  }
  // a side-by-side sheet so the mark can be judged against the one it replaces
  const row = async (files, out) => {
    const tiles = await Promise.all(files.map((f) =>
      sharp(f).resize(180, 180, { fit: "contain",
        background: { r: 255, g: 255, b: 255, alpha: 0 } }).toBuffer()));
    await sharp({ create: { width: 180 * files.length + 40 * (files.length + 1), height: 240,
      channels: 4, background: { r: 245, g: 244, b: 240, alpha: 1 } } })
      .composite(tiles.map((b, i) => ({ input: b, left: 40 + i * 220, top: 30 })))
      .png().toFile(out);
  };
  await row(["glyphs/radix.png", "glyphs/radixgraph.png"], "mark-compare.png");
  console.log("wrote glyphs/radixgraph.png, glyphs/radixgraphW.png, mark-compare.png");
})();
