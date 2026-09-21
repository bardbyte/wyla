# The Radix Graph mark

![Radix and Radix Graph side by side](radix-graph-vs-radix.png)

The Radix bubble, holding the graph instead of the sparkle — so the mark
says what the product is: the Radix assistant, with the graph inside it.

## Files

| file | use |
|---|---|
| `radix-graph.svg` | the master. Navy bubble, white graph — for light grounds |
| `radix-graph-white.svg` | white bubble, navy graph — for navy and blue grounds |
| `radix-graph-{512,256,64}.png` | raster, light grounds |
| `radix-graph-white-{512,256,64}.png` | raster, dark grounds |

Prefer the SVG. It is one path for the bubble and six primitives for the
graph, so it recolours and rescales without going back to a generator.

## Geometry

The silhouette is traced from the existing Radix glyph rather than
approximated, so the two sit together as a family: body from `26,42` to
`229,192` on a 256 canvas, corner radius `43`, tail anchored `112→142` on
the bottom edge with its tip at `88,229`.

Inside it, three nodes at `129,81` · `87,147` · `171,147`, radius `14`,
joined by `9`-wide edges with round caps. Those values were tuned once: the
first pass carried noticeably more ink than the sparkle it replaces, which
made it read heavier than the rest of the family.

Colours are the house navy `#002663` and white — no third colour.

## Regenerating

`docs/presentation/radix-graph-deck/gen-mark.js` emits every file here.
Change the node positions or radius there, run `node gen-mark.js`, and all
variants and sizes come out together.

## Two caveats

- The mark is derived from the existing Radix glyph's geometry. If that
  glyph belongs to a design team, have them look before it goes external.
- It has been checked at 28px — the size it renders at in the deck's product
  pill — and the three nodes still read. Below that, use the bubble alone.
