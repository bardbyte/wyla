# The Radix Graph deck

The source that builds
[`../radix-graph-end-to-end.pptx`](../radix-graph-end-to-end.pptx) — eleven
slides that tell the story once and then draw the architecture at three
depths. Committed as source, not just as a binary, so the next edit is a
diff rather than a fresh afternoon in PowerPoint.

## The arc

You can read the deck's spine off the slide eyebrows alone:

| # | eyebrow | what it does |
|---|---|---|
| 1 | — | title, and what the deck is built on |
| 2 | THE QUESTION | one question, three right answers, three definitions |
| 3 | WHY IT KEEPS HAPPENING | the glossary, the wiki, the note under the chart — and why each decays |
| 4 | THE TURN | it is already written down, just never in one place |
| 5 | WHAT WE BUILT · DEPTH 1 | three blocks, for a room with thirty seconds |
| 6 | HOW IT WORKS · DEPTH 2 | the five stages |
| 7 | ONE LEVEL DOWN · DEPTH 3 | the same flow plus the check at every boundary |
| 8 | HOW IT RUNS | the platform view: what runs where, numbered |
| 9 | WHY YOU CAN TRUST IT | three refusals |
| 10 | WHAT CHANGES FOR YOU | four people, the same Monday |
| 11 | WHERE IT STANDS | honest state, and the ask |

Slides 6 and 7 are droppable for a business-only room; the arc still closes.
Every slide carries speaker notes.

## Rebuilding

```bash
npm install pptxgenjs sharp react-icons react react-dom
node gen-icons.js     # the Material icon set, in brand colours → icons/
node gen-mark.js      # the Radix Graph mark → glyphs/ and mark/
node make.js          # → radix-graph-end-to-end.pptx
```

`icons/` and `mark/` are generated and not committed; `glyphs/` is, because
those are lifted from the source deck and cannot be regenerated here.

## Checking it before you send it

LibreOffice is the usual way to render slides for review, and it is broken
in some sandboxes — it will refuse to load even a text file. `render.py` is
the fallback and is arguably the better check anyway: it reads the **written
`.pptx`** back with `python-pptx` and lays every shape out as HTML, so what
you inspect is the file itself rather than the generator's intentions.

```bash
python3 render.py radix-graph-end-to-end.pptx preview.html   # then open it
python scripts/office/validate.py radix-graph-end-to-end.pptx
```

Arial maps to Liberation Sans, which is metric-compatible, so text fit in
the preview matches PowerPoint. Two things it cannot draw: `ellipse` and
`rightArrow` geometry both come out as rectangles. Check those in PowerPoint.

## The conventions worth keeping

- **Product glyphs never ride in circles.** BigQuery, Gemini, Knowledge
  Catalog and Looker appear as themselves, on the title slide and the
  platform view only. Circled Material icons mean one of our own stages. So
  a reader can always tell what is a Google product and what is ours.
- **The palette is lifted, not invented** — navy `002663`, blue `016FD0`,
  ink `1A2233`, grey `5A6472`, borders `DDE3EA`, tints `F7F9FB` / `EDF1F6`,
  amber `B26A00` for anything that stops the run. Arial throughout.
- **Dark slides carry the story beats** (title, the question, the turn, the
  close); light slides carry the architecture. The brand mark sits behind
  the dark ones at 9% opacity.
- **Numbers on the platform slide are keyed to the walkthrough beneath it**,
  which is the whole reason that diagram explains itself.

## Before you present it

Two things on the slides are ours to assume and yours to confirm:

- The three percentages on slide 2 are **illustrative**, and labelled as
  such. A real disagreement from your own shop is worth more.
- The "What we need" card on slide 11 is an inference about the ask. Check
  it says what you actually want from that audience.
