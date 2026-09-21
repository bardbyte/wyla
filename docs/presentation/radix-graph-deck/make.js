const pptxgen = require("pptxgenjs");

// ── brand kit, lifted from the Context Layer deck ────────────────────────────
const NAVY   = "002663";
const NAVY2  = "0A3A82";
const BLUE   = "016FD0";
const INK    = "1A2233";
const GREY   = "5A6472";
const BORDER = "DDE3EA";
const TINT   = "F7F9FB";
const TINT2  = "EDF1F6";
const ONNAVY = "C9D8F0";
const ONNAVY2= "8FC1FF";
const PALE   = "8FB0E4";
const WHITE  = "FFFFFF";
const AMBER  = "B26A00";
const AMBERBG= "F8F0E3";
const AMBERLN= "D8A94E";
const AMBERTX= "5A4A26";
const F = "Arial";

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE";            // 13.333 x 7.5
pres.author = "Radix Graph";
pres.title  = "Radix Graph — the story, end to end";

// ═══ shared furniture ════════════════════════════════════════════════════════
function head(slide, eyebrow, title, dek, badge) {
  if (badge) {
    slide.addText(badge, {
      isTextBox: true, x: 11.2, y: 0.28, w: 1.7, h: 1.3,
      fontFace: F, fontSize: 80, bold: true, color: TINT2, align: "right", margin: 0,
    });
  }
  slide.addText(eyebrow, {
    isTextBox: true, x: 0.5, y: 0.42, w: 10.4, h: 0.26,
    fontFace: F, fontSize: 10.5, bold: true, color: BLUE, charSpacing: 0.9, margin: 0,
  });
  slide.addText(title, {
    isTextBox: true, x: 0.5, y: 0.72, w: 10.4, h: 0.6,
    fontFace: F, fontSize: 27, bold: true, color: NAVY, margin: 0,
  });
  if (dek) {
    slide.addText(dek, {
      isTextBox: true, x: 0.5, y: 1.36, w: 11.4, h: 0.4,
      fontFace: F, fontSize: 13.5, color: INK, margin: 0,
    });
  }
}

function darkHead(slide, eyebrow) {
  slide.addText(eyebrow, {
    isTextBox: true, x: 0.7, y: 0.78, w: 9.0, h: 0.3,
    fontFace: F, fontSize: 10.5, bold: true, color: ONNAVY2, charSpacing: 1.1, margin: 0,
  });
}

function arrow(slide, x, y, w) {
  slide.addShape(pres.ShapeType.rightArrow, {
    x, y: y - 0.085, w, h: 0.17,
    fill: { color: BLUE }, line: { color: BLUE, width: 0.5 },
  });
}

function dash(slide, x, y, color) {
  slide.addShape(pres.ShapeType.rect, {
    x, y, w: 0.22, h: 0.055,
    fill: { color: color || BLUE }, line: { color: color || BLUE, width: 0.5 },
  });
}

// a fresh object every call — pptxgenjs mutates option objects in place
const shadow = () => ({ type: "outer", color: "7E8B9E", blur: 9, offset: 2, angle: 90, opacity: 0.18 });

// the deck's own icon language: concept icons ride in circles, product glyphs never do
const TONES = {
  light: { bg: "DCE7F7", ln: "C9D8F0", col: "blue"  },
  solid: { bg: BLUE,     ln: BLUE,     col: "white" },
  onNavy:{ bg: WHITE,    ln: WHITE,    col: "navy"  },
  amber: { bg: AMBERBG,  ln: "EFDDB6", col: "amber" },
};
function iconCircle(slide, x, y, d, name, tone) {
  const t = TONES[tone || "light"];
  slide.addShape(pres.ShapeType.ellipse, {
    x, y, w: d, h: d, fill: { color: t.bg }, line: { color: t.ln, width: 1 },
  });
  const pad = d * 0.26;
  slide.addImage({ path: `icons/${name}-${t.col}.png`,
    x: x + pad, y: y + pad, w: d - 2 * pad, h: d - 2 * pad });
}

// a whisper of the Radix mark behind the dark slides, added before everything else
function motif(slide) {
  slide.addImage({ path: "glyphs/radixgraphW.png", x: 9.7, y: 2.35, w: 5.6, h: 5.6, transparency: 91 });
}

function closer(slide, y, lead, punch, dark) {
  slide.addText(
    [
      { text: lead + " ", options: { color: dark ? PALE : GREY, breakLine: false } },
      { text: punch, options: { color: dark ? WHITE : NAVY, bold: true } },
    ],
    { isTextBox: true, x: dark ? 0.7 : 0.5, y, w: 11.9, h: 0.5,
      fontFace: F, fontSize: 16, margin: 0 }
  );
}

function card(slide, b, x, y, w, h, s) {
  const hero = b.kind === "hero", outline = b.kind === "outline";
  slide.addShape(pres.ShapeType.roundRect, {
    x, y, w, h, rectRadius: 0.06,
    fill: { color: hero ? NAVY : outline ? WHITE : TINT },
    line: { color: outline ? BLUE : BORDER, width: outline ? 1.6 : 1 },
    shadow: shadow(),
  });
  if (b.eyebrow) {
    slide.addText(b.eyebrow, {
      isTextBox: true, x: x + s.pad, y: y + 0.16, w: w - 2 * s.pad, h: 0.22,
      fontFace: F, fontSize: 10, bold: true, color: hero ? ONNAVY2 : BLUE,
      charSpacing: 0.6, margin: 0,
    });
  }
  if (b.ic) {
    iconCircle(slide, x + s.pad, y + s.iconY, s.circle || 0.5, b.ic, hero ? "onNavy" : "light");
  } else if (b.icon) {
    slide.addImage({ path: b.icon, x: x + s.pad, y: y + s.iconY, w: s.icon, h: s.icon });
  }
  slide.addText(b.title, {
    isTextBox: true, x: x + s.pad, y: y + s.titleY, w: w - 2 * s.pad, h: s.titleH,
    fontFace: F, fontSize: s.titleSize, bold: true, color: hero ? WHITE : NAVY, margin: 0,
  });
  slide.addText(
    b.body.map((line, j) => ({
      text: line, options: { breakLine: j < b.body.length - 1, paraSpaceAfter: s.spaceAfter },
    })),
    { isTextBox: true, x: x + s.pad, y: y + s.bodyY, w: w - 2 * s.pad, h: s.bodyH,
      fontFace: F, fontSize: s.bodySize, color: hero ? ONNAVY : INK,
      lineSpacingMultiple: 1.12, valign: "top", margin: 0 }
  );
  if (b.caption) {
    slide.addText(b.caption, {
      isTextBox: true, x: x + s.pad, y: y + s.capY, w: w - 2 * s.pad, h: 0.34,
      fontFace: F, fontSize: s.capSize, italic: true, color: hero ? ONNAVY2 : GREY, margin: 0,
    });
  }
}

function governanceBand(slide, y, text) {
  slide.addShape(pres.ShapeType.roundRect, {
    x: 0.5, y, w: 12.33, h: 0.9, rectRadius: 0.06,
    fill: { color: TINT2 }, line: { color: BORDER, width: 1 },
  });
  slide.addText("GOVERNED END TO END", {
    isTextBox: true, x: 0.75, y: y + 0.2, w: 2.0, h: 0.3,
    fontFace: F, fontSize: 10.5, bold: true, color: BLUE, charSpacing: 0.6, margin: 0,
  });
  slide.addText(text, {
    isTextBox: true, x: 2.85, y: y + 0.16, w: 9.75, h: 0.58,
    fontFace: F, fontSize: 11, color: INK, valign: "middle", margin: 0,
  });
}

// three-up and four-up column geometry
const C3 = { x: [0.5, 4.615, 8.73], w: 3.85 };
const C5 = { x: [0.5, 3.045, 5.59, 8.135, 10.68], w: 2.15, gap: 0.395 };

// ═══ 1 · title ═══════════════════════════════════════════════════════════════
const s1 = pres.addSlide();
s1.background = { color: NAVY };
motif(s1);
s1.addText("SYSTEM DESIGN  ·  ARCHITECTURE OVERVIEW", {
  isTextBox: true, x: 0.7, y: 0.85, w: 8.0, h: 0.35,
  fontFace: F, fontSize: 11, color: PALE, charSpacing: 1.2, margin: 0,
});
s1.addText("Radix Graph, end to end", {
  isTextBox: true, x: 0.7, y: 1.35, w: 11.9, h: 1.1,
  fontFace: F, fontSize: 54, bold: true, color: WHITE, margin: 0,
});
s1.addText(
  [
    { text: "The sources a team already has, compiled into one trusted snapshot.",
      options: { bold: true, color: WHITE, breakLine: false } },
    { text: "  The agent answers from that snapshot — and says where every number came from.",
      options: { color: ONNAVY } },
  ],
  { isTextBox: true, x: 0.7, y: 2.55, w: 10.9, h: 0.55, fontFace: F, fontSize: 16, margin: 0 }
);

function pillRow(slide, y, items, ours) {
  let x = 0.7;
  items.forEach((it) => {
    const w = 0.52 + it.label.length * 0.093;
    slide.addShape(pres.ShapeType.roundRect, {
      x, y, w, h: 0.46, rectRadius: 0.1,
      fill: ours ? { color: BLUE, transparency: 70 } : { color: WHITE, transparency: 88 },
      line: { color: ours ? "7EB2EC" : WHITE, width: 1, transparency: 45 },
    });
    slide.addImage({ path: it.icon, x: x + 0.1, y: y + 0.085, w: 0.29, h: 0.29 });
    slide.addText(it.label, {
      isTextBox: true, x: x + 0.46, y: y + 0.02, w: w - 0.52, h: 0.42,
      fontFace: F, fontSize: 11.5, color: WHITE, valign: "middle", margin: 0,
    });
    x += w + 0.2;
  });
}
s1.addText("BUILT ON", {
  isTextBox: true, x: 0.7, y: 3.75, w: 2.0, h: 0.3,
  fontFace: F, fontSize: 10, color: PALE, charSpacing: 1.1, margin: 0,
});
pillRow(s1, 4.1, [
  { label: "BigQuery",          icon: "glyphs/bq.png" },
  { label: "Gemini",            icon: "glyphs/gemini.png" },
  { label: "Knowledge Catalog", icon: "glyphs/kc.png" },
  { label: "Looker",            icon: "glyphs/looker.png" },
], false);
s1.addText("OURS", {
  isTextBox: true, x: 0.7, y: 4.85, w: 2.0, h: 0.3,
  fontFace: F, fontSize: 10, color: PALE, charSpacing: 1.1, margin: 0,
});
pillRow(s1, 5.2, [
  { label: "Radix Graph", icon: "glyphs/radixgraphW.png" },
  { label: "Lumi",    icon: "glyphs/lumiW.png" },
], true);
s1.addText("A question, why it has three answers, and what we did about it.", {
  isTextBox: true, x: 0.7, y: 6.55, w: 9.0, h: 0.3,
  fontFace: F, fontSize: 11, color: PALE, margin: 0,
});
s1.addNotes(
  "Ten slides, one arc: the question everybody has asked, why it has three answers, why the " +
  "usual fix decays, the turn, what we built, and what changes. The architecture sits in the " +
  "middle at three depths — drop slides 6 and 7 for a business-only room and the story still " +
  "holds."
);

// ═══ 2 · the question ════════════════════════════════════════════════════════
const s2 = pres.addSlide();
s2.background = { color: NAVY };
motif(s2);
darkHead(s2, "THE QUESTION");
s2.addText("“What was our approval rate last month?”", {
  isTextBox: true, x: 0.7, y: 1.18, w: 11.9, h: 0.95,
  fontFace: F, fontSize: 38, bold: true, color: WHITE, margin: 0,
});
s2.addText("Ask it in any org and you will get three answers back.", {
  isTextBox: true, x: 0.7, y: 2.18, w: 9.0, h: 0.32,
  fontFace: F, fontSize: 13, color: PALE, margin: 0,
});

const ANSWERS = [
  { n: "41.2%", who: "FINANCE SAYS",  def: "every application counted, including the ones still in flight" },
  { n: "38.7%", who: "RISK SAYS",     def: "test and duplicate applications stripped out first" },
  { n: "44.0%", who: "THE TEAM SAYS", def: "booked in the month, not applied for in the month" },
];
const AX = [0.7, 4.79, 8.88], AW = 3.74;
ANSWERS.forEach((a, i) => {
  s2.addShape(pres.ShapeType.roundRect, {
    x: AX[i], y: 2.78, w: AW, h: 2.12, rectRadius: 0.08,
    fill: { color: WHITE, transparency: 90 },
    line: { color: WHITE, width: 1, transparency: 50 },
  });
  s2.addText(a.who, {
    isTextBox: true, x: AX[i] + 0.3, y: 2.98, w: AW - 0.6, h: 0.26,
    fontFace: F, fontSize: 10, bold: true, color: ONNAVY2, charSpacing: 1.0, margin: 0,
  });
  s2.addText(a.n, {
    isTextBox: true, x: AX[i] + 0.3, y: 3.28, w: AW - 0.6, h: 0.72,
    fontFace: F, fontSize: 40, bold: true, color: WHITE, margin: 0,
  });
  s2.addText(a.def, {
    isTextBox: true, x: AX[i] + 0.3, y: 4.06, w: AW - 0.6, h: 0.7,
    fontFace: F, fontSize: 11.5, color: ONNAVY, lineSpacingMultiple: 1.12, margin: 0,
  });
});

s2.addText("Illustrative figures.", {
  isTextBox: true, x: 0.7, y: 4.98, w: 4.0, h: 0.26,
  fontFace: F, fontSize: 9.5, italic: true, color: PALE, margin: 0,
});
closer(s2, 5.6, "Nobody here is wrong.",
  "Each answer follows a different definition — and the difference was never written down.", true);
s2.addText("So two days go into deciding whose number belongs in the deck. Every month. For every metric.", {
  isTextBox: true, x: 0.7, y: 6.2, w: 11.9, h: 0.4,
  fontFace: F, fontSize: 13.5, color: ONNAVY, margin: 0,
});
s2.addNotes(
  "Open here, not on architecture. Everyone in the room has been on one side of this. The point " +
  "of the three cards is that all three are defensible — this is not a competence problem, it is " +
  "a definition problem. Land the last line and pause: the cost is not the wrong number, it is " +
  "the two days spent every month deciding which one to use."
);

// ═══ 3 · why it keeps happening ══════════════════════════════════════════════
const s3 = pres.addSlide();
head(s3, "WHY IT KEEPS HAPPENING", "Every org has already tried to fix this",
  "A glossary. A wiki page. A note under the chart. They all work — for about a quarter.");

const FIXES = [
  { kind: "tint", ic: "book", title: "The glossary",
    body: ["Promised one agreed vocabulary for the business.",
           "Drifts because somebody has to maintain it by hand, and it is nobody's actual job."],
    caption: "abandoned by the second quarter" },
  { kind: "tint", ic: "wiki", title: "The wiki page",
    body: ["Promised the context behind the number.",
           "Drifts because it sits beside the warehouse, not inside it — nothing ever checks it against the data."],
    caption: "right when written, stale by spring" },
  { kind: "tint", ic: "chartnote", title: "The note under the chart",
    body: ["Promised the definition where the number actually is.",
           "Drifts because it describes one chart, and the next chart starts the argument again."],
    caption: "true, and it does not travel" },
];
const FIXSIZES = {
  pad: 0.28, circle: 0.52, iconY: 0.24, titleY: 0.88, titleH: 0.4, titleSize: 17,
  bodyY: 1.36, bodyH: 1.15, bodySize: 12, spaceAfter: 9, capY: 2.58, capSize: 10,
};
FIXES.forEach((b, i) => card(s3, b, C3.x[i], 1.95, C3.w, 3.06, FIXSIZES));

s3.addShape(pres.ShapeType.roundRect, {
  x: 0.5, y: 5.22, w: 12.33, h: 0.92, rectRadius: 0.06,
  fill: { color: AMBERBG }, line: { color: AMBERLN, width: 1 },
});
s3.addText("THE PATTERN", {
  isTextBox: true, x: 0.75, y: 5.44, w: 1.5, h: 0.3,
  fontFace: F, fontSize: 10.5, bold: true, color: AMBER, charSpacing: 0.6, margin: 0,
});
s3.addText("Every one of them asks a human to write the meaning down a second time — so every one of them decays at exactly the speed the business changes.", {
  isTextBox: true, x: 2.4, y: 5.4, w: 10.2, h: 0.58,
  fontFace: F, fontSize: 12.5, color: AMBERTX, valign: "middle", margin: 0,
});
closer(s3, 6.34, "Which leaves one question.",
  "What if nobody had to write it down at all?", false);
s3.addNotes(
  "Do not skip this slide — it is what stops the room hearing 'another data catalogue'. Each of " +
  "these is a real attempt that a real team made, and each failed for the same structural " +
  "reason, not for lack of discipline. Land the amber band: the common factor is asking a human " +
  "to write the meaning down a second time. That is the assumption we broke."
);

// ═══ 4 · the turn ════════════════════════════════════════════════════════════
const s4 = pres.addSlide();
s4.background = { color: NAVY };
motif(s4);
darkHead(s4, "THE TURN");
s4.addText("It is already written down.", {
  isTextBox: true, x: 0.7, y: 1.2, w: 11.9, h: 0.95,
  fontFace: F, fontSize: 44, bold: true, color: WHITE, margin: 0,
});
s4.addText("Just never in one place, and never as one thing.", {
  isTextBox: true, x: 0.7, y: 2.22, w: 9.0, h: 0.35,
  fontFace: F, fontSize: 15, color: PALE, margin: 0,
});

const ALREADY = [
  ["The queries people already run", "every day, against the real tables"],
  ["The playbooks analysts wrote", "for the questions that come back every month"],
  ["The catalogs already certified", "the definitions somebody has signed for"],
  ["The SQL behind numbers you trust", "the ones that already go in the board pack"],
];
ALREADY.forEach((a, i) => {
  const y = 3.0 + i * 0.72;
  dash(s4, 0.7, y + 0.17);
  s4.addText(a[0], {
    isTextBox: true, x: 1.08, y, w: 4.6, h: 0.32,
    fontFace: F, fontSize: 14.5, bold: true, color: WHITE, margin: 0,
  });
  s4.addText(a[1], {
    isTextBox: true, x: 5.85, y: y + 0.02, w: 6.7, h: 0.32,
    fontFace: F, fontSize: 12.5, color: ONNAVY, margin: 0,
  });
});

closer(s4, 5.98, "The number was never the hard part.",
  "Agreeing what it means is — so we stopped asking people to write it down, and compiled what they had already produced.", true);
s4.addNotes(
  "This is the hinge of the deck. The meaning is not missing — it is scattered across artefacts " +
  "the team already produces as a by-product of doing the work. Nobody has to author anything " +
  "new. Say the last line slowly: we compile what already exists, and we keep the evidence " +
  "attached, which is what lets disagreement stay visible instead of being averaged away."
);

// ═══ 5 · what we built (depth 1) ═════════════════════════════════════════════
const s5 = pres.addSlide();
head(s5, "WHAT WE BUILT  ·  DEPTH 1 OF 3", "Three blocks",
  "For a room with thirty seconds. Left to right.", "1");

const D1 = [
  { kind: "tint", ic: "storage", title: "What you already have",
    body: ["Tables, metadata and query history, plus the glossary and playbooks your team has already written."],
    caption: "nothing new to collect" },
  { kind: "hero", ic: "snapshot", title: "One trusted snapshot",
    body: ["All of it compiled into a single versioned picture of what the business means — kept current, not hand-maintained."],
    caption: "the one source everything reads" },
  { kind: "outline", ic: "chat", title: "Answers you can trust",
    body: ["Ask in plain words. Every number comes back with the definition behind it and where it came from."],
    caption: "trustworthy by default" },
];
const D1X = [0.5, 4.82, 9.14], D1W = 3.7, D1Y = 2.15, D1H = 3.35;
const D1SIZES = {
  pad: 0.28, circle: 0.66, iconY: 0.34, titleY: 1.16, titleH: 0.5, titleSize: 18,
  bodyY: 1.74, bodyH: 1.1, bodySize: 12, spaceAfter: 6, capY: 2.9, capSize: 10,
};
D1.forEach((b, i) => {
  card(s5, b, D1X[i], D1Y, D1W, D1H, D1SIZES);
  if (i < 2) arrow(s5, D1X[i] + D1W + 0.1, D1Y + D1H / 2, 0.42);
});
s5.addText("Meaning your team already owns — compiled once, then answered from every time.", {
  isTextBox: true, x: 0.5, y: 5.95, w: 11.8, h: 0.45,
  fontFace: F, fontSize: 17, color: NAVY, margin: 0,
});
s5.addNotes(
  "Thirty-second version. Left: nothing new to collect. Middle: compiled, not hand-maintained — " +
  "which is the whole answer to the decay on slide 3. Right: an answer always arrives with its " +
  "definition, so it can be acted on without a second opinion."
);

// ═══ 6 · how it works (depth 2) ══════════════════════════════════════════════
const s6 = pres.addSlide();
head(s6, "HOW IT WORKS  ·  DEPTH 2 OF 3", "The five stages",
  "For the people who will use it. What goes in, what it becomes, and where people meet it.", "2");

const CARD_Y = 2.0, CARD_H = 3.45;
const D2SIZES = {
  pad: 0.16, circle: 0.44, iconY: 0.44, titleY: 0.98, titleH: 0.3, titleSize: 14,
  bodyY: 1.34, bodyH: 1.56, bodySize: 10, spaceAfter: 7, capY: 2.94, capSize: 9.5,
};
const D2 = [
  { kind: "tint", ic: "storage", eyebrow: "01 · SOURCES", title: "Sources",
    body: ["Warehouse tables and metadata", "Query history", "Glossary, playbooks and proven queries"],
    caption: "what the team already has" },
  { kind: "tint", ic: "graph", eyebrow: "02 · SEMANTIC GRAPH", title: "Semantic graph",
    body: ["Terms, metrics and joins", "Each one carries the evidence behind it", "Where sources disagree, both are kept"],
    caption: "meaning, with its receipts" },
  { kind: "hero", ic: "snapshot", eyebrow: "03 · SNAPSHOT", title: "Trusted snapshot",
    body: ["One versioned build of the whole picture", "Compiled, never hand-edited", "Everything downstream reads this"],
    caption: "one source of truth" },
  { kind: "outline", ic: "model", eyebrow: "04 · AGENT HARNESS", title: "Agent harness",
    body: ["A general model given tools, checks and limits", "It looks things up instead of recalling them", "Every step is checked, not trusted"],
    caption: "answers, not guesses" },
  { kind: "outline", ic: "chat", eyebrow: "05 · EXPERIENCES", title: "Experiences",
    body: ["Ask in plain words", "Explore the semantics", "Steward what the business means"],
    caption: "where people meet it" },
];
D2.forEach((b, i) => {
  card(s6, b, C5.x[i], CARD_Y, C5.w, CARD_H, D2SIZES);
  if (i < D2.length - 1) arrow(s6, C5.x[i] + C5.w + 0.055, CARD_Y + CARD_H / 2, C5.gap - 0.11);
});
governanceBand(s6, 5.72,
  "Every answer names the definition it used and where the number came from. What the business " +
  "means is changed by people, on the record — never by the model.");
s6.addNotes(
  "Read it left to right. Sources are what the team already owns. The semantic graph is where " +
  "those become meaning, and every statement keeps the evidence behind it. That compiles into " +
  "one versioned snapshot — the single thing everything downstream reads. The harness gives a " +
  "general model tools, checks and limits. The experiences are the two front doors."
);

// ═══ 7 · one level down (depth 3) ════════════════════════════════════════════
const s7 = pres.addSlide();
head(s7, "ONE LEVEL DOWN  ·  DEPTH 3 OF 3", "The working view",
  "For the people who will build on it. The same flow, plus the check that sits at every boundary.", "3");

const D3Y = 2.0;
const D3 = [
  { kind: "tint", title: "Sources", ic: "storage",
    items: ["Warehouse tables, columns, metadata", "30 days of query history", "Glossary, playbooks, proven SQL"] },
  { kind: "tint", title: "Meaning", ic: "graph",
    items: ["Read in and normalised", "Identity resolved before anything lands", "Every statement keeps its evidence"] },
  { kind: "hero", title: "Snapshot", ic: "snapshot",
    items: ["Compiled as one unit", "Versioned and published whole", "A single read path downstream"] },
  { kind: "outline", title: "Harness", ic: "model",
    items: ["A fixed path for a governed number", "An open path for analysis", "Checks and budgets around both"] },
  { kind: "outline", title: "Surfaces", ic: "chat",
    items: ["Ask, in plain words", "Explore the semantics", "Steward and publish meaning"] },
];
D3.forEach((b, i) => {
  const x = C5.x[i];
  const hero = b.kind === "hero", outline = b.kind === "outline";
  s7.addShape(pres.ShapeType.roundRect, {
    x, y: D3Y, w: C5.w, h: 0.5, rectRadius: 0.06,
    fill: { color: hero ? NAVY : outline ? WHITE : TINT },
    line: { color: outline || hero ? BLUE : BORDER, width: outline || hero ? 1.4 : 1 },
    shadow: shadow(),
  });
  iconCircle(s7, x + 0.12, D3Y + 0.09, 0.32, b.ic, hero ? "onNavy" : "light");
  s7.addText(b.title, {
    isTextBox: true, x: x + 0.5, y: D3Y + 0.08, w: C5.w - 0.64, h: 0.34,
    fontFace: F, fontSize: 13, bold: true, color: hero ? WHITE : NAVY, valign: "middle", margin: 0,
  });
  b.items.forEach((it, j) => {
    const iy = D3Y + 0.62 + j * 0.7;
    s7.addShape(pres.ShapeType.roundRect, {
      x, y: iy, w: C5.w, h: 0.62, rectRadius: 0.05,
      fill: { color: hero ? NAVY2 : TINT }, line: { color: BORDER, width: 1 },
    });
    s7.addText(it, {
      isTextBox: true, x: x + 0.14, y: iy + 0.04, w: C5.w - 0.28, h: 0.54,
      fontFace: F, fontSize: 9.5, color: hero ? ONNAVY : INK,
      valign: "middle", lineSpacingMultiple: 1.1, margin: 0,
    });
  });
  if (i < D3.length - 1) arrow(s7, x + C5.w + 0.055, D3Y + 0.25, C5.gap - 0.11);
});

const CHECKS = [
  { ic: "fingerprint", label: "Identity", note: "a record only lands if its table is confirmed" },
  { ic: "factcheck", label: "Validation", note: "the graph must hold together, or nothing compiles" },
  { ic: "publish", label: "Promotion", note: "a build is published only if every check passes" },
  { ic: "lock", label: "Cost & access", note: "every query priced and permission-checked first" },
];
const CHK_Y = 4.86, CHK_W = 2.5, CHK_H = 0.86;
s7.addText("BOUNDARY CHECKS", {
  isTextBox: true, x: 0.5, y: 4.78, w: 2.15, h: 0.24,
  fontFace: F, fontSize: 9.5, bold: true, color: GREY, charSpacing: 0.8, margin: 0,
});
CHECKS.forEach((c, i) => {
  const centre = C5.x[i] + C5.w + C5.gap / 2;
  const cx = centre - CHK_W / 2;
  s7.addShape(pres.ShapeType.roundRect, {
    x: cx, y: CHK_Y + 0.22, w: CHK_W, h: CHK_H, rectRadius: 0.05,
    fill: { color: AMBERBG }, line: { color: AMBERLN, width: 1 },
  });
  iconCircle(s7, cx + 0.14, CHK_Y + 0.31, 0.3, c.ic, "amber");
  s7.addText(c.label, {
    isTextBox: true, x: cx + 0.52, y: CHK_Y + 0.32, w: CHK_W - 0.66, h: 0.28,
    fontFace: F, fontSize: 10, bold: true, color: AMBER, valign: "middle", margin: 0,
  });
  s7.addText(c.note, {
    isTextBox: true, x: cx + 0.14, y: CHK_Y + 0.62, w: CHK_W - 0.28, h: 0.44,
    fontFace: F, fontSize: 9, color: AMBERTX, lineSpacingMultiple: 1.1, margin: 0,
  });
  s7.addShape(pres.ShapeType.line, {
    x: centre, y: D3Y + 0.25, w: 0, h: CHK_Y + 0.22 - (D3Y + 0.25),
    line: { color: AMBERLN, width: 1, dashType: "dash" },
  });
});
governanceBand(s7, 6.18,
  "Nothing is answered from outside the snapshot, and what the business means is changed by " +
  "people, on the record — the model can read it, never write it.");
s7.addNotes(
  "The level an architect asks for. Two things to land. The harness has two paths, not one: a " +
  "fixed path for a governed number, an open path for analysis, same limits on both. And the " +
  "amber row: each boundary has a check that stops the run rather than degrading it, so a bad " +
  "input never becomes a confident wrong answer."
);

// ═══ 8 · how it runs — the platform view ════════════════════════════════════
const sRuns = pres.addSlide();
head(sRuns, "HOW IT RUNS", "What runs where",
  "Six steps, left to right. Everything inside the boundary is managed Google Cloud.");

// the cloud boundary
sRuns.addShape(pres.ShapeType.roundRect, {
  x: 0.5, y: 1.95, w: 10.35, h: 3.55, rectRadius: 0.05,
  fill: { color: WHITE }, line: { color: BLUE, width: 1.25 },
});
sRuns.addText("Google Cloud", {
  isTextBox: true, x: 0.72, y: 2.03, w: 2.2, h: 0.26,
  fontFace: F, fontSize: 10, bold: true, color: BLUE, charSpacing: 0.5, margin: 0,
});

const ZONES = [
  { label: "SOURCES", x: 0.75, tiles: [
      { icon: "glyphs/bq.png",     t: "BigQuery" },
      { icon: "glyphs/kc.png",     t: "Knowledge Catalog" },
      { icon: "glyphs/looker.png", t: "Looker" },
    ] },
  { label: "COMPILE", x: 4.17, tiles: [
      { n: 1, t: "Read the sources" },
      { n: 2, t: "Resolve and extract" },
      { n: 3, t: "Compile and validate" },
    ] },
  { label: "SERVE", x: 7.59, tiles: [
      { n: 4, icon: "glyphs/radixgraphW.png", t: "Versioned snapshot", hero: true },
      { n: 5, icon: "glyphs/gemini.png", t: "Harness + Gemini" },
      { n: 6, icon: "glyphs/lumi.png", t: "Surfaces" },
    ] },
];
const ZW = 3.0, ZY = 2.45, ZH = 2.85;

function numCircle(slide, x, y, n, onNavy) {
  slide.addShape(pres.ShapeType.ellipse, {
    x, y, w: 0.26, h: 0.26,
    fill: { color: onNavy ? WHITE : BLUE }, line: { color: onNavy ? WHITE : BLUE, width: 0.5 },
  });
  slide.addText(String(n), {
    isTextBox: true, x, y: y + 0.015, w: 0.26, h: 0.24,
    fontFace: F, fontSize: 11, bold: true, color: onNavy ? NAVY : WHITE,
    align: "center", valign: "middle", margin: 0,
  });
}

ZONES.forEach((z) => {
  sRuns.addShape(pres.ShapeType.roundRect, {
    x: z.x, y: ZY, w: ZW, h: ZH, rectRadius: 0.05,
    fill: { color: TINT }, line: { color: BORDER, width: 1, dashType: "dash" },
  });
  sRuns.addText(z.label, {
    isTextBox: true, x: z.x + 0.18, y: ZY + 0.12, w: ZW - 0.36, h: 0.24,
    fontFace: F, fontSize: 9.5, bold: true, color: GREY, charSpacing: 0.9, margin: 0,
  });
  z.tiles.forEach((tile, j) => {
    const ty = ZY + 0.46 + j * 0.78;
    sRuns.addShape(pres.ShapeType.roundRect, {
      x: z.x + 0.18, y: ty, w: ZW - 0.36, h: 0.66, rectRadius: 0.05,
      fill: { color: tile.hero ? NAVY : WHITE },
      line: { color: tile.hero ? NAVY : BORDER, width: 1 },
    });
    let tx = z.x + 0.32;
    if (tile.n !== undefined) {
      numCircle(sRuns, tx, ty + 0.2, tile.n, tile.hero);
      tx += 0.38;
    }
    if (tile.icon) {
      sRuns.addImage({ path: tile.icon, x: tx, y: ty + 0.19, w: 0.28, h: 0.28 });
      tx += 0.38;
    }
    sRuns.addText(tile.t, {
      isTextBox: true, x: tx, y: ty + 0.06, w: z.x + ZW - 0.18 - tx - 0.1, h: 0.54,
      fontFace: F, fontSize: 11, bold: true, color: tile.hero ? WHITE : NAVY,
      valign: "middle", margin: 0,
    });
  });
});

// flow between the zones, and out to the people
arrow(sRuns, 3.83, ZY + ZH / 2, 0.3);
arrow(sRuns, 7.25, ZY + ZH / 2, 0.3);
arrow(sRuns, 10.93, ZY + ZH / 2, 0.3);

sRuns.addShape(pres.ShapeType.roundRect, {
  x: 11.35, y: ZY, w: 1.48, h: ZH, rectRadius: 0.05,
  fill: { color: TINT2 }, line: { color: BORDER, width: 1, dashType: "dash" },
});
sRuns.addText("PEOPLE", {
  isTextBox: true, x: 11.5, y: ZY + 0.12, w: 1.2, h: 0.24,
  fontFace: F, fontSize: 9.5, bold: true, color: GREY, charSpacing: 0.9, margin: 0,
});
["Analyst", "Leader", "Steward"].forEach((p, j) => {
  const ty = ZY + 0.46 + j * 0.78;
  sRuns.addShape(pres.ShapeType.roundRect, {
    x: 11.5, y: ty, w: 1.18, h: 0.66, rectRadius: 0.05,
    fill: { color: WHITE }, line: { color: BORDER, width: 1 },
  });
  sRuns.addText(p, {
    isTextBox: true, x: 11.6, y: ty + 0.06, w: 1.0, h: 0.54,
    fontFace: F, fontSize: 11, bold: true, color: NAVY, valign: "middle", margin: 0,
  });
});

// the numbered walkthrough — the diagram explains itself
const WALK = [
  "Read what the team already produces.",
  "Resolve identity, then extract meaning with its evidence.",
  "Compile as one unit; publish only if every check passes.",
  "One versioned snapshot — the single thing everything reads.",
  "Gemini answers through the harness, never from free recall.",
  "People ask, explore and steward. Numbers carry definitions.",
];
const WX = [0.5, 4.74, 8.98];
WALK.forEach((t, i) => {
  const x = WX[i % 3], y = 5.72 + Math.floor(i / 3) * 0.62;
  numCircle(sRuns, x, y, i + 1, false);
  sRuns.addText(t, {
    isTextBox: true, x: x + 0.36, y: y - 0.04, w: 3.48, h: 0.42,
    fontFace: F, fontSize: 10.5, color: INK, valign: "middle", margin: 0,
  });
});

sRuns.addNotes(
  "The platform view, and the one to use if somebody asks what this costs to run. Nothing " +
  "exotic: the warehouse and the catalog are already yours, the model is Gemini, and the only " +
  "new thing inside the boundary is the compile step and the snapshot it publishes. Walk the " +
  "six numbers in order — that is the whole system in one pass. The line worth repeating is " +
  "step 5: the model never answers from free recall, it answers through the harness, and the " +
  "harness only reads step 4."
);

// ═══ 9 · why you can trust it ════════════════════════════════════════════════
const s8 = pres.addSlide();
head(s8, "WHY YOU CAN TRUST IT", "It would rather say nothing than guess",
  "Three things it will not do. Each one is a reason to believe the answers it does give.");

const REFUSALS = [
  { ic: "help", quote: "“Nobody has agreed this one yet.”",
    body: ["No certified definition exists for this metric, so it will not hand you a number as though one did.",
           "It shows you what it found and who would need to sign it."],
    caption: "a gap, stated" },
  { ic: "block", quote: "“That is outside the snapshot.”",
    body: ["If something is not in the compiled picture, it does not get answered from the model's memory.",
           "No plausible-sounding invention, ever."],
    caption: "no answers from nowhere" },
  { ic: "science", quote: "“This one is still exploratory.”",
    body: ["A figure it assembled from parts carries that mark until a check clears it.",
           "You always know which kind of number you are looking at."],
    caption: "marked until proven" },
];
REFUSALS.forEach((r, i) => {
  const x = C3.x[i];
  s8.addShape(pres.ShapeType.roundRect, {
    x, y: 2.05, w: C3.w, h: 3.44, rectRadius: 0.06,
    fill: { color: WHITE }, line: { color: BLUE, width: 1.6 }, shadow: shadow(),
  });
  iconCircle(s8, x + 0.28, 2.26, 0.56, r.ic, "light");
  s8.addText(r.quote, {
    isTextBox: true, x: x + 0.28, y: 2.96, w: C3.w - 0.56, h: 0.78,
    fontFace: F, fontSize: 17, bold: true, color: NAVY, lineSpacingMultiple: 1.1, margin: 0,
  });
  s8.addText(
    r.body.map((t, j) => ({
      text: t, options: { breakLine: j < r.body.length - 1, paraSpaceAfter: 9 },
    })),
    { isTextBox: true, x: x + 0.28, y: 3.86, w: C3.w - 0.56, h: 1.44,
      fontFace: F, fontSize: 12, color: INK, lineSpacingMultiple: 1.12, margin: 0 }
  );
  s8.addText(r.caption, {
    isTextBox: true, x: x + 0.28, y: 5.08, w: C3.w - 0.56, h: 0.3,
    fontFace: F, fontSize: 10, italic: true, color: GREY, margin: 0,
  });
});
closer(s8, 5.88, "Anyone can build something that always answers.",
  "A system that will admit a gap is the only kind you can act on without checking.", false);
s8.addNotes(
  "This is the credibility beat, and for a sceptical room it is the most important slide in the " +
  "deck. The instinct is to sell coverage; we are selling the opposite. Each refusal maps to " +
  "something real: no certified definition means no number; nothing outside the compiled " +
  "snapshot gets answered from memory; anything assembled from parts stays marked until a check " +
  "clears it. Expect the question 'how often does it refuse' — the honest answer is that it " +
  "refuses exactly as often as the business has not agreed, and that number falls as stewards " +
  "certify."
);

// ═══ 10 · what changes for you ════════════════════════════════════════════════
const s9 = pres.addSlide();
head(s9, "WHAT CHANGES FOR YOU", "Four people, the same Monday",
  "Nothing here needs a new tool on anybody's desk, or a new process to follow.");

const PEOPLE = [
  ["analyst", "The analyst", "rebuilds the same query for the fifth time", "starts from the definition everyone already agreed"],
  ["leader", "The leader", "reconciles two decks before the meeting", "reads one number with its source attached"],
  ["newcomer", "The newcomer", "asks three people what a term means", "finds what the business means on day one"],
  ["steward", "The steward", "defends definitions in comment threads", "changes one, on the record, and everyone sees it"],
];
PEOPLE.forEach((p, i) => {
  const y = 2.1 + i * 0.95;
  s9.addShape(pres.ShapeType.roundRect, {
    x: 0.5, y, w: 12.33, h: 0.8, rectRadius: 0.06,
    fill: { color: i % 2 === 0 ? TINT : WHITE }, line: { color: BORDER, width: 1 },
    shadow: shadow(),
  });
  iconCircle(s9, 0.78, y + 0.16, 0.48, p[0], "light");
  s9.addText(p[1], {
    isTextBox: true, x: 1.42, y: y + 0.04, w: 1.9, h: 0.72,
    fontFace: F, fontSize: 13.5, bold: true, color: NAVY, valign: "middle", margin: 0,
  });
  s9.addText(p[2], {
    isTextBox: true, x: 3.4, y: y + 0.04, w: 4.0, h: 0.72,
    fontFace: F, fontSize: 12, color: GREY, valign: "middle", margin: 0,
  });
  arrow(s9, 7.52, y + 0.4, 0.42);
  s9.addText(p[3], {
    isTextBox: true, x: 8.2, y: y + 0.04, w: 4.3, h: 0.72,
    fontFace: F, fontSize: 12, bold: true, color: INK, valign: "middle", margin: 0,
  });
});
closer(s9, 6.15, "Same people, same questions, same Monday.",
  "What changes is that the answer arrives with its meaning attached.", false);
s9.addNotes(
  "Make it about them, not the system. Read one row aloud — the analyst one usually gets a " +
  "laugh of recognition — and let the others land by themselves. The steward row is the one to " +
  "linger on if governance people are in the room: they stop arguing in threads and start " +
  "changing the record once."
);

// ═══ 11 · where it stands ════════════════════════════════════════════════════
const s10 = pres.addSlide();
s10.background = { color: NAVY };
motif(s10);
darkHead(s10, "WHERE IT STANDS");
s10.addText("Working today, on a real warehouse", {
  isTextBox: true, x: 0.7, y: 1.15, w: 11.9, h: 0.8,
  fontFace: F, fontSize: 36, bold: true, color: WHITE, margin: 0,
});
s10.addText("Not a prototype, and not finished either. Here is the honest state of it.", {
  isTextBox: true, x: 0.7, y: 2.0, w: 9.5, h: 0.35,
  fontFace: F, fontSize: 14, color: PALE, margin: 0,
});

const STATE = [
  { ic: "verified", label: "WORKING END TO END", ours: true,
    lines: ["The pipeline, the compiled snapshot and both surfaces.",
            "The assistant answers and runs queries under real limits.",
            "Proven against a reference warehouse, not a toy dataset."] },
  { ic: "clock", label: "STILL EARLY", ours: false,
    lines: ["A couple of lanes are a first cut.",
            "The export is basic.",
            "One plane is a validated candidate rather than the default."] },
  { ic: "people", label: "WHAT WE NEED", ours: false,
    lines: ["The definitions your team already treats as certified.",
            "A name against each one who can sign it.",
            "One business area to go deep on first."] },
];
const SX = [0.7, 4.79, 8.88], SW = 3.74;
STATE.forEach((s, i) => {
  s10.addShape(pres.ShapeType.roundRect, {
    x: SX[i], y: 2.75, w: SW, h: 2.62, rectRadius: 0.08,
    fill: s.ours ? { color: BLUE, transparency: 70 } : { color: WHITE, transparency: 90 },
    line: { color: s.ours ? "7EB2EC" : WHITE, width: 1, transparency: 50 },
  });
  iconCircle(s10, SX[i] + 0.3, 2.95, 0.46, s.ic, "onNavy");
  s10.addText(s.label, {
    isTextBox: true, x: SX[i] + 0.88, y: 2.96, w: SW - 1.18, h: 0.44,
    fontFace: F, fontSize: 10.5, bold: true, color: s.ours ? WHITE : ONNAVY2,
    charSpacing: 1.0, valign: "middle", margin: 0,
  });
  s10.addText(
    s.lines.map((t, j) => ({
      text: t, options: { breakLine: j < s.lines.length - 1, paraSpaceAfter: 8 },
    })),
    { isTextBox: true, x: SX[i] + 0.3, y: 3.62, w: SW - 0.6, h: 1.7,
      fontFace: F, fontSize: 12, color: s.ours ? WHITE : ONNAVY,
      lineSpacingMultiple: 1.12, valign: "top", margin: 0 }
  );
});
closer(s10, 5.55, "We are not asking you to adopt a tool.",
  "We are asking which definitions are worth agreeing on first.", true);
s10.addText("Everything else here already runs.", {
  isTextBox: true, x: 0.7, y: 6.15, w: 11.9, h: 0.4,
  fontFace: F, fontSize: 13.5, color: ONNAVY, margin: 0,
});
s10.addNotes(
  "Close honestly — the 'still early' card buys more credibility than it costs, and it is the " +
  "reason the room believes the first card. The ask is deliberately small and not a budget " +
  "request: name the definitions that are already treated as certified, put a person against " +
  "each, and pick one business area. CHECK BEFORE PRESENTING: confirm the third card matches " +
  "what you actually want to ask this audience for."
);

pres.writeFile({ fileName: "radix-graph-end-to-end.pptx" }).then((f) => console.log("wrote", f));
