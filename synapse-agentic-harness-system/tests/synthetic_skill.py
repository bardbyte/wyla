"""A synthetic skill pack the size of a real runtime knowledge bundle
(~2.5 MB): hundreds of sections under a dozen parts, each about two
invented terms it alone owns, with near-duplicate twins (the same
prose around different terms), tables, fenced code, and cross
references — the ground truth the retrieval tests score against.

Deterministic: the same seed writes the same pack, so a query's right
section is known before the index is built."""

from __future__ import annotations

import random
from dataclasses import dataclass

SYLLABLES = ["vor", "tan", "dril", "lex", "mo", "quen", "sab", "ri", "pol",
             "gar", "nu", "tesh", "ka", "lom", "bry", "sel", "dun", "fex",
             "zir", "hal", "pim", "wor", "cly", "ost", "rav", "min", "jut"]
FILLER = [
    "The step is owned by the data platform team and runs on the shared "
    "scheduler once the upstream loads report complete.",
    "Every value written by this step carries the run id so a later "
    "reconciliation can name the exact load it came from.",
    "When the ceiling is reached the job pauses and the on-call reads the "
    "queue depth before deciding whether to widen it.",
    "The definition line is the sentence the number carries wherever it "
    "is shown, and this section says how it is composed.",
    "A partition filter on the load date keeps the scan under the ceiling; "
    "without one the dry run refuses for cost.",
    "The metric is certified when the check passes twice in a row against "
    "the compiled build; until then it stays exploratory.",
    "Rows with a null key are dropped before the join and counted in the "
    "coverage note that ships with every result.",
    "The naming habit is the market's own: two letters for the region, "
    "then the product, then the grain.",
    "Nothing in this section adds a table or a metric to the world; it "
    "says where to look first and what to check after.",
    "The analyst reads the card whole before using the number, and the "
    "card's join line names the raw-safe path.",
]
TOPIC = [
    "The {a} step runs before the {b} pass and writes its rows first.",
    "When {a}s pile up, the {b} pass waits until the backlog drains.",
    "Reconciling {a} against {b} happens nightly after the close.",
    "A {b}ing failure leaves the {a} half done; rerun from the checkpoint.",
    "The {a} quota and the {b} window are set together, never apart.",
    "Check the {a} ledger before the {b} rollup, not after.",
]


@dataclass(frozen=True)
class Truth:
    """One section's ground truth: its terms, its heading path and id
    (s<N>, in document order), and whether it is a twin."""

    a: str
    b: str
    heading: str
    path: str
    section_id: str
    twin_of: str = ""
    table: bool = False         # carries a settings table
    fence: bool = False         # carries a fenced SQL block


def _words(rng: random.Random, n: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    while len(out) < n:
        w = "".join(rng.choice(SYLLABLES) for _ in range(rng.choice((2, 3))))
        if w in seen or any(w.startswith(o) or o.startswith(w) for o in out):
            continue
        seen.add(w)
        out.append(w)
    return out


def build_pack(target_chars: int = 2_500_000,
               seed: int = 7) -> tuple[str, list[Truth]]:
    """(markdown, truths) — the pack and every section's ground truth
    in document order. Parts are added until the size is reached."""
    rng = random.Random(seed)
    truths: list[Truth] = []
    lines: list[str] = ["# Runtime knowledge bundle", "",
                        "A synthetic bundle the retrieval tests read. Each "
                        "section owns two terms nothing else owns.", ""]
    words = _words(rng, 4000)
    wi = 0
    section_no = 1                      # s1 is the H1 above
    body_chars = 0
    part = 0
    while body_chars < target_chars:
        part += 1
        part_title = f"Part {part}: {words[wi].capitalize()} operations"
        wi += 1
        lines += [f"## {part_title}", "",
                  f"What the {part_title.lower()} cover, and the checks "
                  "that stand behind them.", ""]
        section_no += 1
        per_part = 36
        for j in range(per_part):
            a, b = words[wi], words[wi + 1]
            wi += 2
            kind = j % 3
            if kind == 0:
                heading = f"Handling {a} {b} drift"
            elif kind == 1:
                heading = f"The {a} ledger and the {b} rollup"
            else:
                heading = f"{a.capitalize()} quotas under the {b} window"
            path = f"Runtime knowledge bundle > {part_title} > {heading}"
            section_no += 1
            truths.append(Truth(a, b, heading, path, f"s{section_no}",
                                table=j % 3 == 0, fence=j % 4 == 1))
            body = _section_body(rng, a, b, j, words[wi + 200])
            lines += [f"### {heading}", ""] + body + [""]
            if j % 5 == 2:
                # a subsection: H4 under this H3
                sub = f"Edge cases in {a} handling"
                section_no += 1
                truths.append(Truth(a, b, sub, f"{path} > {sub}",
                                    f"s{section_no}"))
                lines += [f"#### {sub}", "",
                          f"Three edge cases for the {a} step: an empty "
                          f"{b} window, a duplicate {a} key, and a late "
                          f"{b} close.", "", rng.choice(FILLER), ""]
            if j % 10 == 4:
                # the twin: the same prose around two other terms
                a2, b2 = words[wi], words[wi + 1]
                wi += 2
                twin = f"{heading} (legacy)"
                section_no += 1
                truths.append(Truth(a2, b2, twin,
                                    f"Runtime knowledge bundle > "
                                    f"{part_title} > {twin}",
                                    f"s{section_no}",
                                    twin_of=f"s{section_no - 1}",
                                    table=j % 3 == 0, fence=j % 4 == 1))
                twin_body = [line.replace(a, a2).replace(b, b2)
                             for line in body]
                lines += [f"### {twin}", ""] + twin_body + [""]
        body_chars = sum(len(x) + 1 for x in lines)
    return "\n".join(lines) + "\n", truths


def _section_body(rng: random.Random, a: str, b: str, j: int,
                  other: str) -> list[str]:
    out: list[str] = []
    # every topic sentence appears once, somewhere in the section: the
    # ground truth for a query phrased from it is then exact
    order = list(TOPIC)
    rng.shuffle(order)
    for topic in order:
        sentences = [topic.format(a=a, b=b)]
        if rng.random() < 0.5:
            sentences.append(rng.choice(TOPIC).format(a=a, b=b))
        sentences += [rng.choice(FILLER) for _ in range(rng.randint(5, 8))]
        rng.shuffle(sentences)
        out += [" ".join(sentences), ""]
    out += [f"See also the {other} step, which shares the scheduler.", ""]
    if j % 3 == 0:
        out += ["| setting | value | note |", "|---|---|---|",
                f"| {a}-quota | {rng.randint(50, 900)} | per window |",
                f"| {b}-window | {rng.randint(5, 60)} min | wall clock |",
                f"| {a}-retries | {rng.randint(1, 5)} | before paging |",
                ""]
    if j % 4 == 1:
        out += ["```sql", f"SELECT load_date, COUNT(*) AS {a}_rows",
                f"FROM dw.{a}_{b}_ledger", "WHERE load_date >= '2026-01-01'",
                "GROUP BY load_date", "```", ""]
    return out


def queries(truths: list[Truth], n: int = 20) -> list[tuple[str, Truth]]:
    """Twenty asks phrased unlike the headings — inflected terms, the
    body's words, a table cell, a code identifier — each with the
    section that should rank first."""
    originals = [t for t in truths if not t.twin_of and " > " in t.path
                 and t.path.count(" > ") == 2]
    # (template, the feature the section must carry: "" | table | fence)
    templates = [
        ("what happens when {a}s pile up", ""),
        ("{b}ing failure left the {a} half done, where do I rerun", ""),
        ("nightly reconcile of {a} versus {b} after close", ""),
        ("does the {a} step write before the {b} pass", ""),
        ("{a}-quota setting", "table"),
        ("{a}_{b}_ledger rows by load date", "fence"),
        ("{a} ledger before {b} rollup", ""),
        ("why is the {b} pass waiting on {a} backlog", ""),
        ("set the {a} quota and the {b} window together", ""),
        ("checkpoint rerun for {b}ing failures on {a}", ""),
    ]
    out = []
    step = max(1, len(originals) // n)
    for i in range(n):
        template, needs = templates[i % len(templates)]
        pool = [t for t in originals
                if (needs == "table" and t.table) or (needs == "fence" and t.fence)
                or not needs]
        t = pool[(i * step + 3) % len(pool)]
        out.append((template.format(a=t.a, b=t.b), t))
    return out


__all__ = ["Truth", "build_pack", "queries", "FILLER", "TOPIC"]
