"""Artifact types and the rendering rules (Synapse v2 §5/§6).

Artifacts are the outputs the user keeps: standalone, versioned,
exportable. Governance lives HERE now, as schema, not as a gate in
front of the conversation:

  Rule 1 — any artifact that shows a number carries its definition
  status and meridian line in-schema. The validator refuses one that
  does not, with a teaching message; the model fixes its own spec.

  Rule 2 — a status of ``composed`` keeps the EXPLORATORY watermark
  until a passing reconcile/crosscheck fact from THIS trajectory is
  cited. The watermark is forced by the validator, never negotiated
  by prose. (Check facts arrive with the §13.2 toolkit; until a fact
  is cited, composed numbers simply stay watermarked.)

  Rule 3 lives elsewhere: nothing here writes truth; the clerk is
  still the only writer.

Every stored spec is normalized: build id stamped, watermark decided,
unknown fields dropped. What the panel renders is exactly what the
validator passed — the renderer never patches an artifact up.

Two pure helpers ride here too, and the surfaces carry their JS twins
in ``js/artifacts-render.js`` (same cases, same answers):

  ``number_format(values, hint)`` — how a column of numbers reads:
  its kind (percent, currency, count, ratio, duration, date), the
  decimals the DATA carries (never a fixed default), thousands
  grouping, compact units past 100k, the unit. The column's name and
  any unit/format metadata speak first, then the values.

  ``choose_visual(rows, columns, intent)`` — which picture answers
  the question, as a written heuristic (docs/visualizations.md):
  the kind, the encoding and a one-sentence reason the card shows.
"""

from __future__ import annotations

import math
import re
from typing import Any

TYPES: tuple[str, ...] = ("chart", "table", "document", "kpi",
                          "dashboard", "diagram")
# every chart kind the validator accepts and both surfaces draw
CHART_KINDS = ("line", "area", "bar", "hbar", "stacked_bar",
               "percent_bar", "scatter", "histogram", "heatmap",
               "small_multiples", "combo", "waterfall")
# what the model may write for a kind: spelled-out and older names
CHART_KIND_ALIASES = {
    "horizontal_bar": "hbar", "horizontal bar": "hbar", "barh": "hbar",
    "stacked": "stacked_bar", "stacked bar": "stacked_bar",
    "stack": "stacked_bar",
    "hundred_percent": "percent_bar", "hundred_bar": "percent_bar",
    "100%": "percent_bar", "100_percent": "percent_bar",
    "percent": "percent_bar", "normalized_bar": "percent_bar",
    "facets": "small_multiples", "small multiples": "small_multiples",
    "facet": "small_multiples", "multiples": "small_multiples",
    "bar_line": "combo", "bar+line": "combo", "bridge": "waterfall",
    "column": "bar", "hist": "histogram", "matrix": "heatmap",
    "points": "scatter", "dots": "scatter",
}
# kinds drawn from {series: [{name, points: [[x, y]]}]}
SERIES_KINDS = ("line", "area", "bar", "hbar", "stacked_bar",
                "percent_bar", "scatter", "small_multiples", "combo",
                "waterfall", "histogram")
# series ceilings: six reads; small multiples are one picture each
MAX_SERIES = 6
MAX_FACETS = 24
MAX_POINTS = 2000
MAX_SCATTER_POINTS = 5000
MAX_HEATMAP_SIDE = 60
DIAGRAM_KINDS = ("graph", "mermaid")
# a dashboard nests tiles, never another dashboard (or a diagram)
PANEL_TYPES = ("kpi", "chart", "table", "document")
STATUSES = ("certified", "pending", "composed", "exploratory")
# statuses that may shed the watermark without a cited check fact
SELF_STANDING = ("certified", "pending")
FORMAT_KINDS = ("percent", "currency", "count", "ratio", "duration",
                "date", "number", "identifier", "text")
SORTS = ("x", "-x", "y", "-y", "none")


def _problem(code: str, detail: str, hint: str) -> dict[str, str]:
    return {"code": code, "detail": detail, "hint": hint}


def _numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


# ═══ number formatting, from the data ═══════════════════════════
# The JS twin (numberFormat / formatValue in js/artifacts-render.js)
# follows this section line for line; tests/fixtures/number_format_
# cases.json holds the shared cases.

MAX_DECIMALS = 4
CURRENCY_DECIMALS = 2
COMPACT_ABOVE = 100_000            # the median magnitude that compacts
NULL_TEXT = "—"
CURRENCY_SYMBOLS = {"USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥",
                    "INR": "₹", "CAD": "$", "AUD": "$",
                    "$": "$", "€": "€", "£": "£", "¥": "¥", "₹": "₹"}
CURRENCY_CODES = ("usd", "eur", "gbp", "jpy", "inr", "cad", "aud",
                  "chf", "cny", "mxn", "brl", "sgd", "hkd")
DURATION_UNITS = {"ms": "ms", "millisecond": "ms", "milliseconds": "ms",
                  "s": "s", "sec": "s", "secs": "s", "second": "s",
                  "seconds": "s", "min": "min", "mins": "min",
                  "minute": "min", "minutes": "min", "h": "h", "hr": "h",
                  "hrs": "h", "hour": "h", "hours": "h", "d": "d",
                  "day": "d", "days": "d"}
_DATE_VALUE = re.compile(r"^\d{4}-\d{2}(-\d{2})?([T ].*)?$")
_ID_NAME = re.compile(
    r"(^|_)(id|ids|uuid|guid|code|zip|postal|phone|sku|iban|key|"
    r"account_no|acct|mid|pan)($|_)|_number$|_no$")
_DATE_NAME = re.compile(
    r"(^|_)(date|day|month|week|quarter|year|period|dt|ts|time|"
    r"timestamp)($|_)|_at$|_on$")
_PERCENT_NAME = re.compile(
    r"(^|_)(pct|percent|percentage|share|rate|conversion|penetration|"
    r"cvr|ctr|apr)($|_)|_pc$|%")
_CURRENCY_NAME = re.compile(
    r"(^|_)(spend|revenue|sales|cost|costs|amount|amt|price|fee|fees|"
    r"gmv|usd|dollars|value|margin|profit|income|budget|"
    r"volume_usd|tpv|aov)($|_)")
_COUNT_NAME = re.compile(
    r"(^|_)(count|cnt|n|num|number|qty|quantity|transactions|txns|"
    r"users|rows|orders|visits|sessions|customers|merchants|"
    r"accounts|events|clicks|impressions|total)($|_)|^number_|^num_")
_RATIO_NAME = re.compile(r"(^|_)(ratio|multiple|index|factor|per)($|_)"
                         r"|_per_|_x$")
_DURATION_NAME = re.compile(
    r"(^|_)(duration|latency|elapsed|age|tenure|ttl|wait)($|_)|"
    r"_(ms|secs?|seconds|mins?|minutes|hours|hrs|days)$")


def _norm_name(name: Any) -> str:
    return re.sub(r"[^a-z0-9%]+", "_", str(name or "").lower()).strip("_")


def _as_number(value: Any) -> float | None:
    """A number, or a numeric string, as a float; anything else None
    (bool is not a number, and neither is an empty cell)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return None
        try:
            out = float(text)
        except ValueError:
            return None
        return out if math.isfinite(out) else None
    return None


def _decimals_of(value: float) -> int:
    """The decimals a value carries, as written (0.125 → 3, 12.50 → 1,
    0.1 + 0.2 → 1): ten significant digits, trailing zeros dropped,
    never more than MAX_DECIMALS."""
    if not math.isfinite(value) or float(value).is_integer():
        return 0
    text = f"{value:.10g}"
    if "e" in text or "E" in text:
        text = f"{value:.{MAX_DECIMALS}f}".rstrip("0")
    if "." not in text:
        return 0
    return min(MAX_DECIMALS, len(text.split(".")[1].rstrip("0")))


def _median(nums: list[float]) -> float:
    if not nums:
        return 0.0
    ordered = sorted(nums)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _kind_from_unit(unit: str) -> tuple[str, str]:
    """(kind, unit) from a unit word; ("", unit) when it says nothing."""
    u = unit.strip()
    low = u.lower()
    if not u:
        return "", ""
    if low in ("%", "percent", "pct", "percentage"):
        return "percent", "%"
    if u.upper() in CURRENCY_SYMBOLS or low in CURRENCY_CODES:
        return "currency", u.upper() if len(u) == 3 else u
    if low in DURATION_UNITS:
        return "duration", DURATION_UNITS[low]
    if low in ("x", "ratio", "multiple"):
        return "ratio", "x" if low == "x" else ""
    if low in ("count", "n", "rows", "number", "#"):
        return "count", ""
    if low in ("date", "day", "month"):
        return "date", ""
    return "", u


def _kind_from_name(name: str) -> tuple[str, str]:
    """(kind, unit) from a column name; a duration's unit rides its
    suffix (latency_ms → ms), every other unit comes from elsewhere."""
    if not name:
        return "", ""
    if _ID_NAME.search(name):
        return "identifier", ""
    if _PERCENT_NAME.search(name):
        return "percent", "%"
    if _DURATION_NAME.search(name):
        tail = name.rsplit("_", 1)[-1]
        return "duration", DURATION_UNITS.get(tail, "")
    if _RATIO_NAME.search(name):
        return "ratio", ""
    if _CURRENCY_NAME.search(name):
        return "currency", ""
    if _COUNT_NAME.search(name):
        return "count", ""
    if _DATE_NAME.search(name):
        return "date", ""
    return "", ""


def _clean_format(fmt: Any) -> dict[str, Any]:
    """A spec's explicit format, normalized: a kind word, or an object
    {kind?, decimals?, unit?, compact?, grouping?, sign?, scale?}."""
    if isinstance(fmt, str):
        word = fmt.strip().lower()
        if word in FORMAT_KINDS:
            return {"kind": word}
        kind, unit = _kind_from_unit(fmt)
        return {"kind": kind, "unit": unit} if kind else {}
    if not isinstance(fmt, dict):
        return {}
    out: dict[str, Any] = {}
    kind = str(fmt.get("kind", "")).strip().lower()
    if kind in FORMAT_KINDS:
        out["kind"] = kind
    if _numeric(fmt.get("decimals")):
        out["decimals"] = max(0, min(MAX_DECIMALS, int(fmt["decimals"])))
    if fmt.get("unit") is not None:
        out["unit"] = str(fmt["unit"])[:12]
    for flag in ("compact", "grouping"):
        if isinstance(fmt.get(flag), bool):
            out[flag] = fmt[flag]
    if fmt.get("sign") in ("auto", "always"):
        out["sign"] = fmt["sign"]
    if fmt.get("scale") in (1, 100):
        out["scale"] = fmt["scale"]
    return out


def number_format(values: list[Any] | None,
                  hint: dict[str, Any] | None = None) -> dict[str, Any]:
    """How a column of values reads, inferred from the column's name
    and metadata first, then from the values themselves.

    → {kind, decimals, grouping, compact, unit, scale, sign, null}

    kind      percent | currency | count | ratio | duration | date |
              number | identifier | text
    decimals  the max decimals present in the data, capped at 4 (2 for
              currency, 0 for counts and identifiers); a percent given
              as a fraction (0.125) counts its decimals after the ×100
              (→ 1, so 12.5%)
    scale     100 when a percent column is a fraction — every value
              within [-1, 1] — else 1 (the values are already points);
              the rule is documented in docs/visualizations.md
    grouping  thousands separators, off for identifiers and dates
    compact   K/M/B units when the median magnitude exceeds 100,000 and
              the column is a plain number, a count or a currency
    unit      "%", a currency code or symbol, a duration unit, or the
              unit as given

    hint: {name?, label?, unit?, format?} — the column's metadata;
    ``format`` (a kind word or an object) overrides every inferred key
    it names."""
    hint = hint or {}
    name = _norm_name(hint.get("name") or hint.get("key")
                      or hint.get("label"))
    explicit = _clean_format(hint.get("format"))
    unit_kind, unit = _kind_from_unit(str(hint.get("unit") or ""))
    nums = [n for n in (_as_number(v) for v in (values or []))
            if n is not None]
    present = [v for v in (values or [])
               if v is not None and v != "" and not (
                   isinstance(v, float) and math.isnan(v))]
    dates = sum(1 for v in present
                if isinstance(v, str) and _DATE_VALUE.match(v.strip()))

    name_kind, name_unit = _kind_from_name(name)
    kind = explicit.get("kind") or unit_kind or name_kind
    if not unit and name_unit:
        unit = name_unit
    if not kind:
        if present and dates / len(present) >= 0.8:
            kind = "date"
        elif not nums:
            kind = "text"
        elif len(nums) < len(present) * 0.8:
            kind = "text"
        else:
            kind = "number"
    plain = False
    if kind == "date" and not explicit.get("kind") and not dates \
            and present:
        # the name says date but the values do not: "year" as 2024,
        # 2025 is a number that never groups; "day" as Mon, Tue is a
        # label
        kind = "number" if nums else "text"
        plain = True
    elif kind == "identifier" and not nums and present and dates:
        kind = "date"

    scale = 1
    if kind == "percent":
        fraction = bool(nums) and all(abs(n) <= 1.0 for n in nums)
        scale = 100 if fraction else 1
    if "scale" in explicit:
        scale = explicit["scale"]

    if "decimals" in explicit:
        decimals = explicit["decimals"]
    elif kind in ("count", "identifier", "date", "text"):
        decimals = 0
    elif kind == "percent":
        decimals = max([max(0, _decimals_of(n) - 2) if scale == 100
                        else _decimals_of(n) for n in nums] or [0])
    else:
        decimals = max([_decimals_of(n) for n in nums] or [0])
        if kind == "currency":
            decimals = min(decimals, CURRENCY_DECIMALS)
    decimals = min(MAX_DECIMALS, decimals)

    grouping = kind not in ("identifier", "date", "text") and not plain
    if "grouping" in explicit:
        grouping = explicit["grouping"]
    compact = (kind in ("number", "count", "currency")
               and _median([abs(n) for n in nums]) > COMPACT_ABOVE)
    if "compact" in explicit:
        compact = explicit["compact"]
    if "unit" in explicit:
        unit = explicit["unit"]
    elif kind == "percent":
        unit = "%"
    return {"kind": kind, "decimals": decimals, "grouping": grouping,
            "compact": compact, "unit": unit, "scale": scale,
            "sign": explicit.get("sign", "auto"), "null": NULL_TEXT}


def _group(digits: str) -> str:
    out = []
    while len(digits) > 3:
        out.insert(0, digits[-3:])
        digits = digits[:-3]
    out.insert(0, digits)
    return ",".join(out)


def _fixed(value: float, decimals: int, grouping: bool) -> str:
    """|value| to ``decimals`` places, rounded half away from zero on
    the same float arithmetic the JS twin uses, so both sides agree
    on every case (Python's format() rounds half to even)."""
    factor = 10 ** decimals
    n = int(math.floor(abs(value) * factor + 0.5 + 1e-9))
    whole, frac = divmod(n, factor)
    text = _group(str(whole)) if grouping else str(whole)
    if decimals:
        text += "." + str(frac).rjust(decimals, "0")
    return text


def _compact(value: float) -> str:
    mag = abs(value)
    for unit, label in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if mag >= unit:
            scaled = mag / unit
            text = _fixed(scaled, 0 if scaled >= 100 else 1, False)
            if text.endswith(".0"):
                text = text[:-2]
            return text + label
    return _fixed(mag, 0, True)


def format_value(value: Any, fmt: dict[str, Any] | None = None) -> str:
    """One value through a column's format: the null text for nothing,
    text as given for dates and identifiers, else the number with its
    decimals, grouping, compact unit, sign and unit."""
    fmt = fmt or number_format([value])
    null = str(fmt.get("null", NULL_TEXT))
    if value is None or value == "" or (
            isinstance(value, float) and math.isnan(value)):
        return null
    kind = fmt.get("kind", "number")
    if kind in ("date", "text"):
        return str(value)
    num = _as_number(value)
    if num is None:
        return str(value)
    if kind == "identifier":
        return str(value) if isinstance(value, str) else (
            str(int(num)) if num.is_integer() else str(num))
    num = num * (fmt.get("scale") or 1)
    negative = num < 0
    if fmt.get("compact") and abs(num) >= 1000:
        body = _compact(num)
    else:
        body = _fixed(num, int(fmt.get("decimals", 0)),
                      bool(fmt.get("grouping", True)))
        if body.strip("0.,") == "":
            negative = False     # -0.00 is 0.00
    unit = str(fmt.get("unit") or "")
    if kind == "percent":
        body += "%"
    elif kind == "currency":
        symbol = CURRENCY_SYMBOLS.get(unit.upper() if len(unit) == 3
                                      else unit)
        if symbol:
            body = symbol + body
        elif unit:
            body += " " + unit
    elif unit and kind not in ("count",):
        body += (" " if len(unit) > 1 or kind == "duration" else "") + unit
    sign = "-" if negative else ("+" if fmt.get("sign") == "always"
                                 and num > 0 else "")
    return sign + body


# ═══ the selection heuristic ════════════════════════════════════
# One place decides which picture answers the question; the table in
# docs/visualizations.md is this function, row for row.

LONG_LABEL = 12          # characters: past this a bar goes horizontal
MANY_CATEGORIES = 8      # past this a bar goes horizontal
TABLE_CATEGORIES = 40    # past this it is a table with a top-N chart
MANY_SERIES = 12         # past this: small multiples
PART_CATEGORIES = 6      # a part-of-whole with more parts is not a stack
HISTOGRAM_ROWS = 20      # one measure, no dimension, this many rows
TOP_N = 15
_COMPARE_NAME = re.compile(
    r"(^|_)(prev|previous|prior|last|target|plan|baseline|budget|"
    r"forecast|ly|yoy|py)($|_)")
_PART_INTENT = re.compile(
    r"\b(share|shares|part|parts|mix|composition|breakdown|"
    r"proportion|split|make ?up|of the whole|percent of)\b")
_DIST_INTENT = re.compile(r"\b(distribution|histogram|spread|bins?)\b")
_COMPARE_INTENT = re.compile(r"\b(vs\.?|versus|against|compared?|"
                             r"comparison|change|delta)\b")
_BRIDGE_INTENT = re.compile(r"\b(waterfall|bridge|walk|contribution)\b")
_AREA_INTENT = re.compile(r"\b(area|volume|cumulative|stacked)\b")
_MATRIX_INTENT = re.compile(r"\b(heatmap|matrix|grid|by .+ and .+)\b")


def _columns_of(rows: list[dict[str, Any]],
                columns: list[Any] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in columns or []:
        if isinstance(c, dict) and c.get("key"):
            out.append({"key": str(c["key"]),
                        "label": str(c.get("label") or c["key"]),
                        "unit": str(c.get("unit") or ""),
                        "format": c.get("format")})
        elif isinstance(c, str) and c:
            out.append({"key": c, "label": c, "unit": "", "format": None})
    if not out and rows:
        out = [{"key": str(k), "label": str(k), "unit": "", "format": None}
               for k in rows[0].keys()]
    return out


def _profile(rows: list[dict[str, Any]],
             col: dict[str, Any]) -> dict[str, Any]:
    values = [r.get(col["key"]) for r in rows if isinstance(r, dict)]
    fmt = number_format(values, {"name": col["key"], "unit": col["unit"],
                                 "format": col["format"]})
    present = [v for v in values if v is not None and v != ""]
    nums = [n for n in (_as_number(v) for v in present) if n is not None]
    distinct = {str(v) for v in present}
    return {"key": col["key"], "label": col["label"], "format": fmt,
            "kind": fmt["kind"], "nums": nums,
            "numeric": bool(present) and len(nums) >= len(present) * 0.8
            and fmt["kind"] not in ("date", "text", "identifier"),
            "date": fmt["kind"] == "date",
            "distinct": len(distinct),
            "longest": max([len(str(v)) for v in present] or [0]),
            "unit": col["unit"]}


def _pick(kind: str, reason: str, *, x: str = "", y: list[str] | None = None,
          series: str = "", sort: str = "", **extra: Any) -> dict[str, Any]:
    out = {"kind": kind, "x": x, "y": list(y or []), "series": series,
           "sort": sort, "reason": reason}
    out.update(extra)
    return out


def choose_visual(rows: list[dict[str, Any]], columns: list[Any] | None,
                  intent: str = "") -> dict[str, Any]:
    """Which picture answers the question, from the rows' shape and
    the words of the ask. → {kind, x, y, series, sort, reason, …}:
    the kind is one of CHART_KINDS, or ``kpi`` / ``table``; ``reason``
    is the one sentence the card shows on hover. The heuristic, as
    docs/visualizations.md tabulates it:

      time on x                        → line (area when the ask says
                                         volume; small multiples past 12
                                         series; combo when a second
                                         measure is a plan or target)
      one category + one measure       → bar, sorted; horizontal when
                                         labels are long or > 8 cats;
                                         > 40 cats → table + top-N chart
      two measures, nothing else       → scatter
      part of a whole, ≤ 6 parts       → stacked bar (hundred-percent
                                         when the parts are percents);
                                         never a pie
      a single number                  → kpi; with a comparison → kpi
                                         with delta
      a distribution                   → histogram
      two categories + one measure     → heatmap
      more than 12 series              → small multiples
      no measure                       → table
    """
    rows = [r for r in (rows or []) if isinstance(r, dict)]
    cols = _columns_of(rows, columns)
    ask = str(intent or "").lower()
    if not rows or not cols:
        return _pick("table", "Nothing to draw yet: no rows.")
    profiles = [_profile(rows, c) for c in cols]
    measures = [p for p in profiles if p["numeric"]]
    dates = [p for p in profiles if p["date"]]
    cats = [p for p in profiles
            if not p["numeric"] and not p["date"]]
    # an identifier column with one value per row is a label, ranked
    # last among the categoricals
    cats.sort(key=lambda p: (p["kind"] == "identifier",))
    keys = lambda ps: [p["key"] for p in ps]  # noqa: E731

    if not measures:
        return _pick("table", "No numeric column to draw: the rows read "
                     "best as a table.")

    # a single row: one number, or one number against another
    if len(rows) == 1:
        head = measures[0]
        compare = next((p for p in measures[1:]
                        if _COMPARE_NAME.search(p["key"].lower())), None)
        if compare is None and len(measures) >= 2 \
                and _COMPARE_INTENT.search(ask):
            compare = measures[1]
        if compare is not None:
            return _pick("kpi", f"One number ({head['label']}) against "
                         f"{compare['label']}: a tile with its delta.",
                         y=[head["key"]], compare=compare["key"])
        return _pick("kpi", f"One number ({head['label']}): a tile, "
                     "not a one-bar chart.", y=[head["key"]])

    # a distribution: the ask says so, or one measure with no dimension
    if _DIST_INTENT.search(ask) or (
            len(measures) == 1 and not dates and not cats
            and len(rows) >= HISTOGRAM_ROWS):
        m = measures[0]
        return _pick("histogram", f"How {m['label']} is spread over "
                     f"{len(rows)} rows: a histogram over named bins.",
                     y=[m["key"]])

    # time on x
    if dates:
        x = dates[0]
        others = [p for p in cats if p["distinct"] > 1]
        if others and len(measures) >= 1:
            s = others[0]
            if s["distinct"] > MANY_SERIES:
                return _pick("small_multiples",
                             f"{s['distinct']} {s['label']} series over "
                             f"time: one small line each, shared axes.",
                             x=x["key"], y=[measures[0]["key"]],
                             series=s["key"], sort="x")
            return _pick("line", f"{measures[0]['label']} over "
                         f"{x['label']}, one line per {s['label']}.",
                         x=x["key"], y=[measures[0]["key"]],
                         series=s["key"], sort="x")
        if len(measures) > MANY_SERIES:
            return _pick("small_multiples",
                         f"{len(measures)} measures over time: small "
                         "multiples, one per measure.",
                         x=x["key"], y=keys(measures[:MAX_FACETS]),
                         sort="x")
        if len(measures) == 2:
            a, b = measures
            planned = next((p for p in (a, b)
                            if _COMPARE_NAME.search(p["key"].lower())),
                           None)
            if planned is not None and a["kind"] == b["kind"]:
                actual = b if planned is a else a
                return _pick("combo", f"{actual['label']} as bars against "
                             f"{planned['label']} as a line, one axis.",
                             x=x["key"], y=[actual["key"], planned["key"]],
                             sort="x")
        if len(measures) > MAX_SERIES:
            return _pick("small_multiples",
                         f"{len(measures)} measures over time: past six "
                         "lines a chart stops reading, so one small "
                         "line each.", x=x["key"], y=keys(measures),
                         sort="x")
        if len(measures) == 1 and _AREA_INTENT.search(ask):
            return _pick("area", f"{measures[0]['label']} over "
                         f"{x['label']} as a volume: an area.",
                         x=x["key"], y=[measures[0]["key"]], sort="x")
        return _pick("line", f"{', '.join(p['label'] for p in measures)} "
                     f"over {x['label']}: a line reads change over "
                     "time.", x=x["key"], y=keys(measures), sort="x")

    # a matrix of two categories and a measure
    if len(cats) >= 2 and cats[0]["kind"] != "identifier" \
            and cats[1]["kind"] != "identifier":
        a, b = cats[0], cats[1]
        fits = (a["distinct"] <= MAX_HEATMAP_SIDE
                and b["distinct"] <= MAX_HEATMAP_SIDE)
        if fits and (len(rows) > max(a["distinct"], b["distinct"])
                     or _MATRIX_INTENT.search(ask)):
            return _pick("heatmap", f"{measures[0]['label']} by "
                         f"{a['label']} and {b['label']}: a heatmap, "
                         "colour for magnitude.",
                         x=b["key"], y=[measures[0]["key"]],
                         series=a["key"])

    # one category (the first non-identifier) and measures
    if cats:
        c = cats[0]
        n = c["distinct"]
        m = measures[0]
        if _BRIDGE_INTENT.search(ask) and len(measures) == 1:
            return _pick("waterfall", f"How the steps of {c['label']} add "
                         f"up to {m['label']}: a waterfall.",
                         x=c["key"], y=[m["key"]])
        parts = _PART_INTENT.search(ask) or m["kind"] == "percent"
        if parts and n <= PART_CATEGORIES:
            second = cats[1] if len(cats) > 1 else None
            if m["kind"] == "percent":
                return _pick("percent_bar", f"{c['label']} as parts of a "
                             "whole: a hundred-percent bar, never a "
                             "pie.", x=second["key"] if second else "",
                             y=[m["key"]], series=c["key"])
            return _pick("stacked_bar", f"{c['label']} as parts of "
                         f"{m['label']}: a stacked bar, never a pie.",
                         x=second["key"] if second else "",
                         y=[m["key"]], series=c["key"])
        if n > TABLE_CATEGORIES:
            return _pick("table", f"{n} {c['label']} values: a table, "
                         f"with the top {TOP_N} by {m['label']} as a "
                         "bar chart.", x=c["key"], y=[m["key"]],
                         sort="-y", top_n=TOP_N, chart="hbar")
        if len(measures) > MANY_SERIES:
            return _pick("small_multiples",
                         f"{len(measures)} measures by {c['label']}: "
                         "one small bar chart per measure.",
                         x=c["key"], y=keys(measures[:MAX_FACETS]),
                         sort="-y")
        horizontal = c["longest"] > LONG_LABEL or n > MANY_CATEGORIES
        kind = "hbar" if horizontal else "bar"
        why = ("long labels" if c["longest"] > LONG_LABEL
               else f"{n} categories")
        return _pick(kind, f"{m['label']} by {c['label']}: bars sorted "
                     "by value" + (f", horizontal for {why}." if horizontal
                                   else "."),
                     x=c["key"], y=keys(measures[:MAX_SERIES]), sort="-y")

    # two measures and nothing to group by
    if len(measures) >= 2:
        a, b = measures[0], measures[1]
        return _pick("scatter", f"{b['label']} against {a['label']}, one "
                     "point per row: a scatter shows the relationship.",
                     x=a["key"], y=[b["key"]])
    return _pick("table", "One measure and nothing to put it against: "
                 "a table.")


def choose_for_series(series: list[dict[str, Any]],
                      intent: str = "") -> dict[str, Any]:
    """The heuristic for a chart spec the model wrote without a kind:
    the series' shared x labels become the dimension, each series a
    measure."""
    rows: dict[str, dict[str, Any]] = {}
    names: list[str] = []
    for i, entry in enumerate(series or []):
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or f"series {i + 1}")
        names.append(name)
        for p in entry.get("points") or []:
            if isinstance(p, (list, tuple)) and len(p) == 2:
                rows.setdefault(str(p[0]), {"x": str(p[0])})[name] = p[1]
    return choose_visual(list(rows.values()), ["x", *names], intent)


# ═══ the validator ══════════════════════════════════════════════

def _check_provenance(spec: dict[str, Any],
                      problems: list[dict[str, str]]) -> dict[str, Any]:
    prov = spec.get("provenance")
    if not isinstance(prov, dict):
        problems.append(_problem(
            "provenance_missing",
            "this artifact shows numbers but carries no provenance",
            "add \"provenance\": {\"status\": certified|pending|"
            "composed|exploratory, \"meridian_line\": \"<the "
            "one-sentence disclosure>\"} — get_definition_line "
            "writes it for a governed metric"))
        return {}
    status = str(prov.get("status", "")).lower()
    if status not in STATUSES:
        problems.append(_problem(
            "status_unknown",
            f"provenance.status {prov.get('status')!r} is not one of "
            + ", ".join(STATUSES),
            "say what the number IS: certified (on the meridian), "
            "pending (governed but unreviewed), composed (you built "
            "it from parts), exploratory (a look, not a claim)"))
    if not str(prov.get("meridian_line", "")).strip():
        problems.append(_problem(
            "meridian_line_missing",
            "no meridian line: the reader cannot see whose "
            "definition this is",
            "one sentence: which definition, whose authority, on the "
            "meridian or off it — get_definition_line returns it"))
    return prov


def _chart_extras(spec: dict[str, Any], out: dict[str, Any],
                  problems: list[dict[str, str]]) -> None:
    """The keys every chart kind may carry: labels, unit, format, a
    reference line, the sort, the reason the kind was chosen."""
    out["x_label"] = str(spec.get("x_label", ""))[:80]
    out["y_label"] = str(spec.get("y_label", ""))[:80]
    out["unit"] = str(spec.get("unit", ""))[:12]
    for key in ("format", "x_format"):
        if spec.get(key) is not None:
            clean = _clean_format(spec[key])
            if not clean:
                problems.append(_problem(
                    "format_unknown", f"{key} {spec[key]!r} names no "
                    "known format",
                    "a kind word (" + " | ".join(FORMAT_KINDS)
                    + ") or {kind?, decimals?, unit?, compact?}"))
            else:
                out[key] = clean
    ref = spec.get("reference")
    if isinstance(ref, dict) and _numeric(ref.get("value")):
        out["reference"] = {"value": ref["value"],
                            "label": str(ref.get("label", ""))[:60]}
    elif ref is not None:
        problems.append(_problem(
            "reference_line", "reference must be {value: <number>, "
            "label?}", "a target, a mean, a threshold: one number and "
            "its name"))
    sort = str(spec.get("sort", "")).lower()
    if sort:
        if sort in SORTS:
            out["sort"] = sort
        else:
            problems.append(_problem(
                "sort_unknown", f"sort {spec['sort']!r} is not one of "
                + ", ".join(SORTS), "x keeps label order, -y puts the "
                "largest first"))
    if str(spec.get("reason", "")).strip():
        out["reason"] = str(spec["reason"]).strip()[:200]


def _clean_series(spec: dict[str, Any], kind: str,
                  problems: list[dict[str, str]]) -> list[dict[str, Any]]:
    series = spec.get("series")
    clean_series: list[dict[str, Any]] = []
    if not isinstance(series, list) or not series:
        problems.append(_problem(
            "chart_series", "a chart needs a non-empty series "
            "list", "series: [{\"name\": …, \"points\": "
            "[[x, y], …]}] with numeric y"))
        return clean_series
    cap = MAX_SCATTER_POINTS if kind == "scatter" else MAX_POINTS
    for i, entry in enumerate(series):
        points = (entry or {}).get("points") \
            if isinstance(entry, dict) else None
        # a point is [x, y]: y numeric, or null for a gap (a
        # forecast series is null where the actuals are, and
        # the other way round — both ride ONE shared x axis)
        if (not isinstance(points, list) or not points
                or not all(isinstance(p, (list, tuple))
                           and len(p) == 2
                           and (p[1] is None or _numeric(p[1]))
                           for p in points)
                or not any(_numeric(p[1]) for p in points)):
            problems.append(_problem(
                "chart_points",
                f"series[{i}] has no usable points",
                "each point is [x, y] with numeric y (null for "
                "a gap; at least one number); x is a label or "
                "a date string, shared across the series"))
            continue
        if len(points) > cap:
            problems.append(_problem(
                "chart_points_many",
                f"series[{i}] has {len(points)} points",
                f"at most {cap}: aggregate to a coarser period or "
                "keep the top of the list"))
            continue
        if kind == "scatter" and not all(_numeric(p[0]) for p in points):
            problems.append(_problem(
                "scatter_x", f"series[{i}] has a non-numeric x",
                "a scatter puts one measure against another: x and "
                "y are both numbers, one point per entity"))
            continue
        clean: dict[str, Any] = {
            "name": str(entry.get("name", f"series {i + 1}")),
            "points": [[p[0], p[1]] for p in points]}
        if entry.get("dashed") is True:
            clean["dashed"] = True       # a forecast, a target
        if kind == "combo":
            role = str(entry.get("role", "")).lower()
            clean["role"] = role if role in ("bar", "line") \
                else ("bar" if not clean_series else "line")
        if entry.get("unit") is not None:
            clean["unit"] = str(entry["unit"])[:12]
        if entry.get("format") is not None:
            fmt = _clean_format(entry["format"])
            if fmt:
                clean["format"] = fmt
        clean_series.append(clean)
    return clean_series


def _bin_values(values: list[float], bins: int) -> list[list[Any]]:
    """Equal-width bins over the range, labelled "lo–hi" in the
    values' own decimals; the last bin closes on the max."""
    lo, hi = min(values), max(values)
    if hi == lo:
        return [[format_value(lo), len(values)]]
    width = (hi - lo) / bins
    counts = [0] * bins
    for v in values:
        idx = min(bins - 1, int((v - lo) / width))
        counts[idx] += 1
    fmt = number_format([lo + width * i for i in range(bins + 1)])
    fmt["compact"] = fmt["compact"] and hi - lo >= 1000
    labels = [f"{format_value(lo + width * i, fmt)}–"
              f"{format_value(lo + width * (i + 1), fmt)}"
              for i in range(bins)]
    return [[label, n] for label, n in zip(labels, counts)]


def _validate_chart(spec: dict[str, Any], out: dict[str, Any],
                    problems: list[dict[str, str]]) -> None:
    raw_kind = str(spec.get("kind", "")).lower().strip()
    kind = CHART_KIND_ALIASES.get(raw_kind, raw_kind)
    if kind not in CHART_KINDS:
        problems.append(_problem(
            "chart_kind", f"chart kind {spec.get('kind')!r} is "
            "not one of " + ", ".join(CHART_KINDS),
            "pick the shape that answers the question: line for "
            "change over time, bar for comparison — or leave kind "
            "out and the heuristic picks one and says why"))
        return
    out["kind"] = kind
    if kind == "heatmap":
        xs = spec.get("x")
        ys = spec.get("y")
        values = spec.get("values")
        ok = (isinstance(xs, list) and xs and isinstance(ys, list) and ys
              and isinstance(values, list) and len(values) == len(ys)
              and all(isinstance(row, list) and len(row) == len(xs)
                      and all(v is None or _numeric(v) for v in row)
                      for row in values))
        if not ok:
            problems.append(_problem(
                "heatmap_matrix", "a heatmap needs x labels, y labels "
                "and a values matrix of len(y) rows × len(x) cells",
                "{kind: heatmap, x: [\"Mon\", …], y: [\"US\", …], "
                "values: [[3, 5, …], …]} — null for an empty cell"))
            return
        if len(xs) > MAX_HEATMAP_SIDE or len(ys) > MAX_HEATMAP_SIDE:
            problems.append(_problem(
                "heatmap_size", f"{len(ys)}×{len(xs)} cells",
                f"at most {MAX_HEATMAP_SIDE} on a side: fold the tail "
                "or aggregate"))
            return
        if not any(_numeric(v) for row in values for v in row):
            problems.append(_problem(
                "heatmap_empty", "every cell is null",
                "at least one number"))
            return
        out.update(x=[str(v) for v in xs], y=[str(v) for v in ys],
                   values=[[v for v in row] for row in values],
                   name=str(spec.get("name", ""))[:60])
        _chart_extras(spec, out, problems)
        return
    if kind == "histogram" and "values" in spec:
        raw = spec.get("values")
        nums = [float(v) for v in raw if _numeric(v)] \
            if isinstance(raw, list) else []
        if len(nums) < 2:
            problems.append(_problem(
                "histogram_values", "a histogram needs at least two "
                "numeric values (or pre-binned series)",
                "{kind: histogram, values: [12, 15, …], bins?: 10} or "
                "series: [{name, points: [[\"0–1k\", 40], …]}]"))
            return
        if len(nums) > 100_000:
            problems.append(_problem(
                "histogram_values", f"{len(nums)} values",
                "at most 100,000: bin them yourself and pass the "
                "series"))
            return
        bins = spec.get("bins")
        bins = int(bins) if _numeric(bins) and 2 <= bins <= 60 \
            else min(20, max(5, int(math.sqrt(len(nums)))))
        out.update(series=[{"name": str(spec.get("name") or "count"),
                            "points": _bin_values(nums, bins)}],
                   bins=bins, binned=True)
        _chart_extras(spec, out, problems)
        return
    clean_series = _clean_series(spec, kind, problems)
    limit = MAX_FACETS if kind == "small_multiples" else MAX_SERIES
    if len(clean_series) > limit:
        problems.append(_problem(
            "chart_series_many",
            f"{len(clean_series)} series on one chart",
            f"at most {limit}: fold the tail into Other, or split "
            "into one chart per group"
            + ("" if kind == "small_multiples"
               else " (small_multiples takes up to 24)")))
    if kind == "waterfall" and len(clean_series) > 1:
        problems.append(_problem(
            "waterfall_series", f"{len(clean_series)} series on a "
            "waterfall", "one series of steps; name the totals in "
            "totals: [\"Start\", \"End\"]"))
    if kind == "combo" and clean_series and not (
            any(s.get("role") == "bar" for s in clean_series)
            and any(s.get("role") == "line" for s in clean_series)):
        problems.append(_problem(
            "combo_roles", "a combo needs at least one bar series "
            "and one line series", "role: \"bar\" | \"line\" on each "
            "series, the same unit on both"))
    out["series"] = clean_series
    if kind == "waterfall":
        totals = spec.get("totals")
        out["totals"] = [str(t) for t in totals][:10] \
            if isinstance(totals, list) else []
    if kind == "histogram":
        out["binned"] = True
    _chart_extras(spec, out, problems)


def validate_artifact(type: str, spec: Any, *,
                      build_id: str = "",
                      facts: frozenset[str] = frozenset(),
                      build: Any = None
                      ) -> tuple[dict[str, Any] | None,
                                 list[dict[str, str]]]:
    """→ (normalized spec, []) or (None, teaching problems)."""
    problems: list[dict[str, str]] = []
    if type not in TYPES:
        return None, [_problem(
            "unknown_type", f"unknown artifact type {type!r}",
            "types: " + " | ".join(TYPES))]
    if not isinstance(spec, dict):
        return None, [_problem(
            "bad_spec", "the spec must be an object",
            "see the artifact tool description for each type's shape")]

    out: dict[str, Any] = {"build_id": build_id}
    shows_numbers = False

    if type == "chart":
        _validate_chart(spec, out, problems)
        shows_numbers = True

    elif type == "table":
        columns = spec.get("columns")
        rows = spec.get("rows")
        if not isinstance(columns, list) or not columns or not all(
                isinstance(c, dict) and c.get("key") for c in columns):
            problems.append(_problem(
                "table_columns", "columns must be a non-empty list "
                "of {key, label}", "e.g. [{\"key\": \"day\", "
                "\"label\": \"Day\"}, …]"))
            columns = []
        if not isinstance(rows, list):
            problems.append(_problem(
                "table_rows", "rows must be a list of objects keyed "
                "by column key", "e.g. [{\"day\": \"2026-08-01\", "
                "\"spend\": 1200}]"))
            rows = []
        keys = [c["key"] for c in columns]
        clean_rows = [{k: (r or {}).get(k) for k in keys}
                      for r in rows if isinstance(r, dict)]
        shows_numbers = any(_numeric(v) for r in clean_rows
                            for v in r.values())
        clean_columns = []
        for c in columns:
            col = {"key": c["key"], "label": str(c.get("label", c["key"]))}
            if c.get("status"):
                col["status"] = c["status"]
            if c.get("unit"):
                col["unit"] = str(c["unit"])[:12]
            if c.get("format") is not None:
                fmt = _clean_format(c["format"])
                if fmt:
                    col["format"] = fmt
                else:
                    problems.append(_problem(
                        "format_unknown", f"column {c['key']!r} format "
                        f"{c['format']!r} names no known format",
                        "a kind word (" + " | ".join(FORMAT_KINDS)
                        + ") or {kind?, decimals?, unit?, compact?}"))
            clean_columns.append(col)
        out.update(columns=clean_columns, rows=clean_rows)

    elif type == "document":
        markdown = spec.get("markdown")
        if not isinstance(markdown, str) or not markdown.strip():
            problems.append(_problem(
                "document_markdown", "a document needs markdown text",
                "spec: {\"markdown\": \"…\"} — headings, lists, and "
                "tables render; numbers in prose still need the "
                "provenance footer"))
        out["markdown"] = markdown if isinstance(markdown, str) else ""
        # prose numbers cannot be schema-detected; a document is
        # exploratory unless provenance is declared
        shows_numbers = bool(spec.get("provenance"))

    elif type == "kpi":
        value = spec.get("value")
        if not _numeric(value):
            problems.append(_problem(
                "kpi_value", "a kpi needs a numeric value",
                "spec: {\"value\": <number>, \"unit\"?, \"label\"?, "
                "\"delta\"?, \"format\"?} plus provenance {status, "
                "meridian_line}"))
        shows_numbers = True
        out.update(value=value if _numeric(value) else None,
                   unit=str(spec.get("unit", ""))[:12],
                   label=str(spec.get("label", "")),
                   **({"delta": spec["delta"]}
                      if _numeric(spec.get("delta")) else {}),
                   **({"compare_label": str(spec["compare_label"])[:60]}
                      if spec.get("compare_label") else {}))
        # the value's format, and the delta's own when it is not in
        # the value's unit (a change of -4.2% under a dollar figure)
        for key in ("format", "delta_format"):
            if spec.get(key) is None:
                continue
            fmt = _clean_format(spec[key])
            if fmt:
                out[key] = fmt
            else:
                problems.append(_problem(
                    "format_unknown", f"{key} {spec[key]!r} names "
                    "no known format",
                    "a kind word (" + " | ".join(FORMAT_KINDS)
                    + ") or {kind?, decimals?, unit?, compact?}"))

    elif type == "dashboard":
        panels = spec.get("panels")
        clean_panels: list[dict[str, Any]] = []
        marked = False
        grid = spec.get("grid", spec.get("columns"))
        grid_n = int(grid) if _numeric(grid) and 1 <= grid <= 3 else 0
        if grid is not None and not grid_n:
            problems.append(_problem(
                "dashboard_grid", f"grid {grid!r} is not 1, 2 or 3",
                "grid: the number of columns, 1 to 3; each panel may "
                "carry span: 1..grid"))
        if not isinstance(panels, list) or not panels:
            problems.append(_problem(
                "dashboard_panels",
                "a dashboard needs a non-empty panels list",
                "panels: [{\"type\": kpi|chart|table|document, "
                "\"title\"?, \"span\"?, \"spec\": {…}}] — each numeric "
                "panel carries its OWN provenance"))
        else:
            for i, panel in enumerate(panels):
                ptype = str(panel.get("type", "")) \
                    if isinstance(panel, dict) else ""
                if ptype not in PANEL_TYPES:
                    problems.append(_problem(
                        "panel_type",
                        f"panels[{i}] type {ptype!r} is not one of "
                        + ", ".join(PANEL_TYPES),
                        "a dashboard nests tiles, never another "
                        "dashboard"))
                    continue
                sub_spec = panel.get("spec")
                sub, sub_problems = validate_artifact(
                    ptype,
                    sub_spec if isinstance(sub_spec, dict) else {},
                    build_id=build_id, facts=facts, build=build)
                if sub_problems:
                    problems.extend(_problem(
                        p["code"],
                        f"panels[{i}] ({ptype}): {p['detail']}",
                        p["hint"]) for p in sub_problems)
                    continue
                clean_panel = {
                    "type": ptype,
                    "title": str(panel.get("title", ""))[:120],
                    "spec": sub}
                span = panel.get("span")
                if _numeric(span) and 1 <= span <= 3:
                    clean_panel["span"] = min(int(span), grid_n or 3)
                clean_panels.append(clean_panel)
                marked = marked or bool(sub.get("watermark"))
        filters: list[dict[str, Any]] = []
        for j, item in enumerate(spec.get("filters") or []):
            slot = str((item or {}).get("slot", "")).strip() \
                if isinstance(item, dict) else ""
            options = (item or {}).get("options") \
                if isinstance(item, dict) else None
            if not slot or not isinstance(options, list) \
                    or not options:
                problems.append(_problem(
                    "dashboard_filter",
                    f"filters[{j}] needs a slot and options",
                    "filters: [{\"slot\": \"country\", \"options\": "
                    "[\"US\", \"CA\"], \"active\"?}] — picking one "
                    "sends a whatif request through the conversation, "
                    "never a hidden query"))
                continue
            options = [str(o) for o in options][:12]
            active = str(item.get("active", options[0]))
            filters.append({
                "slot": slot, "label": str(item.get("label", slot)),
                "options": options,
                "active": active if active in options
                else options[0]})
        out.update(panels=clean_panels, filters=filters,
                   notes=str(spec.get("notes", ""))[:2000])
        if grid_n:
            out["grid"] = grid_n
        if marked:
            out["watermark"] = "EXPLORATORY"
        shows_numbers = False        # the tiles carry the disclosure

    elif type == "diagram":
        kind = str(spec.get("kind", "")).lower()
        if kind not in DIAGRAM_KINDS:
            problems.append(_problem(
                "diagram_kind", f"diagram kind {spec.get('kind')!r} "
                "is not one of " + ", ".join(DIAGRAM_KINDS),
                "graph: {nodes, edges} as subgraph()/constellation "
                "return them; mermaid: {source} for lineage, "
                "funnels, decision trees"))
        if kind == "mermaid":
            source = spec.get("source")
            if not isinstance(source, str) or not source.strip():
                problems.append(_problem(
                    "diagram_source",
                    "a mermaid diagram needs source text",
                    "spec: {\"kind\": \"mermaid\", \"source\": "
                    "\"flowchart TD; …\"}"))
            out.update(kind="mermaid",
                       source=(source if isinstance(source, str)
                               else "")[:6000])
        elif kind == "graph":
            nodes = spec.get("nodes")
            clean_nodes: list[dict[str, Any]] = []
            if not isinstance(nodes, list) or not nodes:
                problems.append(_problem(
                    "diagram_nodes",
                    "a graph diagram needs a non-empty nodes list",
                    "nodes: [{\"id\", \"kind\"?, \"label\"?, "
                    "\"status\"?}] — subgraph() returns exactly this "
                    "shape"))
            else:
                for n in nodes:
                    if isinstance(n, dict) and n.get("id"):
                        clean_nodes.append({
                            "id": str(n["id"]),
                            "kind": str(n.get("kind", "")),
                            "label": str(n.get("label")
                                         or n["id"])[:80],
                            **({"status": str(n["status"])}
                               if n.get("status") else {})})
            ids = {n["id"] for n in clean_nodes}
            clean_edges: list[dict[str, Any]] = []
            for j, e in enumerate(spec.get("edges") or []):
                a, b = str((e or {}).get("a", "")), \
                    str((e or {}).get("b", ""))
                if a not in ids or b not in ids:
                    problems.append(_problem(
                        "diagram_edges",
                        f"edges[{j}] joins {a!r}–{b!r} but both ends "
                        "must be node ids",
                        "every edge endpoint must appear in nodes"))
                    continue
                clean_edges.append({
                    "a": a, "b": b, "rel": str(e.get("rel", "")),
                    **({"tier": str(e["tier"])}
                       if e.get("tier") else {})})
            out.update(kind="graph", nodes=clean_nodes,
                       edges=clean_edges)
        shows_numbers = bool(spec.get("provenance"))

    # ── the rendering rules ──────────────────────────────────
    if shows_numbers or spec.get("provenance") is not None:
        prov = _check_provenance(spec, problems)
        # rule 1 with teeth: "certified" is a claim about the BUILD,
        # not a vibe — it must name the metric, and the metric must
        # actually be certified there
        if str((prov or {}).get("status", "")).lower() == "certified":
            metric_id = (prov or {}).get("metric_id", "")
            if not metric_id:
                problems.append(_problem(
                    "certified_needs_metric",
                    "status certified without a metric_id",
                    "name the governed metric in provenance."
                    "metric_id (from search_semantics or resolve), "
                    "or use status composed/exploratory"))
            elif build is not None:
                row = next((m for m in getattr(build, "metrics", [])
                            if m.get("id") == metric_id), None)
                served = (row or {}).get("status_served") \
                    or (row or {}).get("status")
                if row is None or served != "certified":
                    problems.append(_problem(
                        "status_overclaimed",
                        f"{metric_id} is "
                        + ("not in this build" if row is None
                           else f"{served}, not certified"),
                        "say what it IS — the reader decides what "
                        "certified means, not the chart"))
        if prov:
            out["provenance"] = {
                "status": str(prov.get("status", "")).lower(),
                "meridian_line": str(prov.get("meridian_line", "")),
                "facts": [str(f) for f in (prov.get("facts") or [])],
                **({"metric_id": prov["metric_id"]}
                   if prov.get("metric_id") else {}),
                **({"source_sql": str(prov["source_sql"])[:2000]}
                   if prov.get("source_sql") else {}),
            }
    elif type == "document":
        out["watermark"] = "EXPLORATORY"

    if problems:
        return None, problems

    # Rule 2: composed/exploratory keep the watermark unless a cited
    # check fact from this trajectory stands behind the number
    prov = out.get("provenance")
    if prov:
        cited = set(prov.get("facts", [])) & set(facts)
        if prov["status"] in SELF_STANDING:
            pass
        elif prov["status"] == "composed" and cited:
            prov["facts_verified"] = sorted(cited)
        else:
            out["watermark"] = "EXPLORATORY"
    if str(spec.get("caption", "")).strip():
        out["caption"] = str(spec["caption"])[:400]
    return out, []


# ═══ one valid example per chart kind ═══════════════════════════
# The docs print these, the tests validate and render every one, and
# the tool description points the model at the shapes.

_PROV = {"status": "exploratory", "meridian_line": "a look, not a claim"}
_MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05",
           "2026-06"]
EXAMPLES: dict[str, dict[str, Any]] = {
    "line": {"kind": "line", "unit": "USD", "x_label": "Month",
             "series": [{"name": "Actuals", "points": [
                 [m, v] for m, v in zip(_MONTHS, [802000, 850000, 900000,
                                                  None, None, None])]},
                        {"name": "Forecast", "dashed": True, "points": [
                            ["2026-03", 900000], ["2026-04", 930000],
                            ["2026-05", 960000], ["2026-06", 1020000]]}],
             "reference": {"value": 950000, "label": "Plan"},
             "reason": "Spend over months: a line reads change over "
                       "time.", "provenance": _PROV},
    "area": {"kind": "area", "unit": "count", "series": [
        {"name": "Sessions", "points": [[m, v] for m, v in zip(
            _MONTHS, [1200, 1350, 1500, 1480, 1700, 1900])]}],
        "provenance": _PROV},
    "bar": {"kind": "bar", "unit": "USD", "sort": "-y", "series": [
        {"name": "Spend", "points": [["US", 512000], ["CA", 300000],
                                     ["MX", 120500], ["BR", 98000]]}],
        "provenance": _PROV},
    "hbar": {"kind": "hbar", "unit": "count", "sort": "-y", "series": [
        {"name": "Merchants", "points": [
            ["Grocery and supermarkets", 1200],
            ["Restaurants and bars", 980], ["Fuel and service", 640],
            ["Travel and lodging", 410], ["Department stores", 380],
            ["Pharmacies", 220], ["Electronics", 180],
            ["Home improvement", 150], ["Other", 900]]}],
        "provenance": _PROV},
    "stacked_bar": {"kind": "stacked_bar", "unit": "USD", "series": [
        {"name": "Card present", "points": [
            ["Q1", 300], ["Q2", 320], ["Q3", 350], ["Q4", 400]]},
        {"name": "Card not present", "points": [
            ["Q1", 200], ["Q2", 240], ["Q3", 260], ["Q4", 310]]}],
        "provenance": _PROV},
    "percent_bar": {"kind": "percent_bar", "series": [
        {"name": "Approved", "points": [["US", 91], ["CA", 88]]},
        {"name": "Declined", "points": [["US", 7], ["CA", 9]]},
        {"name": "Errored", "points": [["US", 2], ["CA", 3]]}],
        "provenance": _PROV},
    "scatter": {"kind": "scatter", "x_label": "Transactions",
                "y_label": "Spend", "format": "currency", "series": [
                    {"name": "Merchants", "points": [
                        [120, 4800.5], [340, 12100], [90, 3900],
                        [510, 20050], [220, 8800]]}],
                "provenance": _PROV},
    "histogram": {"kind": "histogram", "name": "Orders", "bins": 5,
                  "values": [12, 15, 15.5, 18, 22, 23, 25, 31, 33, 35,
                             40, 41, 45, 52, 60, 61, 75, 80, 90, 120],
                  "provenance": _PROV},
    "heatmap": {"kind": "heatmap", "name": "Transactions",
                "x": ["Mon", "Tue", "Wed", "Thu", "Fri"],
                "y": ["US", "CA", "MX"],
                "values": [[120, 130, 125, 140, 200],
                           [40, 42, 45, 50, 70], [10, 12, None, 15, 22]],
                "provenance": _PROV},
    "small_multiples": {"kind": "small_multiples", "unit": "USD",
                        "series": [{"name": n, "points": [
                            [m, (i + 1) * 100 + j * 15]
                            for j, m in enumerate(_MONTHS)]}
                            for i, n in enumerate(
                                ["US", "CA", "MX", "BR", "AR", "CL",
                                 "PE", "CO"])],
                        "provenance": _PROV},
    "combo": {"kind": "combo", "unit": "USD", "series": [
        {"name": "Actual", "role": "bar", "points": [
            [m, v] for m, v in zip(_MONTHS, [80, 85, 90, 88, 95, 99])]},
        {"name": "Plan", "role": "line", "dashed": True, "points": [
            [m, v] for m, v in zip(_MONTHS, [82, 84, 86, 88, 90, 92])]}],
        "provenance": _PROV},
    "waterfall": {"kind": "waterfall", "unit": "USD",
                  "totals": ["Start", "End"], "series": [
                      {"name": "Bridge", "points": [
                          ["Start", 1000], ["New", 250], ["Churn", -120],
                          ["Price", 80], ["FX", -40], ["End", 1170]]}],
                  "provenance": _PROV},
}
