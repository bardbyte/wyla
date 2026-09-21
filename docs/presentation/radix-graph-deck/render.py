"""Render a .pptx to HTML by reading the file back with python-pptx.

Not a re-run of the generator's intentions: every box, fill, line and run is
read out of the written file, so overflow and overlap show up honestly.
Arial maps to Liberation Sans here, which is metric-compatible, so text-fit
in this preview matches PowerPoint's.
"""
import base64, html, sys
from pptx import Presentation
from pptx.util import Emu

EMU_IN = 914400.0
PX = 96.0  # px per inch


def px(v):
    return (v or 0) / EMU_IN * PX


def rgb_of(color):
    try:
        if color is None or color.type is None:
            return None
        return "#" + str(color.rgb)
    except Exception:
        return None


def fill_css(sh):
    try:
        f = sh.fill
        if f.type is None or f.type == 5:  # none / inherit
            return "transparent"
        c = rgb_of(f.fore_color)
        if not c:
            return "transparent"
        try:
            alpha = f.fore_color._xFill.find(
                ".//{http://schemas.openxmlformats.org/drawingml/2006/main}alpha")
        except Exception:
            alpha = None
        if alpha is not None:
            a = int(alpha.get("val")) / 100000.0
            r, g, b = int(c[1:3], 16), int(c[3:5], 16), int(c[5:7], 16)
            return f"rgba({r},{g},{b},{a:.3f})"
        return c
    except Exception:
        return "transparent"


def line_css(sh):
    try:
        ln = sh.line
        c = rgb_of(ln.color)
        w = ln.width
        if not c or not w:
            return ""
        return f"border:{px(w):.2f}px solid {c};"
    except Exception:
        return ""


def run_html(r):
    f = r.font
    styles = []
    if f.size:
        styles.append(f"font-size:{f.size.pt * PX / 72:.2f}px")
    if f.bold:
        styles.append("font-weight:700")
    if f.italic:
        styles.append("font-style:italic")
    c = rgb_of(f.color)
    if c:
        styles.append(f"color:{c}")
    if f.name:
        styles.append(f"font-family:'{f.name}',Liberation Sans,sans-serif")
    try:
        cs = r.font._rPr.get("spc")
        if cs:
            styles.append(f"letter-spacing:{int(cs) / 100 * PX / 72:.2f}px")
    except Exception:
        pass
    return f'<span style="{";".join(styles)}">{html.escape(r.text)}</span>'


def render(path, out):
    prs = Presentation(path)
    W, H = px(prs.slide_width), px(prs.slide_height)
    parts = [
        "<!doctype html><meta charset='utf-8'>",
        "<style>body{margin:0;background:#8a8a8a;font-family:'Liberation Sans',Arial,sans-serif}"
        f".slide{{position:relative;width:{W}px;height:{H}px;overflow:hidden;"
        "background:#fff;margin:0 auto 18px;box-shadow:0 2px 12px rgba(0,0,0,.4)}"
        ".sh{position:absolute;box-sizing:border-box}"
        ".tx{position:absolute;box-sizing:border-box;white-space:pre-wrap;line-height:1.22}"
        "</style>",
    ]
    for si, slide in enumerate(prs.slides):
        bg = "#FFFFFF"
        try:
            el = slide.background.fill
            if el.type == 1:
                bg = rgb_of(el.fore_color) or bg
        except Exception:
            pass
        parts.append(f"<div class='slide' style='background:{bg}'>")
        for sh in slide.shapes:
            L, T, Wd, Ht = px(sh.left), px(sh.top), px(sh.width), px(sh.height)
            if sh.shape_type is not None and sh.shape_type == 13:  # picture
                blob = sh.image.blob
                b64 = base64.b64encode(blob).decode()
                # honour <a:alphaModFix> so faint background art previews faintly
                op = ""
                try:
                    amf = sh._element.find(
                        ".//{http://schemas.openxmlformats.org/drawingml/2006/main}alphaModFix")
                    if amf is not None and amf.get("amt"):
                        op = f"opacity:{int(amf.get('amt')) / 100000:.3f};"
                except Exception:
                    pass
                parts.append(
                    f"<img class='sh' style='left:{L:.2f}px;top:{T:.2f}px;"
                    f"width:{Wd:.2f}px;height:{Ht:.2f}px;{op}' src='data:image/png;base64,{b64}'>")
                continue
            f_css = fill_css(sh)
            l_css = line_css(sh)
            radius = ""
            try:
                if "ROUNDED" in str(sh.shape_type) or sh.auto_shape_type is not None and \
                        "ROUNDED" in str(sh.auto_shape_type):
                    radius = "border-radius:7px;"
            except Exception:
                pass
            if Ht < 2 and l_css:  # a line shape
                parts.append(
                    f"<div class='sh' style='left:{L:.2f}px;top:{T:.2f}px;width:{Wd:.2f}px;"
                    f"height:0;border-top:{l_css.split('solid')[0].split(':')[1]} solid "
                    f"{l_css.split('solid')[1].strip(' ;')}'></div>")
                continue
            if f_css != "transparent" or l_css:
                parts.append(
                    f"<div class='sh' style='left:{L:.2f}px;top:{T:.2f}px;width:{Wd:.2f}px;"
                    f"height:{Ht:.2f}px;background:{f_css};{l_css}{radius}'></div>")
            if sh.has_text_frame and sh.text_frame.text.strip():
                tf = sh.text_frame
                valign = str(tf.vertical_anchor or "")
                just = "flex-start"
                if "MIDDLE" in valign:
                    just = "center"
                elif "BOTTOM" in valign:
                    just = "flex-end"
                inner = []
                for p in tf.paragraphs:
                    runs = "".join(run_html(r) for r in p.runs) or "&nbsp;"
                    sa = ""
                    try:
                        spc = p._pPr.find(
                            "{http://schemas.openxmlformats.org/drawingml/2006/main}spcAft")
                        if spc is not None:
                            pts = spc.find(
                                "{http://schemas.openxmlformats.org/drawingml/2006/main}spcPts")
                            if pts is not None:
                                sa = f"margin-bottom:{int(pts.get('val'))/100*PX/72:.2f}px;"
                    except Exception:
                        pass
                    align = ""
                    if p.alignment is not None and "CENTER" in str(p.alignment):
                        align = "text-align:center;"
                    inner.append(f"<div style='{sa}{align}'>{runs}</div>")
                parts.append(
                    f"<div class='tx' style='left:{L:.2f}px;top:{T:.2f}px;width:{Wd:.2f}px;"
                    f"height:{Ht:.2f}px;display:flex;flex-direction:column;"
                    f"justify-content:{just}'><div>{''.join(inner)}</div></div>")
        parts.append("</div>")
    open(out, "w").write("\n".join(parts))
    print("wrote", out, f"({len(prs.slides)} slides, {W:.0f}x{H:.0f}px)")


if __name__ == "__main__":
    render(sys.argv[1], sys.argv[2])
