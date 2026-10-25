"""
Builds docs/phase3_report.pdf (Phase 3 architecture and FinOps report).

    pip install reportlab matplotlib
    python docs/phase3_figures.py      # architecture, cost chart, DAG placeholder
    python docs/phase3_report.py       # the PDF

The Spark UI / Query Profile screenshot goes in docs/figures/spark_ui_dag.png. If that file is
missing, the PDF shows a marked placeholder box instead. See docs/SCREENSHOT-STEPS.md.
"""
import datetime as dt
import os
import re
import sys

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (BaseDocTemplate, CondPageBreak, Frame, Image, KeepTogether,
                                NextPageTemplate, PageBreak, PageTemplate, Paragraph, Spacer, Table,
                                TableStyle, XPreformatted)
from reportlab.platypus.tableofcontents import TableOfContents

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import phase3_content as C  # noqa: E402

FIG = os.path.join(HERE, "figures")
OUT = os.path.join(HERE, "phase3_report.pdf")

PAGE_W, PAGE_H = A4
MARGIN_X, MARGIN_TOP, MARGIN_BOTTOM = 2.2 * cm, 2.3 * cm, 2.2 * cm
TEXT_W = PAGE_W - 2 * MARGIN_X

INK = colors.HexColor("#1f2933")
MUTED = colors.HexColor("#52606d")
ACCENT = colors.HexColor("#1d4e89")
RULE = colors.HexColor("#c9d2dc")
HEAD_BG = colors.HexColor("#e8eef5")
ZEBRA = colors.HexColor("#f7f9fb")
CODE_BG = colors.HexColor("#f4f5f7")

BODY_FONT, BOLD_FONT, ITALIC_FONT = "Times-Roman", "Times-Bold", "Times-Italic"
SANS, SANS_BOLD, MONO = "Helvetica", "Helvetica-Bold", "Courier"

ST = {
    "body": ParagraphStyle("body", fontName=BODY_FONT, fontSize=10.4, leading=13.7, textColor=INK,
                           alignment=TA_JUSTIFY, spaceAfter=5.5),
    "bullet": ParagraphStyle("bullet", fontName=BODY_FONT, fontSize=10.6, leading=14.0, textColor=INK,
                             leftIndent=14, bulletIndent=3, spaceAfter=2.5, alignment=TA_LEFT),
    "h1": ParagraphStyle("h1", fontName=SANS_BOLD, fontSize=15, leading=19, textColor=ACCENT,
                         spaceBefore=10, spaceAfter=7),
    "h2": ParagraphStyle("h2", fontName=SANS_BOLD, fontSize=11.6, leading=15, textColor=INK,
                         spaceBefore=9, spaceAfter=4),
    "caption": ParagraphStyle("caption", fontName=SANS, fontSize=8.6, leading=11, textColor=MUTED,
                              alignment=TA_CENTER, spaceBefore=3, spaceAfter=10),
    "tcaption": ParagraphStyle("tcaption", fontName=SANS, fontSize=8.6, leading=11, textColor=MUTED,
                               alignment=TA_LEFT, spaceBefore=4, spaceAfter=3),
    "cell": ParagraphStyle("cell", fontName=SANS, fontSize=7.9, leading=9.8, textColor=INK),
    "cellb": ParagraphStyle("cellb", fontName=SANS_BOLD, fontSize=7.9, leading=9.8, textColor=INK),
    "code": ParagraphStyle("code", fontName=MONO, fontSize=7.0, leading=8.6, textColor=INK),
    "note": ParagraphStyle("note", fontName=SANS, fontSize=8.4, leading=11, textColor=MUTED, spaceAfter=6),
    "toc1": ParagraphStyle("toc1", fontName=SANS_BOLD, fontSize=10.5, leading=16, leftIndent=0),
    "toc2": ParagraphStyle("toc2", fontName=SANS, fontSize=9.8, leading=13.5, leftIndent=18),
}


# ------------------------------------------------------------------ numbering helpers
class Counter:
    def __init__(self):
        self.h1 = 0
        self.h2 = 0
        self.fig = 0
        self.tab = 0


N = Counter()


class Heading(Paragraph):
    """Paragraph that registers itself in the table of contents."""

    def __init__(self, text, style, level, key):
        super().__init__(text, style)
        self.toc_level = level
        self.toc_text = re.sub("<[^>]+>", "", text)
        self.toc_key = key


def h1(title):
    N.h1 += 1
    N.h2 = 0
    text = f"{N.h1}&nbsp;&nbsp;{title}"
    return [CondPageBreak(5 * cm), Heading(text, ST["h1"], 0, f"s{N.h1}")]


def h2(title):
    N.h2 += 1
    text = f"{N.h1}.{N.h2}&nbsp;&nbsp;{title}"
    return [CondPageBreak(3.5 * cm), Heading(text, ST["h2"], 1, f"s{N.h1}_{N.h2}")]


def p(text):
    return Paragraph(text, ST["body"])


def bullets(items):
    return [Paragraph(t, ST["bullet"], bulletText="•") for t in items]


def code(text, max_chars=118):
    lines = []
    for line in text.rstrip("\n").splitlines():
        while len(line) > max_chars:
            lines.append(line[:max_chars])
            line = "    " + line[max_chars:]
        lines.append(line)
    esc = "\n".join(lines).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    t = Table([[XPreformatted(esc, ST["code"])]], colWidths=[TEXT_W])
    t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), CODE_BG),
                           ("BOX", (0, 0), (-1, -1), 0.5, RULE),
                           ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                           ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
    return t


def figure(path, caption, width=TEXT_W, max_h=None):
    N.fig += 1
    from reportlab.lib.utils import ImageReader
    iw, ih = ImageReader(path).getSize()
    w = width
    h = w * ih / iw
    if max_h and h > max_h:
        h = max_h
        w = h * iw / ih
    return KeepTogether([Image(path, width=w, height=h),
                         Paragraph(f"<b>Figure {N.fig}.</b> {caption}", ST["caption"])])


def fig_no():
    """Number the next figure will get (for references in text written before it)."""
    return N.fig + 1


def table(rows, widths, caption, header=True, zebra=True, font_size=None):
    N.tab += 1
    total = sum(widths)
    widths = [w / total * TEXT_W for w in widths]
    data = []
    for i, r in enumerate(rows):
        style = ST["cellb"] if (header and i == 0) else ST["cell"]
        data.append([c if not isinstance(c, str) else Paragraph(c, style) for c in r])
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    cmds = [("GRID", (0, 0), (-1, -1), 0.4, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 2.6), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.6)]
    if header:
        cmds.append(("BACKGROUND", (0, 0), (-1, 0), HEAD_BG))
    if zebra:
        for i in range(1 if header else 0, len(rows)):
            if i % 2 == 0:
                cmds.append(("BACKGROUND", (0, i), (-1, i), ZEBRA))
    t.setStyle(TableStyle(cmds))
    cap = Paragraph(f"<b>Table {N.tab}.</b> {caption}", ST["tcaption"])
    if len(rows) <= 9:
        return [KeepTogether([cap, t]), Spacer(1, 8)]
    return [CondPageBreak(3 * cm), cap, t, Spacer(1, 8)]


def tab_no():
    return N.tab + 1


# ------------------------------------------------------------------ document template
class ReportDoc(BaseDocTemplate):
    def __init__(self, filename, **kw):
        super().__init__(filename, pagesize=A4, leftMargin=MARGIN_X, rightMargin=MARGIN_X,
                         topMargin=MARGIN_TOP, bottomMargin=MARGIN_BOTTOM,
                         title=C.TITLE, author=", ".join(a for a, _ in C.AUTHORS),
                         subject=C.SUBTITLE, creator="phase3_report.py", **kw)
        frame = Frame(MARGIN_X, MARGIN_BOTTOM, TEXT_W, PAGE_H - MARGIN_TOP - MARGIN_BOTTOM, id="f")
        self.addPageTemplates([PageTemplate("title", [frame], onPage=lambda c, d: None),
                               PageTemplate("body", [frame], onPage=self._decorate)])

    def _decorate(self, canv, doc):
        canv.saveState()
        canv.setStrokeColor(RULE)
        canv.setLineWidth(0.5)
        canv.line(MARGIN_X, PAGE_H - 1.5 * cm, PAGE_W - MARGIN_X, PAGE_H - 1.5 * cm)
        canv.setFont(SANS, 7.8)
        canv.setFillColor(MUTED)
        canv.drawString(MARGIN_X, PAGE_H - 1.3 * cm, C.RUNNING_HEAD)
        canv.drawRightString(PAGE_W - MARGIN_X, PAGE_H - 1.3 * cm, "Phase 3 report")
        canv.line(MARGIN_X, 1.5 * cm, PAGE_W - MARGIN_X, 1.5 * cm)
        canv.drawCentredString(PAGE_W / 2, 1.0 * cm, f"Page {doc.page}")
        canv.restoreState()

    def afterFlowable(self, flowable):
        if isinstance(flowable, Heading):
            self.canv.bookmarkPage(flowable.toc_key)
            self.canv.addOutlineEntry(flowable.toc_text, flowable.toc_key, level=flowable.toc_level)
            self.notify("TOCEntry", (flowable.toc_level, flowable.toc_text, self.page, flowable.toc_key))


def title_page():
    out = [Spacer(1, 3.2 * cm)]
    out.append(Paragraph(C.COURSE_LINE, ParagraphStyle("k", fontName=SANS, fontSize=10, textColor=MUTED,
                                                       alignment=TA_CENTER, spaceAfter=18)))
    out.append(Paragraph(C.TITLE, ParagraphStyle("t", fontName=SANS_BOLD, fontSize=23, leading=29,
                                                 textColor=ACCENT, alignment=TA_CENTER, spaceAfter=12)))
    out.append(Paragraph(C.SUBTITLE, ParagraphStyle("st", fontName=SANS, fontSize=13.5, leading=18,
                                                    textColor=INK, alignment=TA_CENTER, spaceAfter=40)))
    rows = [[Paragraph("<b>Team</b>", ST["cell"]),
             Paragraph("<br/>".join(f"{n} ({r})" for n, r in C.AUTHORS), ST["cell"])]]
    for k, v in C.TITLE_TABLE:
        rows.append([Paragraph(f"<b>{k}</b>", ST["cell"]), Paragraph(v, ST["cell"])])
    t = Table(rows, colWidths=[4.0 * cm, 10.0 * cm], hAlign="CENTER")
    t.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.4, RULE),
                           ("BACKGROUND", (0, 0), (0, -1), HEAD_BG),
                           ("VALIGN", (0, 0), (-1, -1), "TOP"),
                           ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                           ("LEFTPADDING", (0, 0), (-1, -1), 6)]))
    out.append(t)
    out.append(Spacer(1, 2.2 * cm))
    out.append(Paragraph(C.TITLE_ABSTRACT, ParagraphStyle("abs", parent=ST["body"], fontSize=10,
                                                          leading=13.6, leftIndent=1.2 * cm,
                                                          rightIndent=1.2 * cm)))
    out += [NextPageTemplate("body"), PageBreak()]
    return out


def toc_page():
    toc = TableOfContents()
    toc.levelStyles = [ST["toc1"], ST["toc2"]]
    toc.dotsMinLevel = 0
    return [Paragraph("Contents", ST["h1"]), toc, PageBreak()]


def build():
    N.__init__()
    story = title_page() + toc_page()
    ctx = {"h1": h1, "h2": h2, "p": p, "bullets": bullets, "code": code, "figure": figure,
           "table": table, "fig_no": fig_no, "tab_no": tab_no, "FIG": FIG, "TEXT_W": TEXT_W,
           "ST": ST, "Paragraph": Paragraph, "Spacer": Spacer, "PageBreak": PageBreak, "cm": cm,
           "CondPageBreak": CondPageBreak, "KeepTogether": KeepTogether}
    story += C.body(ctx)
    doc = ReportDoc(OUT)
    doc.multiBuild(story)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    build()
