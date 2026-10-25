"""
Draws the Gold ER diagram (docs/gold_er_diagram.png) from pipeline/schemas.py, so the picture
always matches the tables. Boxes show the PK and FK columns, lines are the foreign keys.

    pip install matplotlib
    python scripts/draw_gold_er.py

Only needs pyspark installed (pyspark.sql.types), no Spark session / Java.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

from pipeline import schemas as sc  # noqa: E402

# grid position (column, row) of every box, row 0 at the top
LAYOUT = {
    "gold_dim_date": (0, 0), "gold_dim_wiki": (1.5, 0), "gold_dim_change_type": (3, 0),
    "gold_fact_revision": (0.75, 1), "gold_fact_page_event": (2.25, 1),
    "gold_dim_page": (0.75, 2), "gold_dim_editor": (2.25, 2),
    "gold_agg_wiki_daily": (4.5, 0), "gold_agg_revert_latency": (4.5, 0.75),
    "gold_agg_edit_wars": (4.5, 1.5), "gold_agg_editor_monthly": (4.5, 2.25),
}
COLORS = {"dim": "#dbeafe", "fact": "#fde68a", "agg": "#dcfce7"}
LINE_COLORS = {"gold_dim_date": "#2563eb", "gold_dim_wiki": "#9333ea", "gold_dim_page": "#ea580c",
               "gold_dim_editor": "#dc2626", "gold_dim_change_type": "#0d9488"}
X_STEP, Y_STEP, BOX_W, LINE_H = 3.2, 3.6, 2.7, 0.22


def box_lines(key):
    _, name, schema, _, pk = sc.TABLES[key]
    fks = {c for c, *_ in sc.GOLD_FKS.get(key, [])}
    lines = []
    for f in schema.fields:
        tags = (["PK"] if f.name in pk else []) + (["FK"] if f.name in fks else [])
        if tags:
            lines.append(f"{'/'.join(tags):<5} {f.name}")
    rest = len(schema.fields) - len(lines)
    if rest:
        lines.append(f"      + {rest} more columns")
    return name, lines


def main():
    fig, ax = plt.subplots(figsize=(19, 12))
    ax.set_axis_off()
    boxes = {}
    for key, (cx, cy) in LAYOUT.items():
        name, lines = box_lines(key)
        h = 0.62 + LINE_H * len(lines)
        x, y = cx * X_STEP, -cy * Y_STEP
        kind = name.split("_")[0]
        boxes[key] = (x, y, BOX_W, h)
        ax.add_patch(FancyBboxPatch((x, y - h), BOX_W, h, boxstyle="round,pad=0.02",
                                    fc=COLORS[kind], ec="#334155", lw=1.2, zorder=3))
        ax.text(x + 0.1, y - 0.28, name, fontsize=11, fontweight="bold", va="center", zorder=4)
        for i, line in enumerate(lines):
            ax.text(x + 0.1, y - 0.62 - i * LINE_H, line, fontsize=8.2, family="monospace",
                    va="center", zorder=4, color="#475569" if line.strip().startswith("+") else "#0f172a")

    def centre(key):
        x, y, w, h = boxes[key]
        return x + w / 2, y - h / 2

    for key, fks in sc.GOLD_FKS.items():
        seen = set()
        for _, ref, _ in fks:
            if ref in seen:      # fact_revision has two date FKs, one line is enough
                continue
            seen.add(ref)
            (x1, y1), (x2, y2) = centre(key), centre(ref)
            ax.annotate("", xy=(x2, y2), xytext=(x1, y1), zorder=1,
                        arrowprops=dict(arrowstyle="-", color=LINE_COLORS[ref], lw=1.1, alpha=0.55,
                                        connectionstyle="arc3,rad=0.08"))

    ax.set_xlim(-0.4, 4.5 * X_STEP + BOX_W + 0.4)
    ax.set_ylim(-2.25 * Y_STEP - 1.6, 0.9)
    ax.text(-0.3, 0.55, "pkwiki_gold: star schema", fontsize=15, fontweight="bold")
    ax.text(-0.3, 0.2, "blue = dimension, yellow = fact, green = aggregate fact. "
            "Lines are foreign keys, coloured by the dimension they point to.", fontsize=9.5, color="#334155")
    out = os.path.join(ROOT, "docs", "gold_er_diagram.png")
    fig.savefig(out, dpi=130, bbox_inches="tight", facecolor="white")
    print(f"written {out}")


if __name__ == "__main__":
    main()
