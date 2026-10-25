"""
Figures for the Phase 3 report. Writes PNGs into docs/figures/.

    python docs/phase3_figures.py

Needs matplotlib only. The cost chart reads its numbers from phase3_costs.py so the chart and
the tables in the PDF always agree.
"""
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import phase3_costs as costs  # noqa: E402

FIG_DIR = os.path.join(HERE, "figures")
os.makedirs(FIG_DIR, exist_ok=True)

INK = "#1f2933"
MUTED = "#52606d"
LAYER = {
    "source": ("#eef2f7", "#7b8794"),
    "landing": ("#f5f7fa", "#9aa5b1"),
    "bronze": ("#f6e7d8", "#a8642a"),
    "silver": ("#e4e9ef", "#5f6f81"),
    "gold": ("#fbf0c9", "#b38600"),
    "bi": ("#e3f1e8", "#2f7d4f"),
    "ops": ("#fbe9e9", "#b44646"),
    "job": ("#e8e6f6", "#5a4fb0"),
}


def box(ax, x, y, w, h, kind, title, lines=(), title_size=9.5, line_size=7.6, dashed=False, step=1.75):
    face, edge = LAYER[kind]
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.25,rounding_size=0.9",
                                linewidth=1.3, edgecolor=edge, facecolor=face,
                                linestyle="--" if dashed else "-"))
    ax.text(x + 0.9, y + h - 1.0, title, fontsize=title_size, fontweight="bold", color=INK,
            va="top", ha="left")
    for i, line in enumerate(lines):
        mono = line.startswith(" ")
        ax.text(x + 0.9, y + h - 3.6 - i * step, line.strip() if not mono else line[1:], fontsize=line_size,
                color=MUTED, va="top", ha="left", family="DejaVu Sans Mono" if mono else None)


def arrow(ax, p0, p1, label=None, color=INK, dashed=False, rad=0.0, label_xy=None, size=7.2):
    ax.add_patch(FancyArrowPatch(p0, p1, arrowstyle="-|>", mutation_scale=11, linewidth=1.2,
                                 color=color, linestyle="--" if dashed else "-",
                                 connectionstyle=f"arc3,rad={rad}", shrinkA=1, shrinkB=1))
    if label:
        lx, ly = label_xy or ((p0[0] + p1[0]) / 2, (p0[1] + p1[1]) / 2 + 0.6)
        ax.text(lx, ly, label, fontsize=size, color=color, ha="center", va="bottom",
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.95))


def header(ax, x, w, text, kind):
    ax.text(x + w / 2, 59.3, text, fontsize=8, color=LAYER[kind][1], ha="center", fontweight="bold")


def architecture(gold_lines):
    fig = plt.figure(figsize=(13.6, 7.1), dpi=200)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 136)
    ax.set_ylim(0, 71)
    ax.axis("off")

    # columns: (x, w)
    SRC, LND, BRZ, SLV, GLD, BI, QS = (1, 17), (21, 16), (40, 17), (60, 19.5), (82.5, 18), (104, 15), (122, 13)

    box(ax, BRZ[0], 61.5, QS[0] + QS[1] - BRZ[0], 8.5, "job",
        "Databricks Job  pkwiki-lakehouse-daily   (max 1 concurrent run, notebooks pulled from GitHub main)",
        ["tasks: bronze > silver > scd2 > gold     daily 21:00 UTC (02:00 PKT)     one run_id shared by all tasks     same parameters for daily, full and backfill runs"],
        title_size=9.0, line_size=7.6)
    for c in (BRZ, SLV, GLD):
        ax.plot([c[0] + c[1] / 2] * 2, [61.3, 60.6], color=LAYER["job"][1], lw=1.2, ls=":")

    header(ax, *SRC, "SOURCES", "source")
    header(ax, *LND, "LANDING", "landing")
    header(ax, *BRZ, "BRONZE (raw)", "bronze")
    header(ax, *SLV, "SILVER (clean, masked)", "silver")
    header(ax, *GLD, "GOLD (star schema)", "gold")
    header(ax, *BI, "SERVING", "bi")

    box(ax, SRC[0], 41, SRC[1], 16, "source", "MediaWiki history dump", title_size=9.0, lines=
        ["monthly snapshot, TSV.bz2", "78 columns, no header", "full load", "rewrites past records"])
    box(ax, SRC[0], 22, SRC[1], 16, "source", "recentchanges API",
        ["MediaWiki Action API, JSON", "from saved watermark", "incremental, daily", "~17k events per day"])

    box(ax, LND[0], 24, LND[1], 32, "landing", "UC volume",
        ["pkwiki_raw/landing", "", " load_type=full/", "  snapshot=YYYY-MM/", "  wiki=<wiki>/", "",
         " load_type=incremental/", "  ingest_date=YYYY-MM-DD/", "  wiki=<wiki>/", "",
         "extractor:", " ingest_wikimedia.py", " watermarks.json"], line_size=7.0)
    arrow(ax, (SRC[0] + SRC[1], 49), (LND[0], 46))
    arrow(ax, (SRC[0] + SRC[1], 30), (LND[0], 33))

    box(ax, BRZ[0], 41, BRZ[1], 16, "bronze", "mediawiki_history",
        ["partitions: snapshot, wiki", "78 string cols + lineage", "_corrupt_record kept", "insert-only MERGE (hash)"])
    box(ax, BRZ[0], 22, BRZ[1], 16, "bronze", "recentchanges",
        ["partitions: ingest_date, wiki", "typed record + raw_json", "unknown / mismatch fields", "insert-only MERGE (hash)"])
    arrow(ax, (LND[0] + LND[1], 49), (BRZ[0], 49), "01", label_xy=(38.5, 49.6))
    arrow(ax, (LND[0] + LND[1], 30), (BRZ[0], 30), "01", label_xy=(38.5, 30.6))

    box(ax, SLV[0], 40, SLV[1], 17, "silver", "revision / page_event / user_event",
        ["partition: wiki", "PK (wiki, rev_id | event_key)", "MERGE only if row_hash changed",
         "dump wins over api", "monthly handover delete"], title_size=7.6)
    box(ax, SLV[0], 22, SLV[1], 15, "silver", "page, editor (SCD Type 2)",
        ["valid_from, valid_to, is_current", "new version only if attr_hash", "revert flags on revision",
         "(15 revisions / 30 days)"], title_size=8.8)
    arrow(ax, (BRZ[0] + BRZ[1], 49), (SLV[0], 49), "02", label_xy=(58.5, 49.6))
    arrow(ax, (BRZ[0] + BRZ[1], 30), (SLV[0], 44), "02", label_xy=(58.2, 38.5))
    arrow(ax, (SLV[0] + SLV[1] / 2, 40), (SLV[0] + SLV[1] / 2, 37), "03", label_xy=(SLV[0] + SLV[1] / 2 + 2.6, 37.6))

    box(ax, GLD[0], 22, GLD[1], 35, "gold", "pkwiki_gold", gold_lines, line_size=7.2, step=1.62)
    arrow(ax, (SLV[0] + SLV[1], 49), (GLD[0], 49), "04", label_xy=(81, 49.6))
    arrow(ax, (SLV[0] + SLV[1], 29), (GLD[0], 29), "04", label_xy=(81, 29.6))

    box(ax, BI[0], 31, BI[1], 17, "bi", "Power BI",
        ["Knowledge Integrity", "Monitor", "SQL warehouse +", "Databricks connector", "reads Gold + ops"])
    arrow(ax, (GLD[0] + GLD[1], 39.5), (BI[0], 39.5))
    box(ax, QS[0], 31, QS[1], 17, "source", "Questions",
        ["Q1 growth", "Q2 revert latency", "Q3 edit wars", "Q4 bot share", "Q5 temp accounts"])
    arrow(ax, (BI[0] + BI[1], 39.5), (QS[0], 39.5))

    box(ax, BRZ[0], 3, 26, 12.5, "ops", "pkwiki_ops (audit and quality)",
        ["pipeline_execution_logs: one row per step", "schema_drift_log: new field, type, columns",
         "quarantine: reasons + raw payload", "gold_watermark: last Silver load_timestamp"], line_size=7.2)
    box(ax, 68.5, 3, 11, 12.5, "landing", "Secret scope",
        ["pkwiki/pii_salt", "salt for hashing", "editor names"], line_size=7.0, title_size=8.6)
    box(ax, GLD[0], 3, GLD[1] + 4, 12.5, "landing", "Delta maintenance",
        ["liquid clustering + OPTIMIZE", "(ZORDER fallback on OSS Delta)", "VACUUM weekly, DESCRIBE HISTORY"], line_size=7.2,
        title_size=8.8)
    red = LAYER["ops"][1]
    arrow(ax, (BRZ[0] + 8, 22), (BRZ[0] + 8, 15.7), color=red, dashed=True)
    arrow(ax, (SLV[0] + 3, 22), (BRZ[0] + 22, 15.7), color=red, dashed=True)
    arrow(ax, (GLD[0] + 3, 22), (BRZ[0] + 25, 13.5), color=red, dashed=True, rad=-0.1)
    arrow(ax, (74, 15.7), (74, 22), color=MUTED, dashed=True)
    ax.text(BRZ[0] + 13, 17.8, "every step logs a row; bad rows go to quarantine", fontsize=6.8, color=red,
            ha="center", bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none"))

    out = os.path.join(FIG_DIR, "architecture.png")
    fig.savefig(out, dpi=200, facecolor="white")
    plt.close(fig)
    return out


def cost_chart():
    rows = costs.enterprise_rows()
    labels = [r["short"] for r in rows]
    dbu = [r["dbu_cost"] for r in rows]
    vm = [r["vm_cost"] for r in rows]
    fig, ax = plt.subplots(figsize=(8.6, 3.9), dpi=200)
    y = range(len(rows))[::-1]
    ax.barh(list(y), dbu, color="#3b6ea8", label="DBU charge")
    ax.barh(list(y), vm, left=dbu, color="#9cb9d9", label="VM charge (classic only)")
    for yi, r in zip(y, rows):
        ax.text(r["total"] + 250, yi, f"${r['total']:,.0f}", va="center", fontsize=8.5, color=INK)
    ax.set_yticks(list(y))
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.set_xlabel("Compute cost per year, USD (8,760 hourly runs)", fontsize=8.5)
    ax.tick_params(axis="x", labelsize=8)
    ax.set_xlim(0, max(r["total"] for r in rows) * 1.18)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.legend(fontsize=8, frameon=False, loc="lower right")
    ax.grid(axis="x", color="#e4e7eb", lw=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()
    out = os.path.join(FIG_DIR, "enterprise_cost.png")
    fig.savefig(out, dpi=200, facecolor="white")
    plt.close(fig)
    return out


def dag_placeholder():
    """Grey box used in the PDF until docs/figures/spark_ui_dag.png exists."""
    fig = plt.figure(figsize=(8.6, 4.6), dpi=150)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    ax.add_patch(FancyBboxPatch((0.03, 0.05), 0.94, 0.9, boxstyle="round,pad=0.01", lw=2,
                                ls="--", ec="#b44646", fc="#fdf6f6"))
    ax.text(0.5, 0.66, "PASTE SCREENSHOT HERE", ha="center", fontsize=18,
            fontweight="bold", color="#b44646")
    ax.text(0.5, 0.53, "Databricks Query Profile of MERGE INTO agg_wiki_daily (serverless)",
            ha="center", fontsize=12, color=INK)
    ax.text(0.5, 0.40, "Save it as docs/figures/spark_ui_dag.png, then run python docs/phase3_report.py",
            ha="center", fontsize=10.5, color=MUTED)
    ax.text(0.5, 0.31, "Gold task run page  >  See performance  >  MERGE INTO ... agg_wiki_daily  >  graph view",
            ha="center", fontsize=10, color=MUTED)
    out = os.path.join(FIG_DIR, "spark_ui_dag_placeholder.png")
    fig.savefig(out, dpi=150, facecolor="white")
    plt.close(fig)
    return out


def star_schema():
    """The classic star around fact_revision (the full model is in gold_er_diagram.png)."""
    fig = plt.figure(figsize=(10.5, 5.0), dpi=200)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 105)
    ax.set_ylim(0, 50)
    ax.axis("off")
    fact = (37, 10, 31, 25)
    box(ax, *fact, "gold", "fact_revision  (one row per edit)",
        [" PK revision_key", " FK wiki_key", " FK date_key, pkt_date_key", " FK page_key", " FK editor_key",
         " FK change_type_key", "measures: bytes_added, bytes_removed,", "seconds_to_revert",
         "flags: is_bot_edit, is_anonymous_edit,", "is_revert, is_reverted,", "is_vandalism_suspect, is_new_article"],
        title_size=9.5, line_size=7.6)
    dims = [
        ((2, 38, 27, 10.5), "dim_date (role playing)", [" PK date_key  yyyymmdd", "UTC day and Pakistan day", "month, weekday, holidays"]),
        ((39, 38.5, 27, 10.5), "dim_wiki", [" PK wiki_key", "language, script, site", "temp accounts first seen"]),
        ((76, 38, 27, 10.5), "dim_change_type", [" PK change_type_key", "create, revert, minor, edit", "page events: move, delete ..."]),
        ((2, 5, 27, 12.5), "dim_page (SCD2)", [" PK page_key  xxhash64", "page_id, title, namespace", "is_redirect, is_deleted",
                                            "valid_from, valid_to"]),
        ((76, 5, 27, 12.5), "dim_editor (SCD2)", [" PK editor_key  xxhash64", "editor_hash (masked)", "editor_type, is_bot",
                                               "valid_from, valid_to"]),
    ]
    for (x, y, w, h), t, lines in dims:
        box(ax, x, y, w, h, "silver", t, lines, title_size=9.2, line_size=7.6)
    cx, cy = fact[0] + fact[2] / 2, fact[1] + fact[3] / 2
    for (x, y, w, h), t, _ in dims:
        tx = min(max(cx, x), x + w)
        ty = min(max(cy, y), y + h)
        fx = min(max(tx, fact[0]), fact[0] + fact[2])
        fy = min(max(ty, fact[1]), fact[1] + fact[3])
        ax.plot([fx, tx], [fy, ty], color="#5f6f81", lw=1.3)
        ax.text(fx + (tx - fx) * 0.12, fy + (ty - fy) * 0.12, "*", fontsize=11, color=INK, ha="center", va="center")
        ax.text(fx + (tx - fx) * 0.9, fy + (ty - fy) * 0.9, "1", fontsize=9, color=INK, ha="center", va="center",
                bbox=dict(boxstyle="circle,pad=0.1", fc="white", ec="none"))
    ax.text(52.5, 1.2, "Same pattern for fact_page_event; the four aggregate facts join dim_wiki and dim_date "
                       "(date_key or month_key), agg_edit_wars also joins dim_page.",
            fontsize=7.6, color=MUTED, ha="center")
    out = os.path.join(FIG_DIR, "star_schema.png")
    fig.savefig(out, dpi=200, facecolor="white")
    plt.close(fig)
    return out


def rotated(path):
    from PIL import Image as PILImage
    out = path.replace(".png", "_rotated.png")
    PILImage.open(path).rotate(90, expand=True).save(out)
    return out


DEFAULT_GOLD_LINES = ["event facts", " fact_revision", " fact_page_event", "", "dimensions",
                      " dim_date  dim_wiki", " dim_page (SCD2)", " dim_editor (SCD2)", " dim_change_type", "",
                      "aggregate facts", " agg_wiki_daily", " agg_revert_latency", " agg_edit_wars",
                      " agg_editor_monthly", "", "MERGE from Silver rows newer", "than gold_watermark"]

if __name__ == "__main__":
    print(rotated(architecture(DEFAULT_GOLD_LINES)))
    print(star_schema())
    print(cost_chart())
    print(dag_placeholder())
