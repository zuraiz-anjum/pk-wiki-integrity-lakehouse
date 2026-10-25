"""
Prints the data dictionary (markdown) straight from pipeline/schemas.py, so the README tables
always match the schemas the pipeline actually creates.

    python scripts/print_data_dictionary.py                      # every table
    python scripts/print_data_dictionary.py --table revision     # one table (key or table name)
    python scripts/print_data_dictionary.py --table bronze_rc,quarantine
    python scripts/print_data_dictionary.py --layer gold         # one layer (docs/gold_data_dictionary.md)

Only needs pyspark installed (pyspark.sql.types), no Spark session / Java.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from pyspark.sql.types import StructType  # noqa: E402

from pipeline import schemas as sc  # noqa: E402

# same as pipeline.gold.CLUSTER_COLUMNS, copied so this script doesn't need pyspark.sql.functions
CLUSTER_COLUMNS = {
    "gold_fact_revision": ["date_key", "wiki_key", "page_key"],
    "gold_fact_page_event": ["date_key", "wiki_key", "page_key"],
    "gold_dim_page": ["wiki_key", "page_id"],
    "gold_dim_editor": ["wiki_key", "editor_hash"],
}

LAYER_TITLES = {"bronze": "Bronze", "silver": "Silver", "gold": "Gold (star schema)",
                "ops": "Ops (audit, drift, quarantine, gold watermark)"}

# one line per table, shown under the heading
TABLE_NOTES = {
    "bronze_history": "Full load. One row per line of the MediaWiki history dump, all 78 fields kept as text.",
    "bronze_rc": "Incremental load. One row per line of the recent changes jsonl files, record parsed "
                 "with our declared types, original line kept in raw_json.",
    "silver_revision": "One row per edit, from both sources. Cast, masked and deduplicated.",
    "silver_page_event": "Page creations, moves, deletions, restores, protections, uploads, merges.",
    "silver_user_event": "Account creations, renames, blocks, unblocks and group changes.",
    "silver_page": "SCD Type 2 page dimension (title, namespace, redirect, deleted). One row per version.",
    "silver_editor": "SCD Type 2 editor dimension (editor_type, is_bot). One row per version.",
    "gold_dim_date": "Calendar dimension, one row per day from 2001-01-01 to 2030-12-31.",
    "gold_dim_wiki": "One row per wiki: language, script and when temporary accounts first show up in the data.",
    "gold_dim_page": "SCD Type 2 page dimension, one row per version of a page (from silver.page) plus one "
                     "unknown page per wiki (key -1 to -4).",
    "gold_dim_editor": "SCD Type 2 editor dimension, one row per version of an editor (from silver.editor) plus "
                       "one unknown editor per wiki (key -1 to -4). Editors are only known by their salted hash.",
    "gold_dim_change_type": "What kind of change a fact row is: 4 revision types and 8 page event types.",
    "gold_fact_revision": "Fact, one row per edit. Measures for growth, reverts, vandalism and bot share.",
    "gold_fact_page_event": "Fact, one row per page log event (create, move, delete, restore, protect, ...).",
    "gold_agg_wiki_daily": "Aggregate fact, one row per wiki per day, built from the two facts.",
    "gold_agg_revert_latency": "Aggregate fact, time to revert per wiki, month and editor type of the reverted edit.",
    "gold_agg_edit_wars": "Aggregate fact, page-days with at least 3 reverts by at least 2 different editors.",
    "gold_agg_editor_monthly": "Aggregate fact, editor counts and next-month return per wiki, month and human / bot.",
    "gold_watermark": "Newest silver load_timestamp each gold table has already taken in (incremental refresh).",
    "execution_log": "Audit log. One row per unit of work (file/partition or table) per run.",
    "drift_log": "Every schema drift we noticed, one row per field per batch.",
    "quarantine": "Rows that failed a silver check, with the reasons and the raw line so they can be replayed.",
}

RAW_HISTORY = set(sc.HISTORY_COLUMN_NAMES)

# used only when the column has no comment in schemas.py (the comment there always wins).
# (table key, column) first, then just the column name.
FALLBACK = {
    # api record fields
    "record.pageid": "page id", "record.revid": "revision id of this edit (0 for log events)",
    "record.old_revid": "previous revision of the page", "record.userid": "editor's user id (0 for logged out)",
    "record.new": "edit created the page", "record.minor": "marked as a minor edit",
    "record.oldlen": "page size in bytes before", "record.newlen": "page size in bytes after",
    "record.comment": "edit summary (raw in bronze, masked in silver)",
    "record.redirect": "page is a redirect", "record.tags": "change tags, e.g. mw-undo, mw-reverted",
    "record.logid": "log entry id (log events only)", "record.logtype": "move, delete, block, newusers, ...",
    "record.logaction": "action within the log type, e.g. delete / restore",
    # shared lineage / bookkeeping columns
    "wiki": "urwiki, pnbwiki, sdwiki, pswiki",
    "source_file": "file the row came from",
    "batch_id": "run id that last wrote the row",
    "load_timestamp": "when this row was written",
    "source_ref": "snapshot for dump rows, ingest_date for api rows",
    "row_hash": "hash of the source attributes, MERGE only updates when it changes",
    "attr_hash": "hash of the tracked attributes",
    "comment_masked": "edit / log summary with IPs, emails and user links masked",
    "editor_key": "salted hash of the editor (bot names kept)",
    "log_id": "MediaWiki log id",
    "page_id": "MediaWiki page id",
    "page_namespace": "namespace number (0 = articles)",
    "event_ts": "event time UTC", "event_ts_pkt": "event time in Pakistan time",
    "tags": "change tags", "block_flags": "block options, e.g. nocreate, noautoblock",
    # silver.revision
    "page_is_redirect": "page was a redirect", "is_bot": "edit made by a bot",
    "bytes_diff": "size change in bytes", "is_minor": "marked as a minor edit",
    "is_page_creation": "first revision of the page",
    "reverted_by_rev_id": "revision that undid this one", "seconds_to_revert": "how long the edit survived",
    # scd2
    "page_title": "title in this version", "is_redirect": "redirect in this version",
    "is_deleted": "deleted in this version", "valid_from": "when this version started",
    "valid_to": "when it was replaced, null while current", "is_current": "true for the latest version",
    ("silver_editor", "is_bot"): "bot account (name kept unhashed)",
    # ops
    ("execution_log", "log_id"): "random id of the log row",
    "target_table": "table written by the step", "start_ts": "step start (UTC)", "end_ts": "step end (UTC)",
    "duration_sec": "end_ts - start_ts in seconds", "rows_read": "rows the step read from its source",
    "rows_inserted": "rows inserted by the MERGE", "rows_updated": "rows updated by the MERGE",
    "rows_deleted": "rows deleted by the MERGE (monthly handover only)",
    "rows_quarantined": "rows sent to ops.quarantine", "error_message": "exception and short traceback when FAILED, reason when SKIPPED",
    "row_count": "rows affected in that batch", "sample_value": "first 300 chars of one affected line",
    "action_taken": "what the pipeline did about it", "first_seen_run_id": "run that first logged it",
    ("drift_log", "field_name"): "field name, or the column count for column_count drift",
    ("quarantine", "record_hash"): "record_hash of the bronze row",
    ("quarantine", "wiki"): "wiki folder of the bronze row",
    ("quarantine", "batch_ref"): "snapshot or ingest_date of the bronze row",
}


def describe_col(key, name, comment):
    return comment or FALLBACK.get((key, name)) or FALLBACK.get(name, "")


def cell(text):
    """Make text safe inside a markdown table cell (| splits cells, <x> looks like html)."""
    return (text or "").replace("|", "\\|").replace("<", "&lt;").replace(">", "&gt;")


def type_str(dtype):
    if isinstance(dtype, StructType):
        return "struct"
    return dtype.simpleString()


def row(name, dtype, nullable, comment, pk, fks=None):
    null = "yes" if nullable else "no"
    keys = (["PK"] if name in pk else []) + [f"FK {ref}" for col, ref in (fks or []) if col == name]
    key = ", ".join(keys)
    return f"| `{name}` | `{type_str(dtype)}` | {null} | {key} | {cell(comment)} |"


def table_rows(key, schema, pk):
    out = ["| Column | Type | Null | Key | Description |", "| --- | --- | --- | --- | --- |"]
    fks = [(col, f"{sc.TABLES[ref][1]}.{ref_col}") for col, ref, ref_col in sc.GOLD_FKS.get(key, [])]
    collapsed = False
    for fld in schema.fields:
        comment = describe_col(key, fld.name, fld.metadata.get("comment", ""))
        if key == "bronze_history" and fld.name in RAW_HISTORY:
            if not collapsed:
                first, last = sc.HISTORY_COLUMN_NAMES[0], sc.HISTORY_COLUMN_NAMES[-1]
                out.append(f"| `{first}` ... `{last}` (78 columns) | `string` | yes | | "
                           "the dump fields in source order, read as raw text. Cast in silver, "
                           "full list with target types below |")
                collapsed = True
            continue
        out.append(row(fld.name, fld.dataType, fld.nullable, comment, pk, fks))
        if isinstance(fld.dataType, StructType):
            for sub in fld.dataType.fields:
                path = f"{fld.name}.{sub.name}"
                out.append(row(path, sub.dataType, sub.nullable,
                               describe_col(key, path, sub.metadata.get("comment", "")), pk))
    return out


def history_column_list():
    out = ["<details>",
           "<summary>All 78 dump columns, in file order, with the type silver casts them to (when silver uses them)</summary>",
           "",
           "| # | Column | Typed as |",
           "| --- | --- | --- |"]
    for i, (name, t) in enumerate(sc.HISTORY_COLUMNS, 1):
        t = "array<string>" if t == "array" else t
        out.append(f"| {i} | `{name}` | `{t}` |")
    out += ["", "</details>"]
    return out


def describe(key):
    layer, name, schema, parts, pk = sc.TABLES[key]
    lines = [f"#### pkwiki_{layer}.{name}", ""]
    if key in TABLE_NOTES:
        lines += [TABLE_NOTES[key], ""]
    if parts:
        part_txt = "Partitioned by " + ", ".join(f"`{p}`" for p in parts)
    elif layer == "gold" and key in CLUSTER_COLUMNS:
        part_txt = "Not partitioned, clustered by " + ", ".join(f"`{c}`" for c in CLUSTER_COLUMNS[key])
    else:
        part_txt = "Not partitioned"
    lines += [f"{part_txt}. Primary key: {', '.join(f'`{c}`' for c in pk)}.", ""]
    lines += table_rows(key, schema, pk)
    if key == "bronze_history":
        lines += [""] + history_column_list()
    return lines + [""]


def pick(filter_arg, layer=None):
    if layer:
        return [k for k, v in sc.TABLES.items() if v[0] == layer or (layer == "gold" and k == "gold_watermark")]
    if not filter_arg:
        return list(sc.TABLES)
    wanted = [w.strip() for w in filter_arg.split(",") if w.strip()]
    keys = [k for k, v in sc.TABLES.items() if k in wanted or v[1] in wanted]
    if not keys:
        sys.exit(f"no table matches {wanted}. keys: {list(sc.TABLES)}")
    return keys


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--table", help="comma separated table keys or names, e.g. silver_revision or revision")
    ap.add_argument("--layer", help="bronze, silver, gold or ops (gold also lists ops.gold_watermark)")
    a = ap.parse_args()
    keys = pick(a.table, a.layer)
    out, layer_seen = [], None
    for k in keys:
        layer = sc.TABLES[k][0]
        if layer != layer_seen and not a.table:
            out += [f"### {LAYER_TITLES[layer]}", ""]
            layer_seen = layer
        out += describe(k)
    sys.stdout.reconfigure(encoding="utf-8")
    print("\n".join(out).rstrip() + "\n")


if __name__ == "__main__":
    main()
