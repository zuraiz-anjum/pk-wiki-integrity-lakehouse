"""
Report sections that depend on measured results: the Gold strategy text, the Spark plan walk-through
and the daily run cost. Measured numbers come from docs/databricks_facts.md and the plan from
docs/gold_fact_explain.txt.
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
FIG = os.path.join(HERE, "figures")

CLUSTER_TEXT = ("date_key, wiki_key and page_key on the facts, wiki_key and page_id on dim_page, wiki_key and "
                "editor_hash on dim_editor")


def gold_strategy_paragraphs(X, c):
    p, table = X["p"], X["table"]
    s = []
    s.append(p(
        "Gold runs as the last task of every job run, right after the Silver SCD2 step. The order "
        "inside the task matters: the small dimensions first, then " + c("dim_page") + " and " +
        c("dim_editor") + ", then the two event facts (which look up dimension versions), and the "
        "aggregates last because they are built from the facts."))
    s.append(p(
        "<b>Facts and SCD2 dimensions</b> are MERGEd on their surrogate key. The source is only the "
        "Silver rows whose " + c("load_timestamp") + " is newer than the table's watermark, plus any "
        "fact rows that still point at an unknown member. A matched row is updated only when its " +
        c("row_hash") + " differs (" + c("NOT (t.row_hash <=> s.row_hash)") + "), so a rerun is a "
        "no-op and an unchanged row is never rewritten. The watermark is moved in a separate MERGE "
        "only after the data MERGE has succeeded, so a failed run simply reads the same rows again "
        "next time. The page and editor version for each edit is found with one equi-join on the "
        "natural key plus a window that picks the version whose validity range contains the edit "
        "time. We avoided a range join on " + c("valid_from") + " and " + c("valid_to") + " because "
        "Spark cannot hash-partition on an inequality and would fall back to a much slower nested "
        "loop. A full load MERGEs against all of Silver and adds " +
        c("WHEN NOT MATCHED BY SOURCE THEN DELETE") + ", so Gold follows the monthly handover."))
    s.append(p(
        "<b>Aggregates</b> are not MERGEd from a delta. A median or a count of distinct editors cannot "
        "be updated by adding new rows to old totals, so we recompute whole groups instead. The run "
        "collects the " + c("date_key") + " values of the fact rows it touched (they carry its " +
        c("batch_id") + "), recomputes " + c("agg_wiki_daily") + " and " + c("agg_edit_wars") +
        " for those days and " + c("agg_revert_latency") + " for those months, and MERGEs the result "
        "with a scoped delete such as " + c("WHEN NOT MATCHED BY SOURCE AND t.date_key IN (...) THEN DELETE") +
        ". That removes groups that no longer exist without touching any other day. A normal daily "
        "run touches only a day or two (the last one on 25 October touched one), so the aggregates "
        "cost little. If a backfill touches "
        "more than 400 days, we simply recompute everything, which is cheaper than a huge IN list. "
        "This is the same result " + c("replaceWhere") + " would give, but a MERGE leaves unchanged "
        "rows alone and records exact insert, update and delete counts in the audit log."))
    s.append(p(
        "<b>Optimisation.</b> After the loads, " + c("optimize_gold()") + " lays out the four tables "
        "that grow with the data. On Databricks it uses liquid clustering: " +
        c("ALTER TABLE ... CLUSTER BY (date_key, wiki_key, page_key)") + " is set once, and the daily " +
        c("OPTIMIZE") + " only clusters files that are not clustered yet, so it stays cheap as the "
        "table grows. The open source Delta we use locally has no " + c("CLUSTER BY") + " for existing "
        "tables, so there the same function falls back to " + c("OPTIMIZE ... ZORDER BY") + ". We "
        "chose liquid clustering over partitions plus ZORDER for three reasons: the tables are small "
        "enough that partitions would create small files, ZORDER rewrites everything each time while "
        "liquid clustering is incremental, and the keys can be changed later without a rewrite. The "
        "chosen keys follow the dashboard: almost every visual filters on a date range and a wiki, and "
        "the edit-war and page views join on " + c("page_key") + ". An OPTIMIZE that fails is logged as "
        "SKIPPED and never fails the load, because layout only affects speed, not correctness."))
    s.append(p(
        "<b>What the Databricks runs showed.</b> The current numbers come from one multi-task run on "
        "serverless on 25 October, after the key fix in Section 4.6. It first ran a full Gold refresh "
        "(" + c("dbx-gold-full-3") + ", 167 seconds of execution), then a normal daily pass through "
        "Bronze, Silver, SCD2 and Gold (" + c("dbx-gold-incr-2") + ") on a second extractor pull of 168 "
        "new changes for the same day. The incremental Gold task took 162 seconds. Table " +
        str(X["tab_no"]()) + " shows what each Gold table looked like before and after that daily pass."))
    s += table([
        ["Gold table", "After full refresh", "After daily run", "Change", "How the daily run wrote it"],
        ["dim_page (SCD2)", "13,859", "13,886", "+27", "MERGE of Silver versions newer than the watermark"],
        ["dim_editor (SCD2)", "577", "578", "+1", "MERGE of Silver versions newer than the watermark"],
        ["fact_revision", "32,102", "32,144", "+42", "MERGE of new Silver revisions"],
        ["fact_page_event", "7,786", "7,799", "+13", "MERGE of new Silver page events"],
        ["agg_wiki_daily", "727", "727", "0", "recomputed for the 1 touched day, 3 rows updated"],
        ["agg_revert_latency", "59", "59", "0", "recomputed for the touched month, no change"],
        ["agg_edit_wars", "18", "18", "0", "recomputed for the touched day; 6 rows are strict edit wars"],
        ["agg_editor_monthly", "160", "160", "0", "recomputed, 4 rows updated"],
        ["dim_date, dim_wiki, dim_change_type", "10,957 / 4 / 12", "unchanged", "0", "no-op MERGE"],
    ], [2.4, 1.3, 1.3, 0.8, 3.9], "Gold row counts on Databricks before and after one daily incremental run.")
    s.append(p(
        "The facts match Silver row for row (Silver " + c("revision") + " also has 32,144 rows), and "
        "each SCD2 dimension is its Silver table plus the four unknown members. The checks found no "
        "duplicate surrogate keys, one row per business key in both facts, and zero orphans in all 15 "
        "foreign-key checks (a " + c("LEFT ANTI JOIN") + " of every fact key to its dimension). "
        "Active editors in " + c("agg_wiki_daily") + " match a recount from " + c("fact_revision") +
        " on every row we compared. " + c("DESCRIBE DETAIL") + " lists the clustering columns on all "
        "four clustered tables, and OPTIMIZE compacts the files each daily MERGE adds back into one "
        "per table."))

    s += X["h2"]("A surrogate key bug, found and fixed")
    s.append(p(
        "The first Gold runs looked fine on row counts, but a key check showed a real bug. " +
        c("xxhash64") + " returns a signed 64-bit number, so about half of all hashes are negative. "
        "Our code reserves negative keys for the unknown members (" + c("-1") + " to " + c("-4") + "), "
        "and the point-in-time lookup skips dimension rows with a negative key. As a result 7,060 of "
        "13,859 page versions and 274 of 577 editor versions could never be found, and 62.68 percent "
        "of " + c("fact_revision") + " rows pointed at an unknown page (37.83 percent at an unknown "
        "editor). Every incremental run also re-read those rows as \"retries\", which inflated its "
        "input."))
    s.append(p(
        "The fix is one line in " + c("sk()") + ": clear the sign bit with a bitwise AND of " +
        c("0x7FFFFFFFFFFFFFFF") + ", so every real key is zero or positive. Because about half of the "
        "keys changed, we ran a full Gold refresh before any incremental run. The full path re-keyed "
        "7,060 page and 274 editor versions, replaced 16,097 revision facts with re-keyed copies and "
        "repointed 9,700 more, and its " + c("WHEN NOT MATCHED BY SOURCE THEN DELETE") + " removed the "
        "rows with old keys. An incremental run first would have matched on the new " +
        c("revision_key") + ", found nothing, and inserted duplicates. Afterwards the unknown-page share "
        "fell to 23.09 percent, all of it explained (Section 3.1), and the unknown-editor share to "
        "zero. The lesson for us was to test key ranges and orphan counts, not only row counts."))
    return s
    return s


from phase3_dag import dag_section  # noqa: E402,F401
