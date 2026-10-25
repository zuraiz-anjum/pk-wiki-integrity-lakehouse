"""Section 5 of the Phase 3 report: the Query Profile of a Gold MERGE and the agg_wiki_daily plan."""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
FIG = os.path.join(HERE, "figures")
SHOT = os.path.join(FIG, "spark_ui_dag.png")
SHOT_CROPPED = os.path.join(FIG, "spark_ui_dag_cropped.png")
SHOT_CAPTION = ("Databricks Query Profile (serverless, graph view) of the heaviest MERGE of a full Gold "
                "refresh, the 11.83 s call result = spark.sql(merge_sql), most likely the MERGE into "
                "fact_revision. Shown: execution 3 of 5 of that statement (1.11 s). Read from the bottom up.")


def screenshot_for_pdf():
    """Crop the account bar (it shows a personal e-mail) and the mostly empty query-text panel,
    so the graph gets the page width. The original file is left untouched."""
    if not os.path.exists(SHOT):
        return None
    try:
        from PIL import Image
        im = Image.open(SHOT)
        w, h = im.size
        box = (int(w * 0.40), int(h * 0.077), w, h)
        im.crop(box).save(SHOT_CROPPED)
        return SHOT_CROPPED
    except Exception:  # noqa: BLE001 - fall back to the uncropped file
        return SHOT


def explain_excerpt():
    """Physical plan tree of agg_wiki_daily as run on Databricks (docs/databricks_facts.md)."""
    path = os.path.join(HERE, "databricks_facts.md")
    text = open(path, encoding="utf-8").read()
    i = text.find("== Physical Plan ==", text.find("### Plan of the main fact aggregation"))
    if i < 0:
        return None
    tree = []
    for line in text[i:].splitlines()[1:]:
        if not line.strip():
            break
        tree.append(line.replace("workspace.pkwiki_gold.", ""))
    return "\n".join(tree)


def dag_section(X, c):
    h1, h2, p, bullets, code, figure, table = (X[k] for k in ("h1", "h2", "p", "bullets", "code", "figure", "table"))
    cm = X["cm"]
    s = []
    s += h1("Spark UI and DAG analysis")
    s.append(p(
        "This section looks at two Gold queries. The first is a MERGE, captured in the Databricks Query "
        "Profile while the Gold task ran (Figure " + str(X["fig_no"]()) + "). It shows how Delta matches "
        "new rows to the rows already in a table. The second is the SELECT that builds " +
        c("agg_wiki_daily") + ", the heaviest aggregation, which we read from its " + c("EXPLAIN FORMATTED") +
        " plan (Listing 1). Databricks Free Edition is serverless, and serverless compute has no classic "
        "Spark UI because there is no cluster behind it. The Query Profile [7] takes its place: it shows "
        "the same operator graph, with rows, time and memory for each node."))

    # ------------------------------------------------------------------ the MERGE profile
    s += h2("Query Profile of a Gold MERGE")
    shot = screenshot_for_pdf()
    if shot:
        s.append(figure(shot, SHOT_CAPTION, max_h=15.5 * cm))
    else:
        s.append(figure(os.path.join(FIG, "spark_ui_dag_placeholder.png"),
                        "Databricks Query Profile of MERGE INTO agg_wiki_daily (serverless). Placeholder: save "
                        "the screenshot as docs/figures/spark_ui_dag.png and rebuild (see docs/SCREENSHOT-STEPS.md)."))
    s.append(p(
        "The profile belongs to the slowest statement of a full Gold refresh, a " + c("spark.sql(merge_sql)") +
        " call that took 11.83 seconds. One MERGE statement runs as several Spark executions; this one "
        "had five, and the figure shows the third. It is the step where Delta works out which rows of "
        "the target table the new data touches. Reading from the bottom up:"))
    s += bullets([
        "<b>Source side (left branch).</b> " + c("Rdd Scan") + " and " + c("Row To Columnar") + " read "
        "the MERGE source, the batch of changed rows that " + c("merge_gold()") + " registers as a "
        "temporary view. A " + c("Filter") + " follows. About 11 thousand rows (10.96 K) go in and the "
        "same number come out, so nothing is dropped in this run, but the point is where it sits: the "
        "predicate is applied before any data moves between tasks.",
        "<b>Shuffle into the join.</b> The " + c("Shuffle") + " above the filter repartitions the "
        "source rows on the MERGE key, so that every source row lands in the same task as the target "
        "rows with the same key.",
        "<b>Target side (right branch).</b> " + c("Scan Table") + " reads the Delta target. Delta does "
        "not need every file here. It compares the MERGE condition with the minimum and maximum values "
        "it stores for each file and skips the files that cannot match. On another Gold MERGE of the "
        "same run the profile reported 27 percent of the files skipped and only 2 percent of the bytes "
        "read after the filter.",
        "<b>Inner Join.</b> This join is the heart of the MERGE: it pairs each source row with the "
        "target row that has the same key. Rows that find a partner become updates (if their " +
        c("row_hash") + " differs). Rows that find none become inserts in a later execution.",
        "<b>Two-phase aggregation.</b> " + c("Object Hash Aggregate") + ", then a " + c("Shuffle") +
        ", then a second " + c("Object Hash Aggregate") + ". The first one aggregates inside each task "
        "(partial), the shuffle brings equal keys together, and the second one finishes the job "
        "(final). Delta uses this to count matches per target row, because MERGE must fail if two "
        "source rows match the same target row, and to collect the names of the files that contain "
        "matches. Only those files get rewritten.",
        "<b>Top of the graph.</b> The three " + c("Logical Plan") + " nodes and the " +
        c("Empty Relation") + " are parts of the plan that the profile keeps in logical form or that "
        "were found empty at run time. " + c("Deserialize To Object") + ", " + c("Map Partitions") +
        " and " + c("Serialize From Object") + " are typed Dataset code inside Delta's MERGE that "
        "turns the result into the list of touched files. The " + c("Result Query Stage") + " returns "
        "that small result to the driver.",
    ])
    s.append(p(
        "The whole execution took 1.11 seconds, and the time scale of the profile tops out at 62 "
        "milliseconds for a single operator. Most of the 11.83 seconds of the statement is therefore "
        "not in this matching step. It goes to the other executions, which rewrite the touched files, "
        "and to the Delta commit. That fits what the billing data told us in Section 6: at our volume "
        "the cost is mostly fixed overhead per statement, not data processing."))

    # ------------------------------------------------------------------ the agg_wiki_daily plan
    s += h2("Physical plan of the agg_wiki_daily build")
    s.append(p(
        "The second query is the SELECT inside " + c("wiki_daily_frame()") + " (" + c("pipeline/gold.py") +
        "), whose result is then written with " + c("MERGE INTO agg_wiki_daily") + ". It left-joins " +
        c("fact_revision") + " to " + c("dim_editor") + " on " + c("editor_key") + ", groups by " +
        c("(wiki_key, date_key)") + " and computes about fifteen measures, among them two distinct "
        "counts of editors and a median of " + c("seconds_to_revert") + ". A smaller aggregation over " +
        c("fact_page_event") + " counts deletions, restores and moves per wiki and day, and the two "
        "results are joined on " + c("(wiki_key, date_key)") + ". We ran " + c("EXPLAIN FORMATTED") +
        " on it on Databricks serverless after the full Gold build."))
    tree = explain_excerpt()
    if tree:
        s.append(code(tree))
        s.append(X["Paragraph"]("<b>Listing 1.</b> Physical plan of the SELECT that builds agg_wiki_daily, "
                                "Databricks serverless (EXPLAIN FORMATTED, 25 October 2026, table prefixes "
                                "shortened). The full output is in docs/databricks_facts.md.", X["ST"]["caption"]))
    s.append(p(
        "Every operator is a Photon operator, and the plan ends with \"The query is fully supported by "
        "Photon\". Spark splits the query into four map stages and a result stage, with a boundary at "
        "every exchange (Table " + str(X["tab_no"]()) + ")."))
    s += table([
        ["Stage", "Plan nodes", "What happens"],
        ["1", "2 to 5", "Scan dim_editor (3 columns) and send it to every executor. The exchange is "
                        "EXECUTOR_BROADCAST, a broadcast rather than a shuffle."],
        ["2", "1, 6 to 12", "Scan fact_revision, only the 13 columns the query needs. Broadcast hash left join "
                            "with dim_editor (node 7), so the big side is not shuffled for the join. Expand "
                            "(node 9) emits each row twice for the distinct counts, then a partial aggregation and "
                            "a shuffle on (wiki_key, date_key, editor_hash, gid)."],
        ["3", "13 to 17", "Merge the partial results per editor, then a partial aggregation by (wiki_key, "
                          "date_key) and a second shuffle on those two columns."],
        ["4", "20 to 23", "Scan fact_page_event (3 columns), partial aggregation, shuffle on (wiki_key, date_key)."],
        ["Result", "18, 19, 24 to 30", "Final aggregations of both sides, full outer shuffled hash join on "
                                       "(wiki_key, date_key), then coalesce to zero, bot_edit_share and net_bytes."],
    ], [0.8, 1.4, 7.4], "Stages of the agg_wiki_daily SELECT, read from Listing 1.")
    s.append(p(
        "<b>Group-by and shuffles.</b> As in the MERGE profile, Spark aggregates in two phases: a partial "
        "aggregation in each task, a shuffle of the much smaller partial results, then the final "
        "aggregation. The two " + c("countDistinct") + " measures add a third level. " + c("PhotonExpand") +
        " (node 9) emits each row twice, once for the plain sums (" + c("gid = 0") + ") and once carrying " +
        c("editor_hash") + " (" + c("gid = 1") + "). Grouping by " + c("(wiki_key, date_key, editor_hash, gid)") +
        " removes duplicate editors, and grouping again by " + c("(wiki_key, date_key)") + " counts what "
        "is left. That extra shuffle (node 11), with twice the rows, is the most expensive part of the plan."))
    s.append(p(
        "<b>Joins.</b> " + c("dim_editor") + " has a few hundred rows, far below the broadcast threshold, "
        "so it is broadcast to the executors and joined there (node 7). The final join is a full outer "
        "join, because a day can have page events but no edits or the other way round. A full outer "
        "join cannot be broadcast, so Spark chose a shuffled hash join (node 26). It needs no new "
        "exchange, because both inputs already leave their aggregations hash-partitioned on " +
        c("(wiki_key, date_key)") + "."))
    s.append(p(
        "<b>Pruning and pushdown.</b> Each scan reads only the columns it needs (" + c("ReadSchema") +
        "): 13 of " + c("fact_revision") + " and 3 of " + c("fact_page_event") + ". This EXPLAIN covers "
        "all days, so it has no filter. In the daily run the " + c("date_key IN (...)") + " predicate is "
        "pushed into the scans. Gold has no Hive partitions, so Delta skips files using the per-file "
        "minimum and maximum of " + c("date_key") + ", the same mechanism that skipped 27 percent of the "
        "files in the MERGE above, and liquid clustering keeps those ranges narrow. True partition "
        "pruning happens earlier in the pipeline: Bronze reads " +
        c("WHERE ingest_date IN (...) AND wiki IN (...)") + " and opens only those folders, and Silver "
        "prunes on " + c("wiki") + "."))
    s.append(p(
        "<b>Adaptive Query Execution.</b> The root node is " + c("AdaptiveSparkPlan") + " with " +
        c("isFinalPlan=false") + ", so this is the plan before it runs. With AQE [8], Spark plans again after "
        "each stage using the real sizes of the shuffle outputs. The plan starts with 16 shuffle "
        "partitions per exchange, and since the result is only a few hundred rows (727 after the daily "
        "run), AQE merges them into a handful of tasks. It could also switch a join to a broadcast or "
        "split a skewed partition if the data called for it. The " + c("Empty Relation") + " in the "
        "MERGE profile is the same mechanism at work: a branch that turned out to be empty at run time "
        "was dropped."))

    # ------------------------------------------------------------------ how to capture
    s += h2("How the profile was captured")
    s += bullets([
        "Open the run page of the Gold task (Jobs and Pipelines, then the run of " + c("04_gold_star") +
        "), or run " + c("notebooks/04_gold_star") + " again.",
        "Under the cell " + c("run = runner.run_gold(...)") + ", open <b>See performance</b>. It lists "
        "every query the cell ran, with its duration.",
        "Click the slowest " + c("result = spark.sql(merge_sql)") + " entry to open its Query Profile, pick "
        "the execution in the drop-down at the top, and switch to the graph view.",
        "Another way in is <b>Query History</b> in the sidebar, filtered to serverless compute and the "
        "time of the run.",
        "On a classic cluster the same graph is in the <b>Spark UI</b>: open the <b>SQL / DataFrame</b> tab, "
        "click the query and look at its <b>DAG</b>. The <b>Stages</b> tab shows one stage per exchange.",
    ])
    return s
