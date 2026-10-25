"""
Text of the Phase 3 report. phase3_report.py turns this into the PDF.
Inline markup is the reportlab paragraph subset (<b>, <i>, <font>, <br/>).
"""
import os

import phase3_costs as K
import phase3_facts as F
from phase3_finops import finops

TITLE = "Wikipedia Knowledge Integrity Lakehouse for Pakistan's Languages"
SUBTITLE = "Phase 3: Final Architecture, Gold Model and FinOps Report"
RUNNING_HEAD = "Wikipedia Knowledge Integrity Lakehouse"
COURSE_LINE = "Data Analysis and Visualization: course project, Phase 3"
AUTHORS = [("Zuraiz Anjum", "23L-2547"), ("Zoraiz Rizwan", "23L-6008")]
TITLE_TABLE = [
    ("Course", "Data Analysis and Visualization"),
    ("Instructor", "Amir Iqbal"),
    ("Repository", "github.com/zuraiz-anjum/pk-wiki-integrity-lakehouse"),
    ("Platform", "Databricks (Spark, Delta Lake, Unity Catalog); costs priced for Azure Databricks"),
    ("Dashboard", "Power BI Desktop, Knowledge Integrity Monitor"),
    ("Data", "Wikimedia MediaWiki history dumps (full load) and the MediaWiki recent changes API (incremental)"),
    ("Date", "26 October 2026"),
]
TITLE_ABSTRACT = (
    "This report describes the finished lakehouse that tracks edits, page events, reverts and "
    "suspected vandalism on the Urdu, Punjabi (Shahmukhi), Sindhi and Pashto Wikipedias. It covers "
    "the end-to-end architecture, the Gold star schema that feeds Power BI, the design decisions "
    "we made and why, how Spark executes the main Gold aggregation, and what the pipeline costs to "
    "run today and at enterprise scale."
)


def c(s):
    """inline code"""
    s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<font name="Courier" size="9.4">{s}</font>'


def money(x, d=2):
    return f"${x:,.{d}f}"


def body(X):
    h1, h2, p, bullets, code, figure, table = (X[k] for k in
                                               ("h1", "h2", "p", "bullets", "code", "figure", "table"))
    FIG, TEXT_W, cm = X["FIG"], X["TEXT_W"], X["cm"]
    s = []

    # =========================================================================== 1
    s += h1("Introduction")
    s.append(p(
        "Wikipedia has active editions in four of Pakistan's languages: Urdu (" + c("urwiki") + "), "
        "Western Punjabi in Shahmukhi script (" + c("pnbwiki") + "), Sindhi (" + c("sdwiki") + ") and "
        "Pashto (" + c("pswiki") + "). Small volunteer communities look after them, and nobody has a "
        "clear, regularly updated picture of how healthy they are. Our project builds that picture. "
        "A Spark pipeline on Databricks follows every edit, new page, deletion and revert, and a Power "
        "BI dashboard turns the result into answers for the communities and for groups that work on "
        "regional languages."))
    s.append(p(
        "Phase 1 was the proposal. In Phase 2 we built the Bronze and Silver layers, the audit "
        "tables and the daily job, and ran them on Databricks. Phase 3 adds the Gold dimensional "
        "model and the dashboard. This report is the final technical record: what the system looks "
        "like end to end, why we built it this way, how Spark runs the heaviest Gold query, and what "
        "it would cost to run for real."))
    s += table([
        ["#", "Business question (from the Phase 1 proposal)", "Main Gold source"],
        ["Q1", "Are the four wikis growing, both in new articles and in active editors?",
         "agg_wiki_daily, agg_editor_monthly"],
        ["Q2", "When someone vandalises a page, how long does it take to undo it? Does it differ for "
               "registered, temporary and IP editors?", "agg_revert_latency, fact_revision"],
        ["Q3", "Which pages are the most fought over, and what are they about?", "agg_edit_wars, dim_page"],
        ["Q4", "How much of each wiki is written by bots rather than people?",
         "agg_wiki_daily, agg_editor_monthly"],
        ["Q5", "Did anonymous editing or vandalism change after IP editing was replaced by temporary "
               "accounts?", "fact_revision, agg_revert_latency, dim_wiki"],
    ], [0.5, 6.2, 3.0], "The five business questions and the Gold tables that answer them.")
    s.append(p(
        "A note on the platform. All of our runs so far used Databricks Free Edition, which is "
        "serverless only and uses Unity Catalog. That keeps the project free, so there is no real "
        "bill. Databricks still records how many DBUs every run used, and Section 6 prices those "
        "measured DBUs at Microsoft's published Azure Databricks rates, so the numbers mean something "
        "for a paying deployment."))

    # =========================================================================== 2
    s += h1("End-to-end architecture")
    s += h2("Overview")
    s.append(p(
        f"Figure {X['fig_no']()} shows the whole system. Two Wikimedia sources feed it. The monthly "
        "MediaWiki history dump [9] is the full load: one bzip2 TSV per wiki with 78 columns and no "
        "header. The MediaWiki recent changes API [10] is the incremental load: our extractor "
        "(" + c("ingestion/ingest_wikimedia.py") + ") asks each wiki for everything since its saved "
        "watermark and writes JSON lines with a small envelope (" + c("_wiki") + ", " + c("_source") +
        ", " + c("_ingested_at") + ", " + c("_window_start") + "). Both land in a Unity Catalog volume "
        "in Hive-style folders, so a folder name already says which load, which snapshot or day, and "
        "which wiki a file belongs to."))
    s.append(p(
        "Bronze keeps the data as it arrived and only adds lineage (" + c("record_hash") + ", " +
        c("source_file") + ", " + c("batch_id") + ", " + c("load_timestamp") + ") and drift markers. "
        "Silver casts every field, unifies the dump and the API into one model, masks personal data, "
        "sends rows that fail a check to quarantine, and keeps SCD Type 2 history for pages and "
        "editors. Gold reshapes Silver into a star schema with two event facts, four aggregate facts "
        "and five dimensions. Power BI connects through a Databricks SQL warehouse and reads the "
        "Gold tables plus the three ops tables for its pipeline health page. Every unit of work in every layer writes one row to " +
        c("pkwiki_ops.pipeline_execution_logs") + "."))

    s.append(X["PageBreak"]())
    s.append(figure(os.path.join(FIG, "architecture_rotated.png"),
                    "End-to-end architecture (page turned sideways). Solid arrows are data flow, numbered "
                    "by the notebook that does the step (01 to 04). Dashed red arrows are audit and "
                    "quarantine writes. The Databricks job runs the four notebooks in order.",
                    max_h=22.2 * cm))
    s += h2("Layers and tables")
    s += table([
        ["Layer", "Table", "Grain", "Physical layout", "Write pattern"],
        ["Bronze", "mediawiki_history", "one dump line", "partitioned by snapshot, wiki",
         "insert-only MERGE on (snapshot, wiki, record_hash)"],
        ["Bronze", "recentchanges", "one API line", "partitioned by ingest_date, wiki",
         "insert-only MERGE on (ingest_date, wiki, record_hash)"],
        ["Silver", "revision", "one edit", "partitioned by wiki",
         "MERGE on (wiki, rev_id); update only when row_hash changes; dump wins over API"],
        ["Silver", "page_event, user_event", "one log event", "partitioned by wiki",
         "MERGE on (wiki, event_key), same rules"],
        ["Silver", "page, editor", "one version (SCD2)", "partitioned by wiki",
         "staged SCD2 MERGE; new version only when attr_hash changes"],
        ["Gold", "dim_date, dim_wiki, dim_change_type", "day / wiki / change kind", "small, unpartitioned",
         "seeded with insert-if-missing MERGE"],
        ["Gold", "dim_page, dim_editor", "one version (SCD2)", "liquid clustering",
         "MERGE from Silver rows newer than the Gold watermark"],
        ["Gold", "fact_revision, fact_page_event", "one edit / one page event", "liquid clustering",
         "MERGE from Silver rows newer than the Gold watermark"],
        ["Gold", "agg_* (4 tables)", "wiki x day, wiki x month, page x day", "small, unpartitioned",
         "recompute touched days or months, MERGE, delete groups that vanished"],
        ["Ops", "pipeline_execution_logs", "one step of one run", "unpartitioned", "append only, on purpose"],
        ["Ops", "schema_drift_log", "one field per batch", "unpartitioned", "MERGE on its key"],
        ["Ops", "quarantine", "one rejected row", "partitioned by source_table", "insert-only MERGE"],
        ["Ops", "gold_watermark", "one Gold table", "unpartitioned", "MERGE after each Gold write"],
    ], [0.8, 2.2, 1.9, 2.0, 3.6], "Every table in the lakehouse with its grain, layout and write pattern.")

    s += h2("Orchestration")
    s.append(p(
        "One Databricks job, " + c("pkwiki-lakehouse-daily") + " (" + c("jobs/pkwiki_pipeline.json") +
        "), runs the notebooks as dependent tasks: " + c("bronze") + ", " + c("silver") + ", " +
        c("scd2") + " and " + c("gold") + ". The definition has no cluster block, so the tasks run on "
        "serverless compute. Its " + c("git_source") + " points at the " + c("main") + " branch, which "
        "means the code that runs is exactly the code that was reviewed and merged. The schedule is "
        "21:00 UTC (02:00 in Pakistan) and the job is created paused, so it only runs daily once we "
        "unpause it. We set " + c("max_concurrent_runs") + " to 1. Two runs MERGEing into the same "
        "tables at the same time would not corrupt Delta, but one of them would fail with a "
        "concurrent-write conflict, and there is no reason to allow it."))
    s.append(p(
        "All tasks receive the same job parameters (" + c("load_type") + ", " + c("snapshot") + ", " +
        c("start_date") + ", " + c("end_date") + ", " + c("wikis") + ", " + c("bronze_mode") + ", " +
        c("run_id") + "). With no dates the run is the standard daily one. With a date range it is a "
        "backfill, and with " + c("load_type=full") + " it loads a monthly snapshot. " + c("run_id") +
        " defaults to " + c("{{job.run_id}}") + ", so all steps of one run share an id in the audit "
        "log. If one wiki fails in Bronze, the other wikis still load, the failure is logged, and the "
        "notebook ends with an error so the job turns red and the later tasks do not start."))

    # =========================================================================== 3
    s += h1("Gold dimensional model")
    s += h2("Star schema")
    s.append(p(
        "Gold is a star schema built for Power BI. There are two event facts at the finest grain, "
        "four aggregate facts for the heavier calculations, and five shared dimensions. " +
        c("fact_revision") + " has one row per edit and carries the measures most questions need: "
        "byte changes, revert flags, " + c("seconds_to_revert") + " and a vandalism-suspect flag (the "
        "edit was reverted within 24 hours by a different editor). " + c("fact_page_event") + " has one "
        "row per page log event such as a creation, move, deletion or protection. The aggregate "
        "facts are pre-computed for the dashboard. " + c("agg_edit_wars") + " keeps every page and day "
        "with at least two reverts, so the report can rank contested pages, and its " +
        c("is_edit_war") + " flag marks the stricter Phase 1 definition of an edit war: three or more "
        "reverts by two or more different editors within the day. On Databricks, 6 of the 18 "
        "contested page-days meet that stricter rule."))
    s.append(p(
        "Some conventions hold across the model. Surrogate keys of the SCD2 dimensions and the event "
        "facts are " + c("bigint") + " values from " + c("xxhash64") + " of the natural key with the sign bit cleared, so a real "
        "key is never negative and can never be mistaken for an unknown member (Section 4.6). The "
        "same row always gets the same key, and a rerun never renumbers anything. Date keys are " +
        c("yyyymmdd") + " integers and month keys are the date key of the first day of the month, so "
        "both join to " + c("dim_date") + ". " + c("dim_date") + " plays two roles: " + c("date_key") +
        " is the UTC day and " + c("pkt_date_key") + " is the Pakistan day, kept for the hour-of-week "
        "heat map that Phase 1 listed as a stretch goal. A fact joins to the version of the page or "
        "editor that was valid at the time of "
        "the edit, not the current one, so a page renamed last week still shows its old title on "
        "old edits. When no version matches, the fact points at the unknown member of its wiki "
        "(keys " + c("-1") + " to " + c("-4") + "), which exists in both SCD2 dimensions, so no fact "
        "row is ever dropped by an inner join. The next run retries those rows and repoints them "
        "once the dimension has caught up. On Databricks, 23.09 percent of " + c("fact_revision") +
        " rows point at an unknown page and none at an unknown editor. All of those rows are dump "
        "revisions from 2002 to 2006 of pages that were later deleted: the dump keeps the edit but "
        "not the page, so the unknown member is the honest answer. Every revision that has a page "
        "id finds its page."))
    s.append(figure(os.path.join(FIG, "star_schema.png"),
                    "The star around fact_revision. Each fact row points at exactly one row in each "
                    "dimension; dim_date is used twice (UTC day and Pakistan day)."))
    s.append(X["CondPageBreak"](12 * cm))
    er = os.path.join(os.path.dirname(FIG), "gold_er_diagram.png")
    if os.path.exists(er):
        s.append(figure(er, "ER diagram of the whole Gold schema: five dimensions (blue), two event facts "
                            "(yellow) and four aggregate facts (green). Lines are foreign keys, coloured by the "
                            "dimension they point to.", max_h=15.5 * cm))
    else:
        s.append(p("<i>[ER diagram not found: expected docs/gold_er_diagram.png]</i>"))

    s += h2("Data dictionary summary")
    s.append(p(
        "Table " + str(X["tab_no"]()) + " summarises every Gold table. The full column list with "
        "types and descriptions is in " + c("docs/gold_data_dictionary.md") + ", which is generated "
        "from the same schema definitions that create the tables, so the two cannot drift apart."))
    s += table([
        ["Table", "Type", "Grain", "Primary key", "Main columns"],
        ["dim_date", "dimension", "calendar day, 2001 to 2030", "date_key",
         "date, year, quarter, month, year_month, day_of_week, is_weekend, is_pk_public_holiday"],
        ["dim_wiki", "dimension", "wiki (4 rows)", "wiki_key",
         "wiki, language_name, language_native, script, site_url, temp_accounts_first_seen_ts"],
        ["dim_page", "SCD2 dimension", "page version", "page_key",
         "page_id, page_title, namespace_name, is_content_page, is_redirect, is_deleted, valid_from, valid_to, is_current"],
        ["dim_editor", "SCD2 dimension", "editor version", "editor_key",
         "editor_hash (masked), editor_type, is_bot, is_anonymous, registration_ts, valid_from, valid_to, is_current"],
        ["dim_change_type", "dimension", "kind of change (12)", "change_type_key",
         "entity (revision or page_event), change_type, description"],
        ["fact_revision", "event fact", "one edit", "revision_key",
         "FKs to all five dims; bytes_added, bytes_removed, seconds_to_revert; is_bot_edit, is_anonymous_edit, "
         "is_new_article, is_revert, is_reverted, is_vandalism_suspect"],
        ["fact_page_event", "event fact", "one page log event", "page_event_key",
         "FKs to all five dims; event_key, log_id, event_ts, event_ts_pkt"],
        ["agg_wiki_daily", "aggregate", "wiki x day", "wiki_key, date_key",
         "edits, bot_edit_share, anonymous_edits, new_articles, pages_deleted, active_editors, reverts, "
         "vandalism_suspects, median_seconds_to_revert"],
        ["agg_revert_latency", "aggregate", "wiki x month x editor type", "wiki_key, month_key, editor_type",
         "reverted_edits, median, p90 and average seconds to revert"],
        ["agg_edit_wars", "aggregate", "contested page x day (2+ reverts)", "wiki_key, page_id, date_key",
         "revert_count, distinct_reverters, reverted_edits, distinct_editors, span_minutes, is_edit_war "
         "(3+ reverts by 2+ editors, the Phase 1 rule; true for 6 of 18 rows)"],
        ["agg_editor_monthly", "aggregate", "wiki x month x human/bot", "wiki_key, month_key, editor_group",
         "editors, active (5+ edits), very active (100+), new_editors, returning_next_month"],
    ], [1.7, 1.2, 1.9, 1.7, 4.6], "Gold data dictionary, summary level.")
    s.append(p(
        "No raw usernames or IP addresses reach Gold. Editors are only known by the salted hash from "
        "Silver, bot accounts keep their public names, and edit summaries are not copied to Gold at "
        "all because the dashboard never needs them."))

    # =========================================================================== 4
    s += h1("Decision log")
    s.append(p(
        "This section records the main design choices and the reasons behind them. Table " +
        str(X["tab_no"]()) + " is the short version; the subsections give the detail and, where it "
        "matters, the alternative we rejected."))
    s += table([
        ["Area", "What we chose", "Main reason"],
        ["Partitioning", "Bronze by (snapshot or ingest_date, wiki); Silver by wiki; Gold not partitioned but "
                         "liquid clustered", "Partitions match the unit of loading and of deletion; Gold is too "
                                             "small for partitions to help"],
        ["Schema drift", "Explicit schemas, all-string parse plus try_cast, drift log and quarantine; schema "
                         "evolution only through a code change, in Bronze and in Gold", "Never lose data and never "
                                                                                       "fail four wikis for one bad file"],
        ["PII", "Salted SHA-256 for editor names, IPs and temporary accounts; regex masking of summaries; salt "
                "in a secret scope", "Distinct-editor counts still work but nobody can be identified"],
        ["Idempotency", "Every write is a MERGE with a change condition, except the append-only audit log",
         "A failed or repeated run can simply be run again"],
        ["History", "SCD Type 2 for page and editor, staged MERGE", "Old titles and old editor types stay "
                                                                    "correct on old edits"],
        ["Incremental", "Extractor watermark per wiki, two newest ingest days in Silver, load_timestamp "
                        "watermark for Gold", "Late files and dump restatements are picked up without "
                                              "full rebuilds"],
        ["Gold refresh", "MERGE for facts and dimensions; recompute only touched days for aggregates",
         "Cost scales with the day's changes, not with history"],
        ["Layout", "Liquid clustering plus OPTIMIZE on Databricks, ZORDER as the fallback on open source Delta",
         "Data skipping on the dashboard filters without small-file problems"],
        ["Surrogate keys", "xxhash64 of the natural key with the sign bit cleared; negative keys only for "
                           "unknown members", "Stable keys without a lookup table; fixes a bug that sent half "
                                              "the facts to unknown members"],
    ], [1.3, 4.6, 3.6], "Decision summary.")

    # ---- 4.1 partitioning
    s += h2("Partition columns")
    s.append(p(
        "<b>Bronze.</b> " + c("bronze.mediawiki_history") + " is partitioned by " + c("snapshot") +
        " and " + c("wiki") + ", and " + c("bronze.recentchanges") + " by " + c("ingest_date") + " and " +
        c("wiki") + ". These are exactly the folder levels in the landing volume, and each " +
        "(day or snapshot, wiki) pair is one unit of work in the code. Three things follow from that. "
        "The insert-only MERGE includes the partition in its condition (" +
        c("ON t.record_hash = s.record_hash AND t.ingest_date = '2026-09-27' AND t.wiki = 'urwiki'") +
        "), so Delta only reads the files of that one partition to find duplicates. A rebuild after a "
        "schema change is a cheap " + c("DELETE") + " of whole partitions followed by a normal load. And "
        "a broken file for one wiki cannot touch the data of another."))
    s.append(p(
        "There is a cost we accept. Pashto and Punjabi produce only a handful of API events per day "
        "(4 and 6 in our sample day), so their daily partitions hold tiny files. At our volume this is "
        "harmless, and a weekly " + c("OPTIMIZE") + " compacts them. At a much larger scale we would "
        "move Bronze to liquid clustering on the same columns and keep the partition filter as a "
        "clustering filter."))
    s.append(p(
        "<b>Silver.</b> The Silver tables are partitioned by " + c("wiki") + " only. Every Silver "
        "MERGE key starts with " + c("wiki") + ", the monthly handover deletes by wiki, and almost "
        "every dashboard query filters by wiki. We considered partitioning by " + c("rev_date") +
        " and rejected it. Revisions go back to 2002, so date partitions would mean thousands of "
        "small partitions, and the MERGE keys " + c("(wiki, rev_id)") + " do not contain a date, so "
        "the MERGE could not prune by date anyway."))
    s.append(p(
        "<b>Gold.</b> Gold has no Hive-style partitions. Even with the full dumps loaded, the "
        "largest Gold table is a few million rows, and Databricks recommends against partitioning "
        "tables much below a terabyte because it mostly produces small files [5]. Instead the two facts "
        "and the two SCD2 dimensions use liquid clustering on the columns that dashboard filters and joins use "
        "(" + F.CLUSTER_TEXT + "). Clustering keys can be changed later without rewriting the table, "
        "which partitions cannot. " + c("ops.quarantine") + " is partitioned by " + c("source_table") +
        " because a replay always works on one source at a time."))

    # ---- 4.2 schema drift
    s += h2("Schema drift handling")
    s.append(p(
        "We do not control the Wikimedia formats, so the rule is: keep the data, log the change, "
        "quarantine what does not fit, and never fail the whole run because of one bad file. Nothing "
        "uses " + c("inferSchema") + ". Detection happens in Bronze:"))
    s += bullets([
        "The dump is read with an explicit 78-column string schema in " + c("PERMISSIVE") + " mode. A line "
        "that does not split into 78 fields goes to " + c("_corrupt_record") + " and the drift log gets a " +
        c("column_count") + " entry such as \"79 fields instead of 78\".",
        "Each API line is parsed first with every field as a string, then each field is " + c("try_cast") +
        " to its declared type. A field that had a value before the cast and is null after it had the wrong "
        "type, so we know exactly which field broke (" + c("type_mismatch") + ").",
        "Keys in the JSON that are not in our schema are found with " + c("json_object_keys") + " and stored "
        "in " + c("unknown_fields") + " (" + c("new_field") + " in the drift log). The full original line "
        "stays in " + c("raw_json") + ", so nothing is lost.",
        "Lines that are not valid JSON at all are kept with a null record and logged as " +
        c("malformed_json") + ".",
    ])
    s.append(p(
        "Silver then quarantines any row that is malformed, has a type mismatch, has a wiki that does "
        "not match its folder, or lacks a field we depend on (timestamp, revision id, page id, byte "
        "count). " + c("ops.quarantine") + " keeps the list of reasons and the raw line, so a row can "
        "be replayed once the schema is fixed. We proved this with a small set of deliberately broken "
        "Pashto files in " + c("sample_data/drift_demo") + ", run into a throwaway schema prefix so "
        "the real tables stay clean:"))
    s += table([
        ["Broken input", "What the pipeline did"],
        ["New field rc_quality_score in an API record", "Row loaded normally; field kept in raw_json; new_field row in the drift log"],
        ["revid: \"abc123\" and oldlen: \"big\"", "type_mismatch entries for revid and oldlen; both rows quarantined"],
        ["JSON line cut off halfway", "malformed_json in the drift log; row quarantined"],
        ["Two dump rows with a 79th column, one with only 40", "column_count entries; 3 rows quarantined"],
        ["revision_id = not_a_number in the dump", "Quarantined in Silver as \"bad revision_id\""],
        ["<b>Total</b>", "<b>6 drift log rows, 7 quarantined rows; the good rows from the same files (1 dump revision, "
                         "3 API revisions, 2 page events) reached Silver; every step SUCCESS; a second run added nothing</b>"],
    ], [3.4, 6.0], "Drift demo results, identical on the local runner and on Databricks.")
    s.append(p(
        "Handling a new field properly is a deliberate step. We add it to " + c("pipeline/schemas.py") +
        ". On the next run " + c("evolve_schema()") + " compares the incoming DataFrame with the table "
        "and runs " + c("ALTER TABLE ... ADD COLUMNS") + " for anything missing, nested fields included. "
        "That is the same effect as " + c("mergeSchema") + ", done by hand because we write with MERGE. "
        "To fill the new column for past days we rerun those days with " + c("bronze_mode=rebuild") +
        ", which re-reads the raw files. We chose not to switch on automatic schema merging. With it, a "
        "single malformed record carrying a misspelt key would silently add a permanent column to a "
        "table. We would rather see the field in the drift log and decide."))
    s.append(p(
        "Gold uses the same mechanism for its own changes. " + c("merge_gold()") + " calls " +
        c("evolve_schema()") + " before every MERGE, so a column we add to a Gold table definition "
        "appears on the next run without dropping or recreating the table. We used this ourselves late "
        "in Phase 3: " + c("agg_edit_wars") + " gained " + c("reverted_edits") + " and " +
        c("is_edit_war") + " when we widened it from strict edit wars to all contested page-days. The "
        "existing table got the two columns through " + c("ALTER TABLE ... ADD COLUMNS") + " inside "
        "the next MERGE, and a full Gold refresh (" + c("load_type=full") + ") filled them for every "
        "day: " + c("agg_edit_wars") + " went from 1 row to 18. Power BI only ever sees new columns, "
        "never renamed or retyped ones, so the report does not break."))

    # ---- 4.3 PII
    s += h2("PII management")
    s.append(p(
        "The data really does contain personal information. Until 2025, logged-out editors were shown "
        "by their IP address, and 874 of the 20,000 rows in our dump sample (4.4 percent) had a raw IP "
        "as the editor. Registered usernames are often real names. Newer logged-out edits come from "
        "temporary accounts such as " + c("~2026-12345-6") + ". All rules live in one place, " +
        c("common/pii_policy.py") + ", and " + c("pipeline/pii.py") + " applies the same regexes and "
        "the same hash in Spark during the Bronze to Silver step."))
    s += table([
        ["Field", "Treatment", "Result"],
        ["Registered username", "salted SHA-256, first 16 hex characters", "user_&lt;hash&gt;"],
        ["IP address (older logged-out edits)", "salted SHA-256", "ip_&lt;hash&gt;"],
        ["Temporary account name", "salted SHA-256", "tmp_&lt;hash&gt;"],
        ["Bot and system accounts", "kept: public service accounts that must carry \"bot\" in the name", "name unchanged"],
        ["User and User talk page titles", "user part hashed", "User:user_&lt;hash&gt;"],
        ["Edit summaries", "IPs, e-mail addresses, User: and Special:Contributions links replaced by regex",
         "[ip], [email], [user]; not copied to Gold"],
        ["Block and rename targets", "hashed like editors", "target_key"],
    ], [2.6, 4.2, 2.6], "PII rules applied before data reaches Silver.")
    s.append(p(
        "Hashing, not encryption, is the right tool here because we never need the original value "
        "back. The salt matters. There are only about 4.3 billion IPv4 addresses, so an unsalted hash "
        "of an IP can be reversed by brute force in minutes. The salt is a long random string in the "
        "Databricks secret scope " + c("pkwiki/pii_salt") + "; it is never in the code or on GitHub. "
        "Because the hash is deterministic, the same editor gets the same key in the dump and in the "
        "API, so distinct-editor counts, retention and revert-by-a-different-editor all still work. "
        "Truncating to 64 bits keeps keys short; with even a million distinct editors the chance of "
        "any collision is about 3 in 100 million."))
    s.append(p(
        "Raw values exist only in Bronze, which only the two of us can read. While building Phase 2 we "
        "also found and fixed a leak in our own Phase 1 rules: the API's " + c("bot") + " flag is set "
        "for humans who use the flood flag, so treating it as \"bot account\" kept a few real usernames "
        "unhashed. Now only accounts with \"bot\" in the name and a short list of system accounts keep "
        "their names, and the published sample files were masked again. A check in " +
        c("99_checks") + " confirms every editor key is a hash or a bot name and that no IP pattern is "
        "left in any masked summary."))

    # ---- 4.4 merge scd2 watermark
    s += h2("Idempotent MERGE, SCD Type 2 and incremental watermarking")
    s.append(p(
        "<b>Idempotency.</b> Every table except the audit log is written with " + c("MERGE INTO") +
        " [6], and every MERGE has a condition that makes a repeat a no-op. Bronze only inserts lines whose " +
        c("record_hash") + " is not yet in the partition. Silver updates a row only when its " +
        c("row_hash") + " (a hash of all source attributes) changed, and inserts only new keys. Revert "
        "flags only ever move from false to true. Duplicates inside one batch are removed with a window "
        "function before the MERGE, preferring the dump over the API and then the newest row. We "
        "tested this on Databricks by running the full load and the daily load twice: the second runs "
        "inserted, updated and deleted zero rows in every layer."))
    s.append(p(
        "<b>Dump and API together.</b> The dump is rebuilt every month and Wikimedia warns against "
        "stitching dumps together. So a full load of snapshot " + c("2026-08") + " takes over everything "
        "before 1 September for the wikis in the run. The Silver MERGE adds " +
        c("WHEN NOT MATCHED BY SOURCE AND t.source = 'api' AND t.wiki IN (...) AND t.rev_ts < cutoff THEN DELETE") +
        ". API rows the dump also has are replaced by the dump version, API rows before the cutoff that "
        "the dump does not have are removed, and API rows after the cutoff stay. This is the only "
        "place the pipeline deletes rows. Deleted pages are never removed; they get " +
        c("is_deleted = true") + "."))
    s.append(p(
        "<b>SCD Type 2.</b> " + c("silver.page") + " tracks title, namespace, redirect and deleted; " +
        c("silver.editor") + " tracks editor type and bot status. For each batch we build a timeline of "
        "observations per key, forward-fill attributes the source did not send, hash them into " +
        c("attr_hash") + ", and keep only the rows where the hash changes. The write is the usual staged "
        "MERGE: a changed key appears twice in the source, once with a merge key that matches and "
        "closes the current version (" + c("valid_to") + ", " + c("is_current = false") + ") and once "
        "with a null merge key that can never match and is inserted as the new current version. A new "
        "version is only written when the hash differs and the change is newer than the current "
        "version, which keeps reruns and late batches safe. The checks notebook confirms exactly one "
        "current row per key and no gaps or overlaps between versions."))
    s.append(p(
        "<b>Watermarking</b> works at three levels. The extractor keeps a per-wiki watermark in " +
        c("watermarks.json") + " and asks the API for " + c("rcstart = watermark") + " to " +
        c("rcend = now") + ". If two windows overlap, the duplicate lines have the same " +
        c("record_hash") + " and Bronze ignores them. The standard daily run then processes the two "
        "newest " + c("ingest_date") + " partitions, not one: re-reading yesterday costs almost nothing "
        "(the MERGE inserts zero rows) and catches files that landed after yesterday's run. Gold keeps "
        "its own watermark per table in " + c("ops.gold_watermark") + ", set to the highest Silver " +
        c("load_timestamp") + " it has consumed. We use the load time and not the event time on "
        "purpose. A dump restatement changes rows with old event times, but every changed Silver row "
        "gets a new " + c("load_timestamp") + ", while unchanged rows are not touched at all. So the "
        "watermark picks up restatements that an event-time watermark would miss, and nothing else."))

    # ---- 4.5 gold merge + optimisation
    s += h2("Gold MERGE strategy and table optimisation")
    s += F.gold_strategy_paragraphs(X, c)

    # =========================================================================== 5
    s += F.dag_section(X, c)

    # =========================================================================== 6
    s += finops(X)

    # =========================================================================== 7
    s += h1("Conclusion and future work")
    s.append(p(
        "We set out to build a lakehouse that could tell the Urdu, Punjabi, Sindhi and Pashto "
        "Wikipedia communities whether their projects are growing, how quickly vandalism is cleaned "
        "up, where edit wars happen and how much is written by bots. The finished pipeline does this "
        "from two very different sources. It keeps raw data intact in Bronze, merges and masks it in "
        "Silver, and serves a star schema from Gold. Every write is idempotent, every step is audited, "
        "and schema changes end up in a log instead of a failed job. The hardest part was the monthly "
        "handover between dump and API, and it is handled by one MERGE with a bounded delete."))
    d, rows = K.daily_run(), K.enterprise_rows()
    s.append(p(
        "The cost picture is clear. At our real size the daily run costs about " + money(d["classic"]) +
        " on a job cluster or " + money(d["serverless"]) + " on serverless, and nothing on Free Edition. "
        "Run every hour for a year, the same pipeline costs " + money(rows[2]["total"], 0) + " to " +
        money(rows[1]["total"], 0) + " on job clusters and " + money(rows[4]["total"], 0) + " to " +
        money(rows[3]["total"], 0) + " on serverless jobs, against " + money(rows[0]["total"], 0) +
        " for an always-on interactive cluster. The billing data also taught us that our runs are "
        "dominated by fixed overhead, so the cheapest levers are the ones that cut idle time and per-run overhead, not a faster "
        "engine."))
    s.append(p("The main items left for future work:"))
    s += bullets([
        "Move the extractor into the job as a first task, with its watermark file in the volume, so "
        "nothing runs on a laptop.",
        "Load the real all-time dumps for all four wikis (about 5 to 10 million events) and repeat the "
        "timing and cost measurements at that size.",
        "Add the DB-IP Lite country lookup for old IP edits from the proposal, then drop the IP hash "
        "entirely for those rows.",
        "Automate deletion of raw landing files 30 days after processing, as the proposal promised.",
        "Replace the daily API pull with the Wikimedia EventStreams feed and Structured Streaming for "
        "near real-time Gold, using the same MERGE logic in " + c("foreachBatch") + ".",
        "Alert on FAILED rows in the execution log and on new drift log entries, and track spend "
        "with the " + c("system.billing.usage") + " table once we are on a paid workspace.",
    ])

    # =========================================================================== refs
    s += h1("References")
    refs = [
        "Microsoft. Azure Databricks pricing. https://azure.microsoft.com/en-us/pricing/details/databricks/ "
        "(instance DBU ratings and DBU rates; accessed 25 October 2026).",
        "Microsoft. Azure Retail Prices REST API. https://prices.azure.com/api/retail/prices "
        "(service 'Azure Databricks' and 'Virtual Machines', region eastus; queried 25 October 2026).",
        "Microsoft. Linux Virtual Machines pricing. https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/.",
        "Microsoft. Azure Blob Storage pricing. https://azure.microsoft.com/en-us/pricing/details/storage/blobs/.",
        "Databricks. Use liquid clustering for tables. https://docs.databricks.com/en/delta/clustering.html.",
        "Databricks. Upsert into a Delta Lake table using merge. https://docs.databricks.com/en/delta/merge.html.",
        "Databricks. Query profile. https://docs.databricks.com/en/sql/user/queries/query-profile.html.",
        "Apache Spark. Performance tuning: Adaptive Query Execution. https://spark.apache.org/docs/latest/sql-performance-tuning.html.",
        "Wikimedia. MediaWiki history dumps. https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Edits/MediaWiki_history_dumps.",
        "MediaWiki. API:RecentChanges. https://www.mediawiki.org/wiki/API:RecentChanges.",
    ]
    for i, r in enumerate(refs, 1):
        s.append(X["Paragraph"](f"[{i}] {r}", X["ST"]["note"]))
    return s


