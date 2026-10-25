"""
Every schema the pipeline uses, written out by hand. Nothing is inferred.

Raw read schemas  -> what we expect the source files to look like (schema-on-read)
Table schemas     -> the contract for each Delta table (00_setup creates them from these)

Column comments end up in the table metadata on Databricks and in the README data dictionary
(scripts/print_data_dictionary.py).
"""
from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType, IntegerType, LongType,
                               MapType, StringType, StructField, StructType, TimestampType)

S, L, I, B, T, D = StringType(), LongType(), IntegerType(), BooleanType(), TimestampType(), DateType()
ARR = ArrayType(StringType())


def f(name, dtype, comment="", nullable=True):
    return StructField(name, dtype, nullable, {"comment": comment} if comment else {})


# --------------------------------------------------------------------------------------
# Raw: MediaWiki history dump. TSV, no header, 78 columns, order from
# https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Edits/MediaWiki_history_dumps
# Everything is read as string in Bronze; the second element is the type we cast to in Silver.
# --------------------------------------------------------------------------------------
HISTORY_COLUMNS = [
    ("wiki_db", "string"), ("event_log_id", "bigint"), ("event_entity", "string"),
    ("event_type", "string"), ("event_timestamp", "timestamp"), ("event_comment", "string"),
    ("event_user_id", "bigint"), ("event_user_central_id", "bigint"),
    ("event_user_text_historical", "string"), ("event_user_text", "string"),
    ("event_user_blocks_historical", "array"), ("event_user_blocks", "array"),
    ("event_user_groups_historical", "array"), ("event_user_groups", "array"),
    ("event_user_is_bot_by_historical", "array"), ("event_user_is_bot_by", "array"),
    ("event_user_is_created_by_self", "boolean"), ("event_user_is_created_by_system", "boolean"),
    ("event_user_is_created_by_peer", "boolean"), ("event_user_is_anonymous", "boolean"),
    ("event_user_is_temporary", "boolean"), ("event_user_is_permanent", "boolean"),
    ("event_user_is_cross_wiki", "boolean"), ("event_user_registration_timestamp", "timestamp"),
    ("event_user_creation_timestamp", "timestamp"), ("event_user_first_edit_timestamp", "timestamp"),
    ("event_user_revision_count", "bigint"), ("event_user_seconds_since_previous_revision", "bigint"),
    ("page_id", "bigint"), ("page_title_historical", "string"), ("page_title", "string"),
    ("page_namespace_historical", "int"), ("page_namespace_is_content_historical", "boolean"),
    ("page_namespace", "int"), ("page_namespace_is_content", "boolean"),
    ("page_is_redirect", "boolean"), ("page_is_deleted", "boolean"),
    ("page_creation_timestamp", "timestamp"), ("page_first_edit_timestamp", "timestamp"),
    ("page_revision_count", "bigint"), ("page_seconds_since_previous_revision", "bigint"),
    ("user_id", "bigint"), ("user_central_id", "bigint"), ("user_text_historical", "string"),
    ("user_text", "string"), ("user_blocks_historical", "array"), ("user_blocks", "array"),
    ("user_groups_historical", "array"), ("user_groups", "array"),
    ("user_is_bot_by_historical", "array"), ("user_is_bot_by", "array"),
    ("user_is_created_by_self", "boolean"), ("user_is_created_by_system", "boolean"),
    ("user_is_created_by_peer", "boolean"), ("user_is_anonymous", "boolean"),
    ("user_is_temporary", "boolean"), ("user_is_permanent", "boolean"),
    ("user_registration_timestamp", "timestamp"), ("user_creation_timestamp", "timestamp"),
    ("user_first_edit_timestamp", "timestamp"), ("revision_id", "bigint"),
    ("revision_parent_id", "bigint"), ("revision_minor_edit", "boolean"),
    ("revision_deleted_parts", "array"), ("revision_deleted_parts_are_suppressed", "boolean"),
    ("revision_text_bytes", "bigint"), ("revision_text_bytes_diff", "bigint"),
    ("revision_text_sha1", "string"), ("revision_content_model", "string"),
    ("revision_content_format", "string"), ("revision_is_deleted_by_page_deletion", "boolean"),
    ("revision_deleted_by_page_deletion_timestamp", "timestamp"),
    ("revision_is_identity_reverted", "boolean"),
    ("revision_first_identity_reverting_revision_id", "bigint"),
    ("revision_seconds_to_identity_revert", "bigint"), ("revision_is_identity_revert", "boolean"),
    ("revision_is_from_before_page_creation", "boolean"), ("revision_tags", "array"),
]
assert len(HISTORY_COLUMNS) == 78
HISTORY_COLUMN_NAMES = [c for c, _ in HISTORY_COLUMNS]

HISTORY_RAW_SCHEMA = StructType(
    [f(c, S, f"raw text, cast to {t} in silver") for c, t in HISTORY_COLUMNS]
    + [f("_corrupt_record", S, "whole line when it doesn't have exactly 78 fields")]
)

# --------------------------------------------------------------------------------------
# Raw: recent changes API, one JSON object per line: our envelope + the untouched record.
# Field list taken from the rcprop we request (see ingestion/ingest_wikimedia.py).
# --------------------------------------------------------------------------------------
RC_RECORD_SCHEMA = StructType([
    f("type", S, "edit | new | log | categorize"),
    f("ns", I, "namespace number"),
    f("title", S, "page title with localized namespace prefix"),
    f("pageid", L), f("revid", L), f("old_revid", L), f("rcid", L, "recent changes id"),
    f("user", S, "editor name (masked in samples)"), f("userid", L),
    f("bot", B, "edit was flagged as a bot edit"), f("new", B), f("minor", B),
    f("anon", B, "logged out IP edit (pre temporary accounts)"),
    f("temp", B, "temporary account"),
    f("oldlen", L), f("newlen", L),
    f("timestamp", S, "ISO-8601 UTC, e.g. 2026-09-27T08:24:57Z"),
    f("comment", S), f("redirect", B), f("tags", ARR),
    f("sha1", S, "hex sha1 of the revision text"),
    f("logid", L), f("logtype", S), f("logaction", S),
    f("logparams", MapType(S, S), "varies by log type, nested values kept as json text"),
])
RC_ENVELOPE_FIELDS = ["_wiki", "_source", "_ingested_at", "_window_start", "record"]
RC_LINE_SCHEMA = StructType([
    f("_wiki", S), f("_source", S), f("_ingested_at", S), f("_window_start", S),
    f("record", RC_RECORD_SCHEMA),
])


def loose(schema):
    """Same field names, every leaf as string. Spark keeps the raw json text for non-string
    values, so we can try_cast afterwards and see exactly which field broke."""
    out = []
    for fld in schema.fields:
        dtype = loose(fld.dataType) if isinstance(fld.dataType, StructType) else S
        out.append(StructField(fld.name, dtype, True))
    return StructType(out)


RC_LINE_SCHEMA_LOOSE = loose(RC_LINE_SCHEMA)


# --------------------------------------------------------------------------------------
# Bronze tables
# --------------------------------------------------------------------------------------
BRONZE_HISTORY = StructType(
    list(HISTORY_RAW_SCHEMA.fields) + [
        f("record_hash", S, "sha256 of the raw line, used to make bronze inserts idempotent", False),
        f("source_file", S, "file the row came from"),
        f("snapshot", S, "dump month YYYY-MM (partition)", False),
        f("wiki", S, "wiki folder the file was in (partition)", False),
        f("batch_id", S, "run id that loaded the row"),
        f("load_timestamp", T, "when this row was written to bronze", False),
    ])

BRONZE_RC = StructType([
    f("env_wiki", S, "_wiki from the envelope"),
    f("source_api", S, "api endpoint the record came from"),
    f("ingested_at", S, "when the extractor fetched it (raw string)"),
    f("window_start", S, "watermark the extractor started from"),
    f("record", RC_RECORD_SCHEMA, "the api record with our explicit types"),
    f("raw_json", S, "the original line, nothing dropped (new fields survive here)"),
    f("_corrupt_record", S, "set when the line isn't valid json or a field has the wrong type"),
    f("type_mismatch_fields", ARR, "fields whose value didn't fit the declared type"),
    f("unknown_fields", ARR, "fields in the json that aren't in our schema yet"),
    f("record_hash", S, "sha256 of the raw line", False),
    f("source_file", S),
    f("ingest_date", S, "YYYY-MM-DD folder (partition)", False),
    f("wiki", S, "wiki folder (partition)", False),
    f("batch_id", S),
    f("load_timestamp", T, "when this row was written to bronze", False),
])

# --------------------------------------------------------------------------------------
# Silver tables
# --------------------------------------------------------------------------------------
_EDITOR_TYPES = "registered | temporary | ip | bot"

SILVER_REVISION = StructType([
    f("wiki", S, "urwiki, pnbwiki, sdwiki, pswiki", False),
    f("rev_id", L, "revision id (PK with wiki)", False),
    f("page_id", L), f("parent_rev_id", L, "previous revision of the page, 0 for page creation"),
    f("rev_ts", T, "edit time UTC", False),
    f("rev_ts_pkt", T, "edit time in Pakistan time, for the dashboard"),
    f("rev_date", D, "UTC date of rev_ts"),
    f("page_title", S, "title at the time of the edit, no namespace prefix, user pages masked"),
    f("page_namespace", I), f("page_is_redirect", B),
    f("editor_key", S, "salted hash of the editor (bot names kept)"),
    f("editor_type", S, _EDITOR_TYPES),
    f("is_bot", B), f("editor_registration_ts", T, "only known from the dump"),
    f("bytes", L, "page size after the edit"), f("bytes_diff", L),
    f("sha1", S, "hex sha1 of the text (dump base36 is converted)"),
    f("is_minor", B), f("is_page_creation", B),
    f("comment_masked", S, "edit summary with IPs, emails and user links masked"),
    f("tags", ARR),
    f("is_revert", B, "this edit restored an earlier version"),
    f("is_reverted", B, "this edit was later undone"),
    f("reverted_by_rev_id", L), f("seconds_to_revert", L),
    f("is_deleted", B, "page or revision deleted (soft delete, row is kept)"),
    f("source", S, "dump | api", False),
    f("source_ref", S, "snapshot for dump rows, ingest_date for api rows"),
    f("row_hash", S, "hash of the source attributes, MERGE only updates when it changes"),
    f("batch_id", S, "run that last wrote the row"),
    f("load_timestamp", T, "when silver last wrote this row", False),
])

SILVER_PAGE_EVENT = StructType([
    f("wiki", S, "", False),
    f("event_key", S, "log:<log_id>:<event_type> when there is a log id, otherwise h:<hash> (PK with wiki)", False),
    f("log_id", L), f("page_id", L),
    f("event_type", S, "create | move | delete | restore | protect | upload | merge | other:*"),
    f("event_ts", T, "", False), f("event_ts_pkt", T),
    f("page_namespace", I), f("page_title", S, "title the event happened on"),
    f("new_title", S, "target title for moves"),
    f("editor_key", S), f("editor_type", S, _EDITOR_TYPES),
    f("comment_masked", S),
    f("source", S, "dump | api", False), f("source_ref", S),
    f("row_hash", S), f("batch_id", S), f("load_timestamp", T, "", False),
])

SILVER_USER_EVENT = StructType([
    f("wiki", S, "", False),
    f("event_key", S, "log:<log_id>:<event_type> or h:<hash> (PK with wiki)", False),
    f("log_id", L),
    f("event_type", S, "create | rename | block | unblock | altergroups | alterblocks"),
    f("event_ts", T, "", False), f("event_ts_pkt", T),
    f("performer_key", S, "who did it (hashed)"), f("performer_type", S, _EDITOR_TYPES),
    f("target_key", S, "account it was done to (hashed)"), f("target_type", S, _EDITOR_TYPES),
    f("block_duration", S, "as given by the api, e.g. '1 week', 'infinity'"),
    f("block_flags", ARR), f("user_groups", ARR, "groups after the change"),
    f("comment_masked", S),
    f("source", S, "dump | api", False), f("source_ref", S),
    f("row_hash", S), f("batch_id", S), f("load_timestamp", T, "", False),
])

SILVER_PAGE = StructType([
    f("page_sk", S, "surrogate key, one per version", False),
    f("wiki", S, "", False), f("page_id", L, "", False),
    f("page_title", S), f("page_namespace", I), f("is_redirect", B), f("is_deleted", B),
    f("attr_hash", S, "hash of the tracked attributes"),
    f("valid_from", T, "", False), f("valid_to", T, "null while current"),
    f("is_current", B, "", False),
    f("batch_id", S), f("load_timestamp", T, "", False),
])

SILVER_EDITOR = StructType([
    f("editor_sk", S, "surrogate key, one per version", False),
    f("wiki", S, "", False), f("editor_key", S, "", False),
    f("editor_type", S, _EDITOR_TYPES), f("is_bot", B),
    f("registration_ts", T, "from the dump, null for api-only editors"),
    f("attr_hash", S),
    f("valid_from", T, "first edit seen in this state", False), f("valid_to", T),
    f("is_current", B, "", False),
    f("batch_id", S), f("load_timestamp", T, "", False),
])

# --------------------------------------------------------------------------------------
# Gold tables (star schema, see docs/gold_data_dictionary.md)
# Surrogate keys are xxhash64 of the natural key, -1 is the unknown member.
# --------------------------------------------------------------------------------------
DBL = DoubleType()

GOLD_DIM_DATE = StructType([
    f("date_key", I, "yyyymmdd, e.g. 20260927", False),
    f("date", D, "calendar day", False),
    f("year", I), f("quarter", I), f("month", I),
    f("month_name", S, "January ... December"),
    f("year_month", S, "YYYY-MM"),
    f("month_start_date", D, "first day of the month"),
    f("day_of_month", I),
    f("day_of_week", I, "1 = Monday ... 7 = Sunday"),
    f("day_name", S, "Monday ... Sunday"),
    f("week_of_year", I, "ISO week"),
    f("is_weekend", B, "Saturday or Sunday"),
    f("is_pk_public_holiday", B, "fixed-date Pakistani public holiday (Eid and other lunar holidays not included)"),
    f("holiday_name", S, "name of the holiday, null on normal days"),
    f("load_timestamp", T, "when this row was written", False),
])

GOLD_DIM_WIKI = StructType([
    f("wiki_key", I, "small fixed code, 1 to 4", False),
    f("wiki", S, "urwiki, pnbwiki, sdwiki, pswiki", False),
    f("language_name", S, "Urdu, Punjabi (Shahmukhi), Sindhi, Pashto"),
    f("language_native", S, "language name in its own script"),
    f("language_code", S, "ur, pnb, sd, ps"),
    f("script", S, "writing system"),
    f("site_url", S),
    f("temp_accounts_first_seen_ts", T, "first edit by a temporary account we have for this wiki (rollout as seen in the data)"),
    f("load_timestamp", T, "when this row was last written", False),
])

GOLD_DIM_PAGE = StructType([
    f("page_key", L, "surrogate key, xxhash64 of page_sk, -1 to -4 = unknown page of wiki 1 to 4", False),
    f("page_sk", S, "silver.page key of this version (lineage)", False),
    f("wiki_key", I, "which wiki", False),
    f("wiki", S, "", False),
    f("page_id", L, "MediaWiki page id", False),
    f("page_title", S, "title in this version (user pages masked)"),
    f("page_namespace", I, "namespace number"),
    f("namespace_name", S, "Article, Talk, User, Project, File, Template, Help, Category, Portal, Other"),
    f("is_content_page", B, "namespace 0 (articles)"),
    f("is_redirect", B), f("is_deleted", B, "deleted in this version (soft delete)"),
    f("valid_from", T, "", False), f("valid_to", T, "null while current"),
    f("is_current", B, "", False),
    f("row_hash", S, "hash of the columns above, MERGE only updates when it changes"),
    f("batch_id", S, "run that last wrote the row"),
    f("load_timestamp", T, "when gold last wrote this row", False),
])

GOLD_DIM_EDITOR = StructType([
    f("editor_key", L, "surrogate key, xxhash64 of editor_sk, -1 to -4 = unknown editor of wiki 1 to 4", False),
    f("editor_sk", S, "silver.editor key of this version (lineage)", False),
    f("wiki_key", I, "which wiki", False),
    f("wiki", S, "", False),
    f("editor_hash", S, "salted hash user_/ip_/tmp_<hash>, bot accounts keep their public name", False),
    f("editor_type", S, _EDITOR_TYPES),
    f("is_bot", B), f("is_anonymous", B, "ip or temporary account"),
    f("registration_ts", T, "from the dump, null for api-only editors"),
    f("first_seen_ts", T, "start of the first version we have for this editor"),
    f("valid_from", T, "", False), f("valid_to", T, "null while current"),
    f("is_current", B, "", False),
    f("row_hash", S), f("batch_id", S), f("load_timestamp", T, "", False),
])

GOLD_DIM_CHANGE_TYPE = StructType([
    f("change_type_key", I, "small fixed code", False),
    f("entity", S, "revision | page_event", False),
    f("change_type", S, "revision: create, revert, minor, edit. page_event: create, move, delete, ...", False),
    f("description", S),
    f("load_timestamp", T, "", False),
])

GOLD_FACT_REVISION = StructType([
    f("revision_key", L, "xxhash64 of wiki|rev_id", False),
    f("wiki_key", I, "which wiki", False),
    f("date_key", I, "UTC day of the edit", False),
    f("pkt_date_key", I, "Pakistan day of the edit", False),
    f("page_key", L, "page version valid at the edit, unknown member if not found", False),
    f("editor_key", L, "editor version valid at the edit, unknown member if not found", False),
    f("change_type_key", I, "create, revert, minor or edit", False),
    f("rev_id", L, "revision id (degenerate)", False),
    f("page_id", L), f("parent_rev_id", L),
    f("reverted_by_rev_id", L, "revision that undid this one"),
    f("rev_ts", T, "edit time UTC", False), f("rev_ts_pkt", T, "edit time Pakistan"),
    f("rev_hour_pkt", I, "hour of day in Pakistan time, 0 to 23"),
    f("bytes", L, "page size after the edit"), f("bytes_diff", L),
    f("bytes_added", L, "bytes_diff when positive, else 0"),
    f("bytes_removed", L, "minus bytes_diff when negative, else 0"),
    f("seconds_to_revert", L, "how long the edit survived, null if not reverted"),
    f("is_bot_edit", B), f("is_anonymous_edit", B, "made by an ip or temporary account"),
    f("is_minor", B), f("is_page_creation", B),
    f("is_new_article", B, "page creation in namespace 0 that is not a redirect"),
    f("is_revert", B, "this edit restored an earlier version"),
    f("is_reverted", B, "this edit was later undone"),
    f("is_vandalism_suspect", B, "reverted within 24 hours by a different editor"),
    f("is_deleted", B),
    f("source", S, "dump | api"),
    f("row_hash", S), f("batch_id", S), f("load_timestamp", T, "", False),
])

GOLD_FACT_PAGE_EVENT = StructType([
    f("page_event_key", L, "xxhash64 of wiki|event_key", False),
    f("wiki_key", I, "which wiki", False),
    f("date_key", I, "UTC day", False),
    f("pkt_date_key", I, "Pakistan day", False),
    f("page_key", L, "-1 if unknown", False),
    f("editor_key", L, "who did it, unknown member if not found", False),
    f("change_type_key", I, "kind of change", False),
    f("event_key", S, "silver event key (degenerate)", False),
    f("log_id", L), f("page_id", L), f("page_namespace", I),
    f("event_ts", T, "", False), f("event_ts_pkt", T),
    f("source", S, "dump | api"),
    f("row_hash", S), f("batch_id", S), f("load_timestamp", T, "", False),
])

GOLD_AGG_WIKI_DAILY = StructType([
    f("wiki_key", I, "which wiki", False),
    f("date_key", I, "UTC day", False),
    f("edits", L), f("human_edits", L), f("bot_edits", L),
    f("bot_edit_share", DBL, "bot_edits / edits"),
    f("anonymous_edits", L, "ip + temporary account edits"),
    f("new_articles", L), f("pages_created", L, "all namespaces"),
    f("pages_deleted", L), f("pages_restored", L), f("pages_moved", L),
    f("active_editors", L, "distinct editors with an edit that day"),
    f("active_human_editors", L),
    f("reverts", L), f("reverted_edits", L), f("vandalism_suspects", L),
    f("median_seconds_to_revert", DBL, "over the edits of that day that were reverted"),
    f("bytes_added", L), f("bytes_removed", L), f("net_bytes", L),
    f("batch_id", S), f("load_timestamp", T, "", False),
])

GOLD_AGG_REVERT_LATENCY = StructType([
    f("wiki_key", I, "which wiki", False),
    f("month_key", I, "first day of the month", False),
    f("editor_type", S, "type of the editor whose edit was reverted", False),
    f("reverted_edits", L, "reverted edits with a known time to revert"),
    f("vandalism_suspects", L),
    f("median_seconds_to_revert", DBL), f("p90_seconds_to_revert", DBL),
    f("avg_seconds_to_revert", DBL), f("median_minutes_to_revert", DBL),
    f("batch_id", S), f("load_timestamp", T, "", False),
])

GOLD_AGG_EDIT_WARS = StructType([
    f("wiki_key", I, "which wiki", False),
    f("page_id", L, "MediaWiki page id", False),
    f("date_key", I, "UTC day", False),
    f("page_key", L, "page version at the last revert of the day", False),
    f("revert_count", L, "reverts on the page that day (2 or more)"),
    f("distinct_reverters", L, "different editors who reverted"),
    f("reverted_edits", L, "edits on the page that day that were later undone"),
    f("edits", L, "all edits on the page that day"),
    f("distinct_editors", L, "editors involved that day"),
    f("first_revert_ts", T), f("last_revert_ts", T),
    f("span_minutes", DBL, "minutes between the first and last revert"),
    f("is_edit_war", B, "3 or more reverts by 2 or more different editors (the Phase 1 rule)"),
    f("batch_id", S), f("load_timestamp", T, "", False),
])

GOLD_AGG_EDITOR_MONTHLY = StructType([
    f("wiki_key", I, "which wiki", False),
    f("month_key", I, "first day of the month", False),
    f("editor_group", S, "human | bot", False),
    f("editors", L, "editors with at least one edit"),
    f("active_editors", L, "5 or more edits"),
    f("very_active_editors", L, "100 or more edits"),
    f("new_editors", L, "first edit we have from them is in this month"),
    f("returning_next_month", L, "editors of this month who also edited the next month"),
    f("active_returning_next_month", L, "active editors who also edited the next month"),
    f("edits", L),
    f("batch_id", S), f("load_timestamp", T, "", False),
])

# --------------------------------------------------------------------------------------
# Ops tables
# --------------------------------------------------------------------------------------
GOLD_WATERMARK = StructType([
    f("target_table", S, "gold table the watermark belongs to", False),
    f("source_table", S, "silver table it reads"),
    f("last_silver_load_ts", T, "newest silver load_timestamp already in gold"),
    f("run_id", S, "run that moved it last"),
    f("load_timestamp", T, "", False),
])

EXECUTION_LOG = StructType([
    f("log_id", S, "", False),
    f("run_id", S, "job run id, shared by all steps of one run"),
    f("layer", S, "raw_to_bronze | bronze_to_silver | silver_scd2 | silver_to_gold | gold_optimize"),
    f("load_type", S, "full | incremental"),
    f("target_table", S),
    f("source_param", S, "file path / partition / parameter that was processed"),
    f("start_ts", T), f("end_ts", T), f("duration_sec", DoubleType()),
    f("status", S, "SUCCESS | FAILED | SKIPPED"),
    f("rows_read", L), f("rows_inserted", L), f("rows_updated", L), f("rows_deleted", L),
    f("rows_quarantined", L),
    f("error_message", S), f("params_json", S, "all notebook parameters for this run"),
    f("load_timestamp", T, "", False),
])

SCHEMA_DRIFT_LOG = StructType([
    f("source", S, "mediawiki_history | recentchanges", False),
    f("batch_ref", S, "snapshot or ingest_date", False),
    f("wiki", S, "", False),
    f("drift_type", S, "new_field | type_mismatch | malformed_json | column_count", False),
    f("field_name", S, "", False),
    f("row_count", L), f("sample_value", S),
    f("action_taken", S),
    f("first_seen_run_id", S),
    f("load_timestamp", T, "", False),
])

QUARANTINE = StructType([
    f("source_table", S, "bronze table the row came from", False),
    f("record_hash", S, "", False),
    f("wiki", S), f("batch_ref", S),
    f("reasons", ARR, "why it was rejected"),
    f("raw_payload", S, "the raw line, so it can be fixed and replayed"),
    f("batch_id", S),
    f("load_timestamp", T, "", False),
])

# --------------------------------------------------------------------------------------
# registry: key -> (layer, table, schema, partition columns, primary key)
# --------------------------------------------------------------------------------------
TABLES = {
    "bronze_history": ("bronze", "mediawiki_history", BRONZE_HISTORY, ["snapshot", "wiki"],
                       ["snapshot", "wiki", "record_hash"]),
    "bronze_rc": ("bronze", "recentchanges", BRONZE_RC, ["ingest_date", "wiki"],
                  ["ingest_date", "wiki", "record_hash"]),
    "silver_revision": ("silver", "revision", SILVER_REVISION, ["wiki"], ["wiki", "rev_id"]),
    "silver_page_event": ("silver", "page_event", SILVER_PAGE_EVENT, ["wiki"], ["wiki", "event_key"]),
    "silver_user_event": ("silver", "user_event", SILVER_USER_EVENT, ["wiki"], ["wiki", "event_key"]),
    "silver_page": ("silver", "page", SILVER_PAGE, ["wiki"], ["page_sk"]),
    "silver_editor": ("silver", "editor", SILVER_EDITOR, ["wiki"], ["editor_sk"]),
    # gold: no partitions, the big tables are clustered instead (gold.CLUSTER_COLUMNS)
    "gold_dim_date": ("gold", "dim_date", GOLD_DIM_DATE, [], ["date_key"]),
    "gold_dim_wiki": ("gold", "dim_wiki", GOLD_DIM_WIKI, [], ["wiki_key"]),
    "gold_dim_page": ("gold", "dim_page", GOLD_DIM_PAGE, [], ["page_key"]),
    "gold_dim_editor": ("gold", "dim_editor", GOLD_DIM_EDITOR, [], ["editor_key"]),
    "gold_dim_change_type": ("gold", "dim_change_type", GOLD_DIM_CHANGE_TYPE, [], ["change_type_key"]),
    "gold_fact_revision": ("gold", "fact_revision", GOLD_FACT_REVISION, [], ["revision_key"]),
    "gold_fact_page_event": ("gold", "fact_page_event", GOLD_FACT_PAGE_EVENT, [], ["page_event_key"]),
    "gold_agg_wiki_daily": ("gold", "agg_wiki_daily", GOLD_AGG_WIKI_DAILY, [], ["wiki_key", "date_key"]),
    "gold_agg_revert_latency": ("gold", "agg_revert_latency", GOLD_AGG_REVERT_LATENCY, [],
                                ["wiki_key", "month_key", "editor_type"]),
    "gold_agg_edit_wars": ("gold", "agg_edit_wars", GOLD_AGG_EDIT_WARS, [], ["wiki_key", "page_id", "date_key"]),
    "gold_agg_editor_monthly": ("gold", "agg_editor_monthly", GOLD_AGG_EDITOR_MONTHLY, [],
                                ["wiki_key", "month_key", "editor_group"]),
    "gold_watermark": ("ops", "gold_watermark", GOLD_WATERMARK, [], ["target_table"]),
    "execution_log": ("ops", "pipeline_execution_logs", EXECUTION_LOG, [], ["log_id"]),
    "drift_log": ("ops", "schema_drift_log", SCHEMA_DRIFT_LOG, [],
                  ["source", "batch_ref", "wiki", "drift_type", "field_name"]),
    "quarantine": ("ops", "quarantine", QUARANTINE, ["source_table"], ["source_table", "record_hash"]),
}

# gold foreign keys: table key -> [(column, referenced table key, referenced column)].
# added as informational FOREIGN KEY constraints in unity catalog (pipeline/setup.py)
GOLD_FKS = {
    "gold_dim_page": [("wiki_key", "gold_dim_wiki", "wiki_key")],
    "gold_dim_editor": [("wiki_key", "gold_dim_wiki", "wiki_key")],
    "gold_fact_revision": [("wiki_key", "gold_dim_wiki", "wiki_key"), ("date_key", "gold_dim_date", "date_key"),
                           ("pkt_date_key", "gold_dim_date", "date_key"), ("page_key", "gold_dim_page", "page_key"),
                           ("editor_key", "gold_dim_editor", "editor_key"),
                           ("change_type_key", "gold_dim_change_type", "change_type_key")],
    "gold_fact_page_event": [("wiki_key", "gold_dim_wiki", "wiki_key"), ("date_key", "gold_dim_date", "date_key"),
                             ("pkt_date_key", "gold_dim_date", "date_key"), ("page_key", "gold_dim_page", "page_key"),
                             ("editor_key", "gold_dim_editor", "editor_key"),
                             ("change_type_key", "gold_dim_change_type", "change_type_key")],
    "gold_agg_wiki_daily": [("wiki_key", "gold_dim_wiki", "wiki_key"), ("date_key", "gold_dim_date", "date_key")],
    "gold_agg_revert_latency": [("wiki_key", "gold_dim_wiki", "wiki_key"), ("month_key", "gold_dim_date", "date_key")],
    "gold_agg_edit_wars": [("wiki_key", "gold_dim_wiki", "wiki_key"), ("date_key", "gold_dim_date", "date_key"),
                           ("page_key", "gold_dim_page", "page_key")],
    "gold_agg_editor_monthly": [("wiki_key", "gold_dim_wiki", "wiki_key"), ("month_key", "gold_dim_date", "date_key")],
}
