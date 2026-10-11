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
# Ops tables
# --------------------------------------------------------------------------------------
EXECUTION_LOG = StructType([
    f("log_id", S, "", False),
    f("run_id", S, "job run id, shared by all steps of one run"),
    f("layer", S, "raw_to_bronze | bronze_to_silver | silver_scd2"),
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
    "execution_log": ("ops", "pipeline_execution_logs", EXECUTION_LOG, [], ["log_id"]),
    "drift_log": ("ops", "schema_drift_log", SCHEMA_DRIFT_LOG, [],
                  ["source", "batch_ref", "wiki", "drift_type", "field_name"]),
    "quarantine": ("ops", "quarantine", QUARANTINE, ["source_table"], ["source_table", "record_hash"]),
}
