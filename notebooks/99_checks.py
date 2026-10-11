# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - checks
# MAGIC Queries we use to look at a run and for the screenshots in the report: execution logs, the idempotency proof, duplicate keys, quarantine / drift, row counts and the MERGE history.
# MAGIC Nothing here writes anything. Params: `catalog`, `prefix`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("prefix", "pkwiki")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
prefix = dbutils.widgets.get("prefix")


def t(layer, name):
    return f"{catalog}.{prefix}_{layer}.{name}"


logs = t("ops", "pipeline_execution_logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Latest execution log rows

# COMMAND ----------

display(spark.sql(f"""
    SELECT start_ts, run_id, layer, target_table, source_param, status,
           rows_read, rows_inserted, rows_updated, rows_deleted, rows_quarantined, duration_sec
    FROM {logs}
    ORDER BY start_ts DESC
    LIMIT 50
"""))

# COMMAND ----------

# failures only, with the error
display(spark.sql(f"""
    SELECT start_ts, run_id, layer, target_table, source_param, error_message
    FROM {logs}
    WHERE status = 'FAILED'
    ORDER BY start_ts DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### One row per run

# COMMAND ----------

display(spark.sql(f"""
    SELECT run_id, min(start_ts) AS started, max(end_ts) AS finished,
           first(load_type) AS load_type,
           concat_ws(', ', sort_array(collect_set(layer))) AS layers,
           count(*) AS steps,
           sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
           sum(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped,
           sum(rows_read) AS rows_read, sum(rows_inserted) AS rows_inserted,
           sum(rows_updated) AS rows_updated, sum(rows_deleted) AS rows_deleted,
           sum(rows_quarantined) AS rows_quarantined,
           round(sum(duration_sec), 1) AS duration_sec
    FROM {logs}
    GROUP BY run_id
    ORDER BY started DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Idempotency
# MAGIC Run 01-03 twice with the same params, then look here. Every step that processed the same source more than once is listed; from the 2nd attempt on `rows_inserted` and `rows_updated` should be 0.
# MAGIC (Exception: the normal daily run re-reads yesterday's folder, so a 2nd attempt there can insert rows if new files landed in between.)

# COMMAND ----------

attempts = f"""
    SELECT layer, target_table, source_param, run_id, start_ts,
           rows_read, rows_inserted, rows_updated, rows_deleted,
           row_number() OVER (PARTITION BY layer, target_table, source_param ORDER BY start_ts) AS attempt,
           count(*) OVER (PARTITION BY layer, target_table, source_param) AS attempts
    FROM {logs}
    WHERE status = 'SUCCESS'
"""
display(spark.sql(f"""
    SELECT * FROM ({attempts})
    WHERE attempts > 1
    ORDER BY target_table, source_param, attempt
"""))

# COMMAND ----------

# should print 0
bad = spark.sql(f"""
    SELECT count(*) AS n FROM ({attempts})
    WHERE attempt > 1 AND (rows_inserted > 0 OR rows_updated > 0)
""").first().n
print(f"reruns that changed something: {bad}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate primary keys
# MAGIC All should be 0. For the SCD2 tables we also check there is only one current row per page / editor.

# COMMAND ----------

pk_checks = [
    ("bronze", "mediawiki_history", "snapshot, wiki, record_hash", ""),
    ("bronze", "recentchanges", "ingest_date, wiki, record_hash", ""),
    ("silver", "revision", "wiki, rev_id", ""),
    ("silver", "page_event", "wiki, event_key", ""),
    ("silver", "user_event", "wiki, event_key", ""),
    ("silver", "page", "page_sk", ""),
    ("silver", "page", "wiki, page_id", "WHERE is_current"),
    ("silver", "editor", "editor_sk", ""),
    ("silver", "editor", "wiki, editor_key", "WHERE is_current"),
    ("ops", "quarantine", "source_table, record_hash", ""),
]
rows = []
for layer, name, keys, where in pk_checks:
    n = spark.sql(f"""
        SELECT count(*) AS n FROM (
            SELECT {keys} FROM {t(layer, name)} {where} GROUP BY {keys} HAVING count(*) > 1)
    """).first().n
    rows.append((f"{layer}.{name}", keys + (" (current only)" if where else ""), n))
display(spark.createDataFrame(rows, "table string, key string, duplicate_keys long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarantine and schema drift

# COMMAND ----------

display(spark.sql(f"""
    SELECT source_table, batch_ref, wiki, reason, count(*) AS n
    FROM {t("ops", "quarantine")} LATERAL VIEW explode(reasons) r AS reason
    GROUP BY source_table, batch_ref, wiki, reason
    ORDER BY n DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT source_table, wiki, batch_ref, reasons, substring(raw_payload, 1, 200) AS raw_payload, batch_id
    FROM {t("ops", "quarantine")}
    ORDER BY load_timestamp DESC
    LIMIT 20
"""))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {t('ops', 'schema_drift_log')} ORDER BY load_timestamp DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row counts

# COMMAND ----------

tables = [("bronze", "mediawiki_history"), ("bronze", "recentchanges"),
          ("silver", "revision"), ("silver", "page_event"), ("silver", "user_event"),
          ("silver", "page"), ("silver", "editor"),
          ("ops", "pipeline_execution_logs"), ("ops", "schema_drift_log"), ("ops", "quarantine")]
counts = [(f"{layer}.{name}", spark.table(t(layer, name)).count()) for layer, name in tables]
display(spark.createDataFrame(counts, "table string, row_count long"))

# COMMAND ----------

# per wiki and source, plus how many reverts we have
display(spark.sql(f"""
    SELECT wiki, source, count(*) AS revisions,
           sum(CAST(is_revert AS INT)) AS reverts, sum(CAST(is_reverted AS INT)) AS reverted,
           sum(CAST(is_bot AS INT)) AS bot_edits, min(rev_ts) AS first_edit, max(rev_ts) AS last_edit
    FROM {t("silver", "revision")}
    GROUP BY wiki, source
    ORDER BY wiki, source
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD2 example
# MAGIC Pages that have more than one version (renamed, turned into a redirect, deleted ...).

# COMMAND ----------

display(spark.sql(f"""
    SELECT wiki, page_id, page_title, page_namespace, is_redirect, is_deleted, valid_from, valid_to, is_current
    FROM {t("silver", "page")}
    WHERE (wiki, page_id) IN (SELECT wiki, page_id FROM {t("silver", "page")} GROUP BY wiki, page_id HAVING count(*) > 1)
    ORDER BY wiki, page_id, valid_from
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta history of silver.revision
# MAGIC Shows the MERGE operations and their row metrics. A rerun shows up as a MERGE with 0 inserted / 0 updated (or doesn't add a version at all).

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {t('silver', 'revision')}")
        .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
        .limit(30))
