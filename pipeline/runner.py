"""
Entry points used by the notebooks (and scripts/run_local.py). Every parameter is a string,
exactly what a Databricks widget / job parameter gives us.

    load_type    full | incremental
    snapshot     YYYY-MM                 full loads only
    start_date   YYYY-MM-DD              incremental: first ingest_date to process
    end_date     YYYY-MM-DD              incremental: last one (defaults to start_date)
                 both empty = standard daily run (the two newest ingest_date folders)
    wikis        all | urwiki,sdwiki,...
    landing_root folder with the raw files (defaults to the UC volume)
    bronze_mode  append (default) | rebuild  -> rebuild deletes the bronze partitions first
    catalog / prefix / run_id
"""
from pyspark.sql import functions as F

from pipeline.audit import raise_if_failed
from pipeline.bronze import load_history_snapshot, load_recentchanges, rebuild_partitions
from pipeline.config import Config, date_range, new_run_id, parse_snapshot, parse_wikis
from pipeline.delta_utils import init_session, sql_list, table_name
from pipeline.silver import silver_from_history, silver_from_recentchanges
from pipeline.silver_history import run_silver_history

DEFAULTS = {
    "catalog": "workspace", "prefix": "pkwiki", "landing_root": "", "load_type": "incremental",
    "snapshot": "", "start_date": "", "end_date": "", "wikis": "all", "bronze_mode": "append",
    "run_id": "",
}


def resolve(params):
    p = dict(DEFAULTS)
    p.update({k: (v or "").strip() for k, v in params.items() if k in DEFAULTS})
    if p["load_type"] not in ("full", "incremental"):
        raise ValueError(f"load_type must be full or incremental, got {p['load_type']!r}")
    if p["bronze_mode"] not in ("append", "rebuild"):
        raise ValueError(f"bronze_mode must be append or rebuild, got {p['bronze_mode']!r}")
    cfg = Config(p["catalog"], p["prefix"], p["landing_root"])
    run = {"run_id": new_run_id(p["run_id"]), "load_type": p["load_type"], "params": p}
    return cfg, run, p, parse_wikis(p["wikis"])


def _latest(values, n=2):
    return sorted(values)[-n:]


def bronze_refs(cfg, p):
    if p["load_type"] == "full":
        return [parse_snapshot(p["snapshot"])]
    if p["start_date"]:
        return date_range(p["start_date"], p["end_date"])
    # standard daily run: newest two landed days. re-reading yesterday is free (idempotent)
    # and catches files that landed after yesterday's run
    landed = cfg.landed_ingest_dates()
    if not landed:
        raise ValueError(f"no ingest_date folders under {cfg.landing_root}")
    return _latest(landed)


def silver_refs(spark, cfg, p):
    if p["load_type"] == "full" or p["start_date"]:
        return bronze_refs(cfg, p)
    rows = spark.table(table_name(cfg, "bronze_rc")).select("ingest_date").distinct().collect()
    if not rows:
        raise ValueError("bronze.recentchanges is empty, run 01_bronze_ingest first")
    return _latest([r.ingest_date for r in rows])


def run_bronze(spark, params):
    init_session(spark)
    cfg, run, p, wikis = resolve(params)
    refs = bronze_refs(cfg, p)
    print(f"run {run['run_id']}: raw -> bronze, {p['load_type']} {refs} {wikis}")
    if p["bronze_mode"] == "rebuild":
        if p["load_type"] == "full":
            rebuild_partitions(spark, cfg, "bronze_history",
                               f"snapshot = '{refs[0]}' AND wiki IN ({sql_list(wikis)})")
        else:
            rebuild_partitions(spark, cfg, "bronze_rc",
                               f"ingest_date IN ({sql_list(refs)}) AND wiki IN ({sql_list(wikis)})")
    if p["load_type"] == "full":
        load_history_snapshot(spark, cfg, run, refs[0], wikis)
    else:
        load_recentchanges(spark, cfg, run, refs, wikis)
    raise_if_failed(run)
    return run


def run_silver(spark, params, salt):
    init_session(spark)
    cfg, run, p, wikis = resolve(params)
    refs = silver_refs(spark, cfg, p)
    print(f"run {run['run_id']}: bronze -> silver, {p['load_type']} {refs} {wikis}")
    if p["load_type"] == "full":
        silver_from_history(spark, cfg, run, refs[0], wikis, salt)
    else:
        silver_from_recentchanges(spark, cfg, run, refs, wikis, salt)
    raise_if_failed(run)
    return run


def run_silver_scd2(spark, params):
    init_session(spark)
    cfg, run, p, wikis = resolve(params)
    refs = silver_refs(spark, cfg, p)
    print(f"run {run['run_id']}: silver scd2 + reverts, {p['load_type']} {refs} {wikis}")
    run_silver_history(spark, cfg, run, p["load_type"], refs, wikis)
    raise_if_failed(run)
    return run


def last_run_summary(spark, params, run_id):
    cfg, *_ = resolve(params)
    return (spark.table(table_name(cfg, "execution_log")).where(F.col("run_id") == run_id)
            .select("layer", "target_table", "source_param", "status", "rows_read", "rows_inserted",
                    "rows_updated", "rows_deleted", "rows_quarantined", "duration_sec")
            .orderBy("start_ts"))
