"""
Execution logging. Every unit of work (one wiki partition into bronze, one silver table for a
batch, ...) runs inside audit_step() and leaves exactly one row in ops.pipeline_execution_logs,
whether it worked or not.

    with audit_step(spark, cfg, run, layer="raw_to_bronze", target=..., source=...) as m:
        ...
        m["rows_inserted"] = n
"""
import datetime as dt
import json
import traceback
import uuid
from contextlib import contextmanager

from pipeline.delta_utils import table_name
from pipeline.schemas import EXECUTION_LOG


class StepSkipped(Exception):
    """Raise inside a step when there is nothing to do (e.g. no file landed for that day)."""


def _now():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def write_log(spark, cfg, row):
    spark.createDataFrame([row], EXECUTION_LOG).write.format("delta").mode("append") \
        .saveAsTable(table_name(cfg, "execution_log"))


@contextmanager
def audit_step(spark, cfg, run, layer, target, source, reraise=True):
    """run is a dict with run_id, load_type and params (see notebooks)."""
    m = {"rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0,
         "rows_quarantined": 0}
    start = _now()
    status, error = "SUCCESS", None
    try:
        yield m
    except StepSkipped as e:
        status, error = "SKIPPED", str(e)
    except Exception as e:  # noqa: BLE001 - we log everything, then decide whether to re-raise
        status = "FAILED"
        error = f"{type(e).__name__}: {e}\n{traceback.format_exc(limit=5)}"[:4000]
        if reraise:
            raise
        run.setdefault("failures", []).append(f"{target} <- {source}: {type(e).__name__}: {e}")
    finally:
        end = _now()
        write_log(spark, cfg, {
            "log_id": uuid.uuid4().hex, "run_id": run["run_id"], "layer": layer,
            "load_type": run["load_type"], "target_table": target, "source_param": source,
            "start_ts": start, "end_ts": end, "duration_sec": (end - start).total_seconds(),
            "status": status, **{k: int(v) for k, v in m.items()},
            "error_message": error, "params_json": json.dumps(run.get("params", {}), default=str),
            "load_timestamp": end,
        })
        print(f"[{status}] {layer} {target} <- {source} "
              f"read={m['rows_read']} ins={m['rows_inserted']} upd={m['rows_updated']} "
              f"del={m['rows_deleted']} quarantined={m['rows_quarantined']}"
              + (f" ({error.splitlines()[0]})" if error else ""))


def raise_if_failed(run):
    """Partitions are processed independently (one bad wiki doesn't stop the others), but the
    notebook should still end red so the job shows the failure."""
    if run.get("failures"):
        raise RuntimeError("some steps failed:\n" + "\n".join(run["failures"]))

