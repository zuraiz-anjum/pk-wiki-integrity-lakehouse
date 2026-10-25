"""
Cost model for the Phase 3 report. Every number in the FinOps tables comes from here.

Measured: docs/databricks_facts.md (Databricks Free Edition, serverless, runs of 2026-10-11 and
the Gold runs). Prices: Azure Retail Prices API (https://prices.azure.com/api/retail/prices),
region eastus, pay-as-you-go, Premium, read on 25 Oct 2026. DBU rating of Standard_DS3_v2
(0.75 DBU/h) from https://azure.microsoft.com/en-us/pricing/details/databricks/.
"""

# ---- prices (USD) ---------------------------------------------------------------------
PRICE = {
    "jobs_dbu": 0.30,             # Premium Jobs Compute, per DBU
    "jobs_photon_dbu": 0.30,      # Premium Jobs Compute Photon, per DBU (Photon emits more DBU/h)
    "all_purpose_dbu": 0.55,      # Premium All-purpose Compute, per DBU
    "serverless_jobs_dbu": 0.45,  # Premium Automated Serverless Compute, per DBU (VM included)
    "serverless_sql_dbu": 0.70,   # Premium Serverless SQL, per DBU
    "vm_ds3v2": 0.293,            # Standard_DS3_v2 Linux on-demand, per hour
    "vm_ds3v2_spot": 0.068181,    # Standard_DS3_v2 Linux spot, per hour
    "vm_ds3v2_lowpri": 0.0585,    # Standard_DS3_v2 Linux low priority, per hour
}
DBU_PER_NODE_HOUR = 0.75          # Standard_DS3_v2 (4 vCPU, 14 GiB)
AWS_SERVERLESS_JOBS_LIST = 0.35   # list price of the Free Edition workspace's own SKU (us-east-2)
SQL_2XS_DBU_PER_HOUR = 4.0        # serverless SQL warehouse, 2X-Small

STORAGE_GB_MONTH = {"Hot": 0.0208, "Cool": 0.0152, "Cold": 0.0036, "Archive": 0.00099}

# ---- measured on Databricks (docs/databricks_facts.md) ------------------------------------
# The latest daily incremental run with all four tasks. Setup + execution seconds per task
# from the Jobs API.
STEP_TIMES = {           # dbx-gold-incr-2, after the key fix (commit 3d5673e)
    "bronze": {"setup_s": 2, "exec_s": 56},
    "silver": {"setup_s": 1, "exec_s": 53},
    "scd2": {"setup_s": 1, "exec_s": 50},
    "gold": {"setup_s": 2, "exec_s": 162},
}
# system.billing.usage ends on 2026-10-15, so the DBUs of the 25 October runs are not visible yet.
# dbx-incr-1 (2026-10-11, the loaded incremental run of Bronze + Silver + SCD2) is: 0.5943 DBU for
# 182 s of execution. We apply that measured rate to the new run.
REF_RUN = {"dbu": 0.5943, "exec_s": 182}
DBU_PER_EXEC_S = REF_RUN["dbu"] / REF_RUN["exec_s"]
JOB_RUN_NOOP = {"wall_s": 148.8, "exec_s": 136, "dbu": 0.5029}            # no-op job run, 3 tasks
TOTAL_DBU_2026_10_11 = 4.4546
GOLD_FULL = {"exec_s": 291, "loads_s": 99.2, "optimize_s": 41.4}
GOLD_INCR = {"exec_s": 173, "loads_s": 95.8, "optimize_s": 30.5}
DAILY_CAPTION = ("Daily incremental run dbx-gold-incr-2 on serverless (25 October 2026, current code). "
                 "Times are measured; DBUs are estimated from the rate measured on 11 October.")

# ---- workload assumptions ---------------------------------------------------------------
STARTUP_MINUTES = 5.0          # classic job cluster spin-up, billed (conservative: VM and DBU)
RUNS_PER_YEAR_HOURLY = 8760
RUNS_PER_YEAR_DAILY = 365
SQL_MINUTES_PER_REFRESH = 10   # Power BI import refresh; warehouse auto-stop after 10 idle minutes


def steps():
    out = {}
    for k, v in STEP_TIMES.items():
        out[k] = {"exec_s": v["exec_s"], "wall_s": v["setup_s"] + v["exec_s"],
                  "dbu": v["exec_s"] * DBU_PER_EXEC_S}
    return out


def run_totals():
    st = steps()
    return {"exec_s": sum(v["exec_s"] for v in st.values()),
            "wall_s": sum(v["wall_s"] for v in st.values()),
            "dbu": sum(v["dbu"] for v in st.values())}


def classic_run_cost(exec_minutes, dbu_price, vm_price, nodes=1):
    billable_h = (exec_minutes + STARTUP_MINUTES) / 60
    dbu = nodes * DBU_PER_NODE_HOUR * billable_h * dbu_price
    vm = nodes * vm_price * billable_h
    return billable_h, dbu, vm


def daily_run():
    t = run_totals()
    exec_min = t["exec_s"] / 60
    sv = t["dbu"] * PRICE["serverless_jobs_dbu"]
    bh, d, v = classic_run_cost(exec_min, PRICE["jobs_dbu"], PRICE["vm_ds3v2"])
    sql_dbu = SQL_2XS_DBU_PER_HOUR * SQL_MINUTES_PER_REFRESH / 60
    return {
        "exec_s": t["exec_s"], "wall_s": t["wall_s"], "exec_min": exec_min, "dbu": t["dbu"],
        "serverless": sv, "serverless_aws": t["dbu"] * AWS_SERVERLESS_JOBS_LIST,
        "serverless_year": sv * RUNS_PER_YEAR_DAILY,
        "classic_billable_h": bh, "classic_dbu": d, "classic_vm": v, "classic": d + v,
        "classic_year": (d + v) * RUNS_PER_YEAR_DAILY,
        "sql_dbu": sql_dbu, "sql": sql_dbu * PRICE["serverless_sql_dbu"],
        "sql_year": sql_dbu * PRICE["serverless_sql_dbu"] * RUNS_PER_YEAR_DAILY,
    }


def enterprise_rows():
    """Hourly runs; each run does the work of today's measured daily run (see report)."""
    t = run_totals()
    exec_min = t["exec_s"] / 60
    n = RUNS_PER_YEAR_HOURLY
    rows = []

    h = 8760
    d = DBU_PER_NODE_HOUR * h * PRICE["all_purpose_dbu"]
    v = PRICE["vm_ds3v2"] * h
    rows.append(dict(name="Always-on all-purpose cluster, 1 node, 24 x 365", short="Always-on all-purpose",
                     hours=h, basis=f"0.75 DBU x 8,760 h x ${PRICE['all_purpose_dbu']:.2f} + VM 8,760 h x ${PRICE['vm_ds3v2']:.3f}",
                     dbu_cost=d, vm_cost=v))

    bh, d, v = classic_run_cost(exec_min, PRICE["jobs_dbu"], PRICE["vm_ds3v2"])
    rows.append(dict(name="Job cluster per run, 1 node, on-demand VM", short="Job cluster, on-demand",
                     hours=bh * n, basis=f"({exec_min:.1f} + {STARTUP_MINUTES:.0f} start-up) min x 8,760 runs, jobs ${PRICE['jobs_dbu']:.2f}/DBU",
                     dbu_cost=d * n, vm_cost=v * n))

    bh, d, v = classic_run_cost(exec_min, PRICE["jobs_dbu"], PRICE["vm_ds3v2_spot"])
    rows.append(dict(name="Job cluster per run, 1 node, spot VM", short="Job cluster, spot VM",
                     hours=bh * n, basis=f"same minutes, VM at spot ${PRICE['vm_ds3v2_spot']:.4f}/h; evicted runs retried",
                     dbu_cost=d * n, vm_cost=v * n))

    d = t["dbu"] * n * PRICE["serverless_jobs_dbu"]
    rows.append(dict(name="Serverless jobs, DBUs of the daily run", short="Serverless jobs",
                     hours=t["exec_s"] / 3600 * n, basis=f"{t['dbu']:.3f} DBU per run (estimated) x 8,760 x ${PRICE['serverless_jobs_dbu']:.2f}",
                     dbu_cost=d, vm_cost=0.0))

    jr = noop_share()
    d = t["dbu"] * jr * n * PRICE["serverless_jobs_dbu"]
    rows.append(dict(name="Serverless jobs, quiet hours (no new rows)", short="Serverless, no-op runs",
                     hours=t["exec_s"] / 3600 * n,
                     basis=f"a measured no-op run used {jr * 100:.0f}% of the DBUs of a loaded run",
                     dbu_cost=d, vm_cost=0.0))

    for r in rows:
        r["total"] = r["dbu_cost"] + r["vm_cost"]
    return rows


def noop_share():
    """DBUs of the measured no-op job run relative to the measured loaded run (same three tasks)."""
    return JOB_RUN_NOOP["dbu"] / REF_RUN["dbu"]


def storage_rows(gb=1000):
    return [(tier, price, price * gb, price * gb * 12) for tier, price in STORAGE_GB_MONTH.items()]


if __name__ == "__main__":
    print(run_totals())
    print(daily_run())
    for r in enterprise_rows():
        print(f"{r['short']:28s} h={r['hours']:8,.0f} dbu={r['dbu_cost']:10,.2f} vm={r['vm_cost']:10,.2f} total={r['total']:10,.2f}")
