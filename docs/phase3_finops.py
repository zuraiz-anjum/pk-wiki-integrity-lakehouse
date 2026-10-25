"""FinOps section of the Phase 3 report. Numbers come from phase3_costs.py."""
import os

import phase3_costs as K


def c(s):
    s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<font name="Courier" size="9.4">{s}</font>'


def money(x, d=2):
    return f"${x:,.{d}f}"


def finops(X):
    h1, h2, p, table, figure = (X[k] for k in ("h1", "h2", "p", "table", "figure"))
    FIG = X["FIG"]
    d = K.daily_run()
    rows = K.enterprise_rows()
    st = K.steps()
    P = K.PRICE
    s = []
    s += h1("FinOps")
    s += h2("Pricing basis")
    s.append(p(
        "Our runs used Databricks Free Edition, which charges nothing but still records usage in "
        "the " + c("system.billing.usage") + " table. So we know how many DBUs (Databricks Units) "
        "each run consumed, and we price those DBUs with Microsoft's published pay-as-you-go rates "
        "for Azure Databricks Premium in East US, read from the Azure Retail Prices API and the Azure "
        "Databricks pricing page on 25 October 2026 [1, 2]. Classic compute is billed twice, once in "
        "DBUs and once for the virtual machines [3]. A " + c("Standard_DS3_v2") + " node (4 vCPU, 14 GiB) "
        "is rated at 0.75 DBU per hour. Serverless compute charges one DBU rate that already includes "
        "the machines. Azure for Students credit is spent at the same rates."))
    s += table([
        ["Item", "Rate (USD)", "Unit"],
        ["Jobs Compute (Premium)", f"{P['jobs_dbu']:.2f}", "per DBU"],
        ["Jobs Compute Photon (Premium)", f"{P['jobs_photon_dbu']:.2f}", "per DBU (Photon nodes emit more DBUs per hour)"],
        ["All-purpose Compute (Premium)", f"{P['all_purpose_dbu']:.2f}", "per DBU"],
        ["Serverless Jobs (Automated Serverless Compute)", f"{P['serverless_jobs_dbu']:.2f}", "per DBU, machines included"],
        ["Serverless SQL warehouse", f"{P['serverless_sql_dbu']:.2f}", "per DBU, machines included"],
        ["Standard_DS3_v2 VM, Linux, on-demand", f"{P['vm_ds3v2']:.3f}", "per hour"],
        ["Standard_DS3_v2 VM, Linux, spot", f"{P['vm_ds3v2_spot']:.4f}", "per hour (can be evicted)"],
        ["DBU rating of Standard_DS3_v2", f"{K.DBU_PER_NODE_HOUR:.2f}", "DBU per node hour"],
    ], [4.6, 1.4, 3.6], "Azure prices used in this section (East US, pay-as-you-go, Premium tier) [1, 2].")

    # ------------------------------------------------------------------ daily run
    s += h2("Typical daily incremental run")
    trows = [["Task", "Notebook", "Setup s", "Execution s", "Wall s", "DBU (estimated)"]]
    names = {"bronze": "01_bronze_ingest", "silver": "02_silver_unify_and_mask",
             "scd2": "03_silver_scd2_and_reverts", "gold": "04_gold_star"}
    for k in ("bronze", "silver", "scd2", "gold"):
        v = st[k]
        trows.append([k, names[k], f"{v['wall_s'] - v['exec_s']:.0f}", f"{v['exec_s']:.0f}", f"{v['wall_s']:.0f}",
                      f"{v['dbu']:.3f}"])
    trows.append(["<b>Total</b>", "", "", f"<b>{d['exec_s']:.0f}</b>", f"<b>{d['wall_s']:.0f}</b>",
                  f"<b>{d['dbu']:.3f}</b>"])
    s += table(trows, [1.0, 2.6, 0.9, 1.2, 0.9, 1.4], K.DAILY_CAPTION)
    s.append(p(
        "Table " + str(X["tab_no"]() - 1) + " is our latest daily run on 25 October (" +
        c("dbx-gold-incr-2") + "): Bronze, Silver, SCD2 and Gold as one multi-task run on the current "
        "code. Bronze re-read the whole 25 October folder (13,070 change lines) and inserted the 168 "
        "that were new. Execution took " + f"{d['exec_s']:.0f}" + " seconds, about " +
        f"{d['exec_s'] / 60:.1f}" + " minutes, and half of it was the Gold task. The first daily run "
        "of that day, before the key fix, inserted 12,902 new changes and took 344 seconds, so the run "
        "time barely depends on how much data arrives. The billing table only runs to 15 October, so "
        "the DBUs of these runs are not visible yet. We estimate them from the incremental run of 11 "
        "October, which used " + f"{K.REF_RUN['dbu']:.4f}" + " DBU for " + f"{K.REF_RUN['exec_s']}" +
        " seconds of execution, or " + f"{K.DBU_PER_EXEC_S:.5f}" + " DBU per second. That gives " +
        f"{d['dbu']:.2f}" + " DBU for the whole run."))
    s.append(p(
        "On Azure serverless jobs that is " + f"{d['dbu']:.3f} x ${P['serverless_jobs_dbu']:.2f} = " +
        money(d["serverless"], 3) + " per run, or " + money(d["serverless_year"]) + " for a year of "
        "daily runs. At the list price of our own workspace's serverless SKU (" +
        f"${K.AWS_SERVERLESS_JOBS_LIST:.2f}" + " per DBU) it would be " + money(d["serverless_aws"], 3) +
        ". On the free tier it cost nothing; all 22 runs of 11 October together used " +
        f"{K.TOTAL_DBU_2026_10_11:.2f}" + " DBU."))
    s.append(p(
        "The same run on a classic single-node job cluster (" + c("Standard_DS3_v2") + ") is billed "
        "for the execution time plus about " + f"{K.STARTUP_MINUTES:.0f}" + " minutes of cluster "
        "start-up. We bill the start-up in full for both the VM and the DBUs, which slightly "
        "overstates the cost, and we assume one 4-core node runs the job about as fast as serverless "
        "did, since the run is dominated by fixed overhead. Billable time is (" +
        f"{d['exec_min']:.1f} + {K.STARTUP_MINUTES:.0f}" + ") / 60 = " + f"{d['classic_billable_h']:.3f}" +
        " h. The DBUs cost 0.75 x " + f"{d['classic_billable_h']:.3f} x ${P['jobs_dbu']:.2f} = " +
        money(d["classic_dbu"], 3) + " and the VM " +
        f"${P['vm_ds3v2']:.3f} x {d['classic_billable_h']:.3f} = {money(d['classic_vm'], 3)}" +
        ", so one run is " + money(d["classic"], 3) + " and a year of daily runs " +
        money(d["classic_year"]) + "."))
    noop_share = K.noop_share()
    s.append(p(
        "Two things in the measurements surprised us. First, the cost is almost all fixed overhead. "
        "The standard daily run that found nothing new (it inserted zero rows) still used " +
        f"{K.JOB_RUN_NOOP['dbu']:.2f}" + " DBU for Bronze, Silver and SCD2, about " +
        f"{noop_share * 100:.0f}" + " percent of what the loaded run of 11 October used for the same three tasks. "
        "Delta commits, MERGE planning and the audit writes cost the same whether a day has 17,000 "
        "events or none. The Gold task shows the same thing: in the last daily run it took 162 "
        "seconds although only 42 new revisions arrived. Second, the SQL warehouse is not a small "
        "line. One warehouse session on 11 "
        "October used 2.44 DBU, more than half of all job DBUs that day. If Power BI refreshes once a "
        "day on a 2X-Small serverless warehouse (about " + f"{K.SQL_2XS_DBU_PER_HOUR:.0f}" +
        " DBU per hour) that auto-stops after " + f"{K.SQL_MINUTES_PER_REFRESH}" + " idle minutes, each "
        "refresh costs about " + money(d["sql"]) + ", or " + money(d["sql_year"], 0) + " a year, close to "
        "what the pipeline itself costs on serverless. That is why the dashboard uses import mode with one "
        "scheduled refresh after the job instead of DirectQuery."))

    # ------------------------------------------------------------------ enterprise
    s += h2("Enterprise extrapolation: hourly runs for a year")
    s.append(p(
        "Now suppose the pipeline becomes an enterprise service. It covers many more wikis and runs "
        "every hour so the dashboard is close to real time, which is " +
        f"{K.RUNS_PER_YEAR_HOURLY:,}" + " runs a year. We assume each hourly run handles about as much "
        "data as one of our current days. Because most of the measured cost is fixed overhead, the "
        "result is not very sensitive to that assumption. Table " + str(X["tab_no"]()) + " compares "
        "five ways to run it and Figure " + str(X["fig_no"]()) + " shows the split between DBU and VM "
        "charges."))
    trows = [["Option", "Billed h / yr", "Basis", "DBU cost", "VM cost", "Total / yr"]]
    for r in rows:
        trows.append([r["name"], f"{r['hours']:,.0f}", r["basis"], money(r["dbu_cost"], 0),
                      money(r["vm_cost"], 0), "<b>" + money(r["total"], 0) + "</b>"])
    s += table(trows, [3.0, 1.0, 3.8, 1.0, 1.0, 1.1],
               "Yearly compute cost of 8,760 hourly runs under different compute options.")
    s.append(figure(os.path.join(FIG, "enterprise_cost.png"),
                    "Yearly compute cost of the hourly enterprise scenario, split into DBU and VM charges.",
                    width=X["TEXT_W"] * 0.9))
    ao, job, spot, sl, sq = rows
    s.append(p(
        "The always-on cluster is the worst choice at " + money(ao["total"], 0) + " a year. It pays "
        "the all-purpose DBU rate for all 8,760 hours although the work needs about " +
        f"{d['exec_min']:.0f}" + " minutes of each hour: 0.75 x 8,760 x " +
        f"${P['all_purpose_dbu']:.2f} = {money(ao['dbu_cost'], 0)}" + " in DBUs plus " +
        f"8,760 x ${P['vm_ds3v2']:.3f} = {money(ao['vm_cost'], 0)}" + " for the VM. A job cluster "
        "that starts for each run and terminates afterwards costs " + money(job["total"], 0) + ", " +
        f"{(1 - job['total'] / ao['total']) * 100:.0f}" + " percent less, and a spot VM brings it to " +
        money(spot["total"], 0) + ". Spot capacity can be taken back at short notice, but our MERGEs "
        "are idempotent, so an evicted run is simply retried."))
    s.append(p(
        "Serverless comes out at " + money(sl["total"], 0) + ", and still " + money(sq["total"], 0) +
        " if most hours bring no new data. We expected the opposite before we had the billing data. "
        "For short, overhead-heavy runs, serverless meters many DBUs per run, while a small classic "
        "node is cheap per hour and its start-up costs a few cents. Serverless still has real "
        "advantages. Our tasks waited only 2 to 6 seconds for their environment instead of minutes, "
        "there is nothing to size or patch, and a multi-task job reuses one environment, which cut our "
        "wall time by about 20 percent compared with running the notebooks one by one. For an hourly "
        "pipeline where a five-minute start-up is acceptable, we would use a job cluster on a spot VM "
        "with fallback to on-demand. Where freshness matters more than cost, serverless is the simpler "
        "choice."))

    # ------------------------------------------------------------------ levers
    s += h2("Optimisations")
    s.append(p("Table " + str(X["tab_no"]()) + " lists the levers we would use, roughly in the order "
               "we would apply them, and what each one does in this pipeline."))
    s += table([
        ["Lever", "What it does here", "Expected effect"],
        ["Job clusters with auto-termination", "Each run gets its own cluster that stops when the job ends; any "
                                               "interactive cluster auto-terminates after 15 to 30 idle minutes",
         f"{money(ao['total'], 0)} down to {money(job['total'], 0)} a year in the hourly case"],
        ["Spot or low-priority VMs", "Spot for the node, or for the workers with an on-demand driver on bigger "
                                     "clusters; idempotent MERGEs make a retried run safe",
         f"VM bill down from {money(job['vm_cost'], 0)} to {money(spot['vm_cost'], 0)} a year"],
        ["Serverless where latency matters", "Seconds to start, nothing idle; one multi-task job reuses one "
                                             "environment", "about 20 percent less wall time, but a higher DBU bill for tiny runs"],
        ["Photon for Gold", "Vectorised engine for the joins and group-bys in the Gold build and for Power BI "
                            "queries (already on for our SQL warehouse)",
         "worth it only when the speed-up beats Photon's higher DBU rate; not for Bronze"],
        ["Incremental Gold", "Facts MERGE only Silver rows newer than the watermark; aggregates recompute only "
                             "the touched days and months", "run time follows the day's changes, not the whole history"],
        ["File compaction", "OPTIMIZE plus optimized writes merge the small files left by daily MERGEs; VACUUM "
                            "removes old versions weekly", "fewer files to list and open on every read"],
        ["Liquid clustering", "Gold facts clustered by date, wiki and page so dashboard filters skip most "
                              "files; OPTIMIZE only touches new data", "less data scanned per query and per MERGE"],
        ["Right-sizing", "Single node for the daily run; more workers only for the monthly full load; "
                         "smallest SQL warehouse with a short auto-stop", "no paying for idle cores"],
        ["Batched SQL sessions", "Power BI import mode with one refresh after the job",
         f"about {money(d['sql_year'], 0)} a year instead of a warehouse that rarely stops"],
        ["Storage lifecycle tiers", "Raw landing files to Cool after 30 days, then Archive or delete; old dump "
                                    "snapshots deleted once the next one is in",
         "storage per GB down by up to 95 percent (Table " + str(X["tab_no"]() + 1) + ")"],
    ], [2.2, 4.7, 2.7], "Cost optimisation levers.")
    srows = [["Tier", "USD per GB-month", "1 TB per month", "1 TB per year"]]
    for tier, price, m, y in K.storage_rows():
        srows.append([tier, f"{price:.5f}".rstrip("0"), money(m), money(y)])
    s += table(srows, [2, 2, 2, 2], "Azure Blob (ADLS Gen2) LRS storage prices, East US [2, 4].")
    s.append(p(
        "Storage is tiny for us. The Phase 1 estimate was 0.3 to 0.8 GB of Delta tables for all four "
        "wikis, which costs cents per month in the Hot tier, and Free Edition recorded only 1.6 "
        "storage units over five weeks. Lifecycle rules still matter for the raw files, because they "
        "are the part that grows every day and is almost never read again after loading."))
    return s
