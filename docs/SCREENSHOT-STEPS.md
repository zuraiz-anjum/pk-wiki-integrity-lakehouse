# Phase 3 report: screenshot and rebuild steps

The report (`docs/phase3_report.pdf`) has one marked placeholder, Figure 4 in section 5.3:
**Databricks Query Profile of MERGE INTO agg_wiki_daily (serverless)**. Everything else is already
in the PDF.

## 1. What to capture

The Query Profile graph of the `MERGE INTO workspace.pkwiki_gold.agg_wiki_daily` statement, the
Gold fact aggregation (built by `wiki_daily_frame()` in `pipeline/gold.py`). One clear screenshot
of the graph view with metrics turned on is enough. The node names should match Listing 1 in the
report: PhotonScan, PhotonBroadcastHashJoin, PhotonExpand, PhotonGroupingAgg, the shuffle
exchanges and PhotonShuffledHashJoin.

## 2. How to capture it

### On Databricks Free Edition (serverless, what we use)

Serverless compute has no classic Spark UI. The same operator graph is in the **Query Profile**.

1. Open the Gold task run of `dbx-gold-incr-1` (or `dbx-gold-full-1`). The links are in
   `docs/databricks_facts.md`, section "Runs and per-task durations". You can also go to
   Jobs & Pipelines, open the run and click the `gold` task.
2. The run page shows the notebook with its outputs. Scroll to the cell
   `run = runner.run_gold(...)` and open **See performance** under it. It lists every query the
   cell ran.
3. Click the `MERGE INTO workspace.pkwiki_gold.agg_wiki_daily` query. That opens its Query
   Profile.
4. Switch to the **graph** view, expand it to full screen and turn on **Show metrics** (rows and
   time per operator).
5. Take the screenshot (Windows: `Win + Shift + S`).

Another way in: left sidebar, **Query History**, compute filter set to serverless compute for
jobs, time filter around the run (2026-10-25 15:11 to 15:15 UTC for the Gold task of
`dbx-gold-incr-1`), open the MERGE statement, then **See query profile**.

### On a classic cluster (Azure Databricks all-purpose or job cluster)

1. Run `notebooks/04_gold_star` on the cluster.
2. Open **Spark UI** from the cluster page (or from the cell's Spark Jobs link).
3. Go to the **SQL / DataFrame** tab and click the MERGE into `agg_wiki_daily`.
4. The **DAG** (plan graph) is on that page. Expand the boxes and take the screenshot. The
   **Stages** tab shows one stage per exchange.

## 3. Where to put it

Save the screenshot as a PNG at exactly this path:

```
docs/figures/spark_ui_dag.png
```

The build script looks for that file. If it exists, it replaces the red placeholder box in
section 5.3 with your image, scaled to the page width, with the caption "Databricks Query Profile
of MERGE INTO agg_wiki_daily (serverless)". Nothing else needs editing.

## 4. Rebuild the PDF

From the repository root, in WSL or any Python 3.10+ environment:

```bash
pip install reportlab matplotlib pillow   # once
python docs/phase3_figures.py             # only if figures or cost numbers changed
python docs/phase3_report.py              # writes docs/phase3_report.pdf
```

What each source file holds:

| File | Content |
| --- | --- |
| `docs/phase3_report.py` | layout: title page, contents, numbering, captions, page style |
| `docs/phase3_content.py` | text of sections 1 to 4 and 7, references |
| `docs/phase3_facts.py` | section 4.5 (Gold strategy and the measured Gold runs) |
| `docs/phase3_dag.py` | section 5 (plan walk-through, screenshot slot); reads the plan from `docs/databricks_facts.md` |
| `docs/phase3_finops.py` | section 6 text and tables |
| `docs/phase3_costs.py` | every price and measured number used in section 6; edit here and rebuild |
| `docs/phase3_figures.py` | architecture diagram, star schema, cost chart, placeholder box |

## 5. Optional updates before you submit

- DBUs for the 25 October runs: `system.billing.usage` only went up to 15 October when the report
  was built, so section 6.2 estimates them from the 11 October rate. Once the billing table has
  caught up, query the DBUs of the `dbx-gold-incr-1` run and put them in `phase3_costs.py`
  (replace `DBU_PER_EXEC_S` with the measured total divided by 344 s, or edit `steps()`), then
  rebuild.
- Course name: the Phase 1 proposal left it as a placeholder, so the title page only says
  "Data Engineering project, Phase 3". Change `COURSE_LINE` at the top of
  `docs/phase3_content.py` if you want the course name there.
