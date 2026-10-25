# Power BI guide: Knowledge Integrity Monitor

This is how we get the Phase 3 dashboard running in Power BI Desktop. The report reads the Gold
star schema (`workspace.pkwiki_gold`) plus the audit and quarantine tables (`workspace.pkwiki_ops`)
straight from Databricks.

There are two ways to do it:

- **Quick way (about 10 minutes).** Open the ready-made Power BI project in
  `dashboard/powerbi/databricks/`. The model, relationships, date table, measures, theme and all
  four pages are already in it. You only sign in once and save it as a `.pbix`.
- **By hand (about 35 to 40 minutes).** If the project won't open on your machine, section 6 builds
  the exact same thing click by click.

If Databricks can't be reached at all, there is a CSV version of the same project (section 4).

## 1. What is in the dashboard folder

| Path | What it is |
| --- | --- |
| `powerbi/databricks/Knowledge Integrity Monitor.pbip` | Power BI project, connected to Databricks. Open this one. |
| `powerbi/databricks/Knowledge Integrity Monitor.SemanticModel/` | The model: tables, relationships, measures (TMDL files) |
| `powerbi/databricks/Knowledge Integrity Monitor.Report/` | The four report pages (PBIR files) |
| `powerbi/csv/Knowledge Integrity Monitor CSV.pbip` | Same project, but it reads `dashboard/sample_gold/*.csv` |
| `theme.json` | Our report theme (already applied inside both projects) |
| `CONNECTION.md` | Server Hostname, HTTP Path, catalog and schemas of our SQL warehouse |
| `sample_gold/` | CSV extracts of the Gold tables, for working offline |

## 2. Before you start

1. **Power BI Desktop, September 2026 release or newer.** Projects saved in the PBIR report format
   open without any extra setting from that release on. Check under **Help > About**. If yours is
   older, update it from the Microsoft Store (or download the latest installer). On an older
   version you can also try **File > Options and settings > Options > Preview features**, tick the
   options for Power BI Project files and the enhanced report format (PBIR), and restart.
2. **Check the SQL warehouse.** In Databricks, open **SQL Warehouses** in the left sidebar and
   check that **Serverless Starter Warehouse** exists. It starts by itself on the first query, so
   it doesn't need to be running already. The first refresh can wait 10 to 30 seconds while it
   starts.
3. **Make a personal access token** (Power BI signs in with it):
   1. In Databricks, click your profile picture (top right) and choose **Settings**.
   2. Go to **Developer**, next to **Access tokens** click **Manage**.
   3. Click **Generate new token**. Comment: `power bi`. Lifetime: 30 days is fine.
   4. Click **Generate** and copy the token right away (it starts with `dapi`). Databricks only
      shows it once. Don't put it in any file or in Git.
4. **Server Hostname and HTTP Path.** These are already filled in inside the project (see
   `CONNECTION.md`). To look them up yourself: **SQL Warehouses > Serverless Starter Warehouse >
   Connection details** tab. Copy **Server hostname** and **HTTP path** from there.

## 3. Quick way: open the Databricks project

1. Close Power BI Desktop if it is open. Copy the repo to a short path if you can (Windows limits
   paths to 260 characters, and `C:\pbi\` style paths are safest).
2. Double click `dashboard/powerbi/databricks/Knowledge Integrity Monitor.pbip` (or **File > Open
   report > Browse reports**, set the file type to **Power BI project files (*.pbip)**).
3. The report opens with empty visuals. That is normal: a project is saved without data.
4. Click **Home > Refresh**.
5. Power BI asks for credentials for the Databricks source:
   - On the left pick **Personal Access Token**.
   - Paste the token from step 2.3.
   - Click **Connect**.
6. If it asks about privacy levels, choose **Organizational** for the source and click **Save**.
7. Wait for the refresh to finish (one or two minutes on our sample sized data). The visuals fill in.
8. Click through the four pages at the bottom: **Overview**, **Wikis and bots**, **Vandalism and
   reverts**, **Pipeline health**.
9. Save it as a normal Power BI file: **File > Save as**, pick the `dashboard` folder, set **Save as
   type** to **Power BI file (*.pbix)**, name it `pk_wiki_integrity.pbix`, click **Save**.

If the warehouse ever moves, change the connection without editing any query: **Home > Transform
data > Edit parameters**, then update `DatabricksHost` and `DatabricksHttpPath` (the other three
parameters are the catalog and the two schemas). Click **OK** and then **Apply changes**.

### Import or DirectQuery?

The project uses **Import**, and we recommend keeping it that way:

- Our Gold tables are small (tens of thousands of rows on the sample, a few million at most with
  the full dumps), so they fit in memory easily and every click is instant.
- We are on Databricks Free Edition. DirectQuery sends SQL to the warehouse on every click and
  every slicer change, so it keeps the warehouse awake and eats the daily compute quota. With
  Import the warehouse only runs during a refresh.
- Some of our measures (median and 90th percentile time to revert, editors with 5+ edits) and the
  two calculated columns are slow or limited in DirectQuery.
- The data only changes once a day (the job runs at 2 AM PKT), so a manual or scheduled refresh
  after the job is enough.

DirectQuery would only make sense if the tables got too big to import or if we needed live data
every few minutes.

## 4. Fallback: the CSV project (no Databricks needed)

Use this if the token or the warehouse doesn't work. It is the same model and the same four pages,
it just reads the CSV extracts in `dashboard/sample_gold/` instead of Databricks.

1. Make sure `dashboard/sample_gold/` has the CSV files (`dim_date.csv`, `fact_revision.csv`, and so
   on). They are made with `python scripts/run_local.py export`.
2. Open `dashboard/powerbi/csv/Knowledge Integrity Monitor CSV.pbip`.
3. Power BI can't use paths relative to the project file, so the folder is a parameter. Its default
   is `C:\pbi\pk-wiki-integrity-lakehouse\dashboard\sample_gold\`, so if you cloned the repo to
   `C:\pbi\` there is nothing to change. Otherwise click **Home > Transform data > Edit parameters**
   and set **SampleFolder** to the full path of your `dashboard\sample_gold` folder, **ending with a
   backslash**. Click **OK**.
4. Click **Apply changes** (or **Home > Refresh**). No sign in is needed for local files. If it asks
   about privacy levels, pick **Public** or **Organizational**.
5. Save as `.pbix` like in step 3.9.

Notes:

- The extract keeps every dimension and aggregate row but only the newest 30,000 rows of the two
  big facts, so the totals are smaller than on Databricks.
- A missing CSV file doesn't break the refresh, its table just stays empty. If
  `pipeline_execution_logs.csv`, `quarantine.csv` and `schema_drift_log.csv` aren't in the folder,
  the **Pipeline health** page will be empty in this version.
- To build the CSV version by hand instead, follow section 6 but in step 6.2 use **Get data >
  Text/CSV** for each file (section 7 has the details).

## 5. What the model looks like

This is what the instructor will look at in **Model view**, and what the project already contains.

### Tables

| Table | Kind | Source |
| --- | --- | --- |
| `Key Measures` | measures only | 44 measures, no data |
| `dim_date` | dimension, marked as date table | `workspace.pkwiki_gold.dim_date` |
| `dim_wiki` | dimension | `workspace.pkwiki_gold.dim_wiki` |
| `dim_page` | dimension (SCD Type 2) | `workspace.pkwiki_gold.dim_page` |
| `dim_editor` | dimension (SCD Type 2) | `workspace.pkwiki_gold.dim_editor` |
| `dim_change_type` | dimension | `workspace.pkwiki_gold.dim_change_type` |
| `fact_revision` | fact | `workspace.pkwiki_gold.fact_revision` |
| `fact_page_event` | fact | `workspace.pkwiki_gold.fact_page_event` |
| `agg_wiki_daily` | aggregate fact | `workspace.pkwiki_gold.agg_wiki_daily` |
| `agg_revert_latency` | aggregate fact | `workspace.pkwiki_gold.agg_revert_latency` |
| `agg_edit_wars` | aggregate fact | `workspace.pkwiki_gold.agg_edit_wars` |
| `agg_editor_monthly` | aggregate fact | `workspace.pkwiki_gold.agg_editor_monthly` |
| `pipeline_execution_logs` | ops (audit / errors) | `workspace.pkwiki_ops.pipeline_execution_logs` |
| `quarantine` | ops (audit / errors) | `workspace.pkwiki_ops.quarantine` |
| `schema_drift_log` | ops (audit / errors) | `workspace.pkwiki_ops.schema_drift_log` |

All columns that are only keys (`*_key`, `page_sk`, `editor_sk`, ...) are hidden in the report view
so the field list stays clean. Auto date/time is turned off, so there are no hidden date tables.

### Relationships

All of them are one-to-many, single direction (the dimension filters the fact), as in the star
schema. The two `pkt_date_key` links are inactive because a table can only have one active path to
`dim_date`. Use them in a measure with `USERELATIONSHIP` if you want Pakistan days instead of UTC
days.

| From (many side) | To (one side) | Cardinality | Cross filter direction | Active |
| --- | --- | --- | --- | --- |
| `fact_revision[wiki_key]` | `dim_wiki[wiki_key]` | Many to one (*:1) | Single | Yes |
| `fact_revision[date_key]` | `dim_date[date_key]` | Many to one (*:1) | Single | Yes |
| `fact_revision[pkt_date_key]` | `dim_date[date_key]` | Many to one (*:1) | Single | No |
| `fact_revision[page_key]` | `dim_page[page_key]` | Many to one (*:1) | Single | Yes |
| `fact_revision[editor_key]` | `dim_editor[editor_key]` | Many to one (*:1) | Single | Yes |
| `fact_revision[change_type_key]` | `dim_change_type[change_type_key]` | Many to one (*:1) | Single | Yes |
| `fact_page_event[wiki_key]` | `dim_wiki[wiki_key]` | Many to one (*:1) | Single | Yes |
| `fact_page_event[date_key]` | `dim_date[date_key]` | Many to one (*:1) | Single | Yes |
| `fact_page_event[pkt_date_key]` | `dim_date[date_key]` | Many to one (*:1) | Single | No |
| `fact_page_event[page_key]` | `dim_page[page_key]` | Many to one (*:1) | Single | Yes |
| `fact_page_event[editor_key]` | `dim_editor[editor_key]` | Many to one (*:1) | Single | Yes |
| `fact_page_event[change_type_key]` | `dim_change_type[change_type_key]` | Many to one (*:1) | Single | Yes |
| `agg_wiki_daily[wiki_key]` | `dim_wiki[wiki_key]` | Many to one (*:1) | Single | Yes |
| `agg_wiki_daily[date_key]` | `dim_date[date_key]` | Many to one (*:1) | Single | Yes |
| `agg_revert_latency[wiki_key]` | `dim_wiki[wiki_key]` | Many to one (*:1) | Single | Yes |
| `agg_revert_latency[month_key]` | `dim_date[date_key]` | Many to one (*:1) | Single | Yes |
| `agg_edit_wars[wiki_key]` | `dim_wiki[wiki_key]` | Many to one (*:1) | Single | Yes |
| `agg_edit_wars[date_key]` | `dim_date[date_key]` | Many to one (*:1) | Single | Yes |
| `agg_edit_wars[page_key]` | `dim_page[page_key]` | Many to one (*:1) | Single | Yes |
| `agg_editor_monthly[wiki_key]` | `dim_wiki[wiki_key]` | Many to one (*:1) | Single | Yes |
| `agg_editor_monthly[month_key]` | `dim_date[date_key]` | Many to one (*:1) | Single | Yes |

The three ops tables (`pipeline_execution_logs`, `quarantine`, `schema_drift_log`) have no
relationships. They describe pipeline runs, not edits, so the Pipeline health page filters them with
its own slicers (layer, run, status).

We left `dim_page.wiki_key` and `dim_editor.wiki_key` without a relationship on purpose. Linking them
to `dim_wiki` too would give two filter paths from `dim_wiki` to the facts, which Power BI doesn't
allow (it would make one of them inactive anyway).

The first time you open Model view the tables may sit in a long row. Drag `fact_revision` and
`fact_page_event` to the middle and the `dim_` tables around them so it looks like a star before you
take the screenshot for the report. Power BI remembers the layout when you save.

### Date table

`dim_date` is marked as the date table, with `date` as the date column. To see it: select
`dim_date` in the Data pane, then **Table tools > Mark as date table**. It shows as marked, with
`date` chosen.

## 6. Building it by hand (if the project doesn't open)

This gives the same result as the project. Keep the order, it saves time.

### 6.1 Connection details and token

Do section 2, steps 2 to 4. Keep the Server Hostname, HTTP Path and token at hand.

### 6.2 Get data

1. Open Power BI Desktop with a blank report.
2. **File > Options and settings > Options > Current file > Data load**: untick
   **Auto date/time**, and under **Relationships** untick **Autodetect new relationships after data
   is loaded**. Click **OK**.
3. **Home > Get data > More...**, search for `Databricks`.
   - Our workspace is Databricks Free Edition (the host ends in `cloud.databricks.com`), so pick
     **Databricks**.
   - On an Azure workspace (host ends in `azuredatabricks.net`) pick **Azure Databricks**. The
     rest is the same.
4. Click **Connect**. Fill in:
   - **Server Hostname**: from `CONNECTION.md`
   - **HTTP Path**: from `CONNECTION.md`
   - **Data Connectivity mode**: **Import**
5. Click **OK**. Choose **Personal Access Token**, paste the token, click **Connect**.
6. In the Navigator open **workspace > pkwiki_gold** and tick these 11 tables:
   `dim_date`, `dim_wiki`, `dim_page`, `dim_editor`, `dim_change_type`, `fact_revision`,
   `fact_page_event`, `agg_wiki_daily`, `agg_revert_latency`, `agg_edit_wars`, `agg_editor_monthly`.
7. Still in the Navigator, open **workspace > pkwiki_ops** and tick `pipeline_execution_logs`,
   `quarantine` and `schema_drift_log`.
8. Click **Transform Data** (not Load). For a lighter model, in each table remove the columns
   `row_hash`, `batch_id` and `load_timestamp` (select them, **Home > Remove columns**), except in
   `quarantine` and `schema_drift_log` where we keep `load_timestamp`. In `quarantine` also remove
   `raw_payload`, and in `pipeline_execution_logs` remove `params_json`. This step is optional.
9. In `quarantine`, look at the `reasons` column. If it shows the word `List` in each cell, select
   it, **Transform > Extract values**, separator **Semicolon**, OK. If it already shows text, leave
   it.
10. **Home > Close & Apply** and wait for the load.

### 6.3 Relationships

1. Go to **Model view** (the third icon on the left).
2. **Home > Manage relationships > New...** and add each row of the relationships table in
   section 5. For each one: pick the fact table and its key column at the top, the dimension and
   its key column at the bottom, **Cardinality: Many to one (*:1)**, **Cross filter direction:
   Single**, and tick or untick **Make this relationship active** as in the table. Click **OK**.
3. If any relationship was created automatically and is not in the table, delete it.

Shortcut: you can also drag the fact column onto the dimension column in the diagram. Power BI
picks many to one and single direction by itself. Double click the line to check.

### 6.4 Mark the date table and set sort orders

1. In the Data pane select `dim_date`. **Table tools > Mark as date table**, choose `date`, **OK**.
2. Select the column `dim_date[month_name]`, **Column tools > Sort by column > month**.
3. Select `dim_date[day_name]`, **Column tools > Sort by column > day_of_week**.
4. Hide the key columns: in Model view right click each `*_key` column (and `page_sk`,
   `editor_sk`, `event_key`, `pipeline_execution_logs[log_id]` and `quarantine[record_hash]`) and
   choose **Hide in report view**.

### 6.5 Measure table, calculated columns and measures

1. **Home > Enter data**. Name the table `Key Measures`, leave the one column as it is, click
   **Load**. (After you add the first measure you can hide `Column1`.)
2. Calculated columns. Select `fact_revision` in the Data pane, **Table tools > New column**, paste
   the first formula, press Enter. Then the second one.

   **fact_revision[Temp Account Era]**: Before or after the first temporary account edit we have for that wiki (Q5).

   ```dax
   Temp Account Era =
       VAR rollout = RELATED ( dim_wiki[temp_accounts_first_seen_ts] )
       RETURN
           IF (
               ISBLANK ( rollout ),
               "No temp accounts yet",
               IF ( fact_revision[rev_ts] < rollout, "Before temp accounts", "After temp accounts" )
           )
   ```

   **fact_revision[Temp Account Era Order]**: Sort order for Temp Account Era.

   ```dax
   Temp Account Era Order =
       VAR rollout = RELATED ( dim_wiki[temp_accounts_first_seen_ts] )
       RETURN
           IF ( ISBLANK ( rollout ), 3, IF ( fact_revision[rev_ts] < rollout, 1, 2 ) )
   ```

3. Select `fact_revision[Temp Account Era]`, **Column tools > Sort by column > Temp Account Era
   Order**, then hide `Temp Account Era Order`.
4. Measures. Select `Key Measures` in the Data pane, **Table tools > New measure**, paste one
   formula, press Enter, then set the format under **Measure tools** (the format is listed with
   each measure). Repeat for each measure below. Putting them in display folders is optional
   (Model view, select the measure, **Properties > Display folder**).

#### Edits

**Total Edits**. Number of edits (revisions) in the current filter. Format: Whole number with thousands separator.

```dax
Total Edits = COUNTROWS ( fact_revision )
```

**Human Edits**. Edits not made by bots. Format: Whole number with thousands separator.

```dax
Human Edits = CALCULATE ( [Total Edits], fact_revision[is_bot_edit] = FALSE () )
```

**Bot Edits**. Edits made by bot accounts. Format: Whole number with thousands separator.

```dax
Bot Edits = CALCULATE ( [Total Edits], fact_revision[is_bot_edit] = TRUE () )
```

**Bot Edit Share**. Share of edits made by bots (Q4). Format: Percentage, 1 decimal.

```dax
Bot Edit Share = DIVIDE ( [Bot Edits], [Total Edits] )
```

**Anonymous Edits**. Edits by IP editors or temporary accounts. Format: Whole number with thousands separator.

```dax
Anonymous Edits = CALCULATE ( [Total Edits], fact_revision[is_anonymous_edit] = TRUE () )
```

**Anonymous Share**. Share of edits made by IP editors or temporary accounts (Q5). Format: Percentage, 1 decimal.

```dax
Anonymous Share = DIVIDE ( [Anonymous Edits], [Total Edits] )
```

**New Articles**. Page creations in the article namespace that are not redirects (Q1). Format: Whole number with thousands separator.

```dax
New Articles = CALCULATE ( [Total Edits], fact_revision[is_new_article] = TRUE () )
```

**Pages Created**. Page creations in any namespace. Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Pages Created = CALCULATE ( [Total Edits], fact_revision[is_page_creation] = TRUE () )
```

**Net Bytes Added**. Bytes added minus bytes removed. Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Net Bytes Added = SUM ( fact_revision[bytes_diff] )
```

#### Editors

**Active Editors**. Editors with at least one edit in the current filter. One person counts once per wiki, the unknown editor row is left out. Format: Whole number with thousands separator.

```dax
Active Editors =
    COUNTROWS (
        FILTER (
            SUMMARIZE ( fact_revision, dim_editor[wiki], dim_editor[editor_hash] ),
            dim_editor[editor_hash] <> "(unknown editor)"
        )
    )
```

**Active Editors 5+**. Editors with 5 or more edits in the current filter, the usual Wikimedia definition of active (Q1). Format: Whole number with thousands separator.

```dax
Active Editors 5+ =
    COUNTROWS (
        FILTER (
            SUMMARIZE ( fact_revision, dim_editor[wiki], dim_editor[editor_hash] ),
            dim_editor[editor_hash] <> "(unknown editor)"
                && CALCULATE ( COUNTROWS ( fact_revision ) ) >= 5
        )
    )
```

**Human Editors**. Active editors that are not bots. Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Human Editors = CALCULATE ( [Active Editors], fact_revision[is_bot_edit] = FALSE () )
```

**New Editors**. Editors whose first edit we have falls in the month (from agg_editor_monthly). Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
New Editors = SUM ( agg_editor_monthly[new_editors] )
```

**Next Month Retention**. Share of a month's editors who edited again the next month (from agg_editor_monthly). Format: Percentage, 1 decimal. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Next Month Retention = DIVIDE ( SUM ( agg_editor_monthly[returning_next_month] ), SUM ( agg_editor_monthly[editors] ) )
```

#### Reverts and vandalism

**Reverts**. Edits that restored an earlier version of the page. Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Reverts = CALCULATE ( [Total Edits], fact_revision[is_revert] = TRUE () )
```

**Reverted Edits**. Edits that were later undone. Format: Whole number with thousands separator.

```dax
Reverted Edits = COALESCE ( CALCULATE ( [Total Edits], fact_revision[is_reverted] = TRUE () ), 0 )
```

**Revert Rate**. Share of edits that were later undone. Format: Percentage, 1 decimal.

```dax
Revert Rate = DIVIDE ( [Reverted Edits], [Total Edits] )
```

**Vandalism Suspects**. Edits reverted within 24 hours by a different editor. Format: Whole number with thousands separator.

```dax
Vandalism Suspects = COALESCE ( CALCULATE ( [Total Edits], fact_revision[is_vandalism_suspect] = TRUE () ), 0 )
```

**Vandalism Rate**. Share of edits that are vandalism suspects. Format: Percentage, 1 decimal.

```dax
Vandalism Rate = DIVIDE ( [Vandalism Suspects], [Total Edits] )
```

**Median Minutes to Revert**. Median time in minutes before a reverted edit was undone. Format: Decimal number, 1 decimal. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Median Minutes to Revert =
    VAR reverted =
        FILTER ( fact_revision, NOT ISBLANK ( fact_revision[seconds_to_revert] ) )
    RETURN
        IF ( ISEMPTY ( reverted ), BLANK (), MEDIANX ( reverted, fact_revision[seconds_to_revert] ) / 60 )
```

**Median Minutes to Undo Vandalism**. Median time in minutes before a vandalism suspect was undone (Q2). Format: Decimal number, 1 decimal.

```dax
Median Minutes to Undo Vandalism =
    VAR suspects =
        FILTER (
            fact_revision,
            fact_revision[is_vandalism_suspect] = TRUE ()
                && NOT ISBLANK ( fact_revision[seconds_to_revert] )
        )
    RETURN
        IF ( ISEMPTY ( suspects ), BLANK (), MEDIANX ( suspects, fact_revision[seconds_to_revert] ) / 60 )
```

**P90 Minutes to Undo Vandalism**. 90th percentile of the time in minutes before a vandalism suspect was undone (Q2). Format: Decimal number, 1 decimal.

```dax
P90 Minutes to Undo Vandalism =
    VAR suspects =
        FILTER (
            fact_revision,
            fact_revision[is_vandalism_suspect] = TRUE ()
                && NOT ISBLANK ( fact_revision[seconds_to_revert] )
        )
    RETURN
        IF (
            ISEMPTY ( suspects ),
            BLANK (),
            PERCENTILEX.INC ( suspects, fact_revision[seconds_to_revert], 0.9 ) / 60
        )
```

#### Edit wars

**Contested Days**. Page days with 2 or more reverts (Q3). Format: Whole number with thousands separator.

```dax
Contested Days = COUNTROWS ( agg_edit_wars )
```

**Contested Reverts**. Reverts made on contested page days (Q3). Format: Whole number with thousands separator.

```dax
Contested Reverts = SUM ( agg_edit_wars[revert_count] )
```

**Contested Pages**. Pages that had at least one day with 2 or more reverts (Q3). Format: Whole number with thousands separator.

```dax
Contested Pages = COALESCE ( COUNTROWS ( SUMMARIZE ( agg_edit_wars, agg_edit_wars[wiki_key], agg_edit_wars[page_id] ) ), 0 )
```

**Edit War Days**. Page days that meet the Phase 1 edit war rule: 3 or more reverts by 2 or more editors (Q3). Format: Whole number with thousands separator.

```dax
Edit War Days = CALCULATE ( COUNTROWS ( agg_edit_wars ), agg_edit_wars[is_edit_war] = TRUE () )
```

**Edit War Pages**. Pages with at least one edit war day under the Phase 1 rule (Q3). Format: Whole number with thousands separator.

```dax
Edit War Pages =
    COALESCE (
        CALCULATE (
            COUNTROWS ( SUMMARIZE ( agg_edit_wars, agg_edit_wars[wiki_key], agg_edit_wars[page_id] ) ),
            agg_edit_wars[is_edit_war] = TRUE ()
        )
        , 0
    )
```

**Max Editors Involved**. Most editors involved on a single contested day of the page. Format: Whole number with thousands separator.

```dax
Max Editors Involved = MAX ( agg_edit_wars[distinct_editors] )
```

#### Page events

**Page Events**. Page log events (moves, deletions, restores, protections, ...). Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Page Events = COUNTROWS ( fact_page_event )
```

**Pages Deleted**. Page deletions. Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Pages Deleted = CALCULATE ( [Page Events], dim_change_type[entity] = "page_event", dim_change_type[change_type] = "delete" )
```

**Page Moves**. Page moves (renames). Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Page Moves = CALCULATE ( [Page Events], dim_change_type[entity] = "page_event", dim_change_type[change_type] = "move" )
```

#### Pipeline

**Pipeline Runs**. Number of pipeline runs (distinct run ids) in the audit log. Format: Whole number with thousands separator.

```dax
Pipeline Runs = COALESCE ( DISTINCTCOUNT ( pipeline_execution_logs[run_id] ), 0 )
```

**Steps Logged**. Rows in the audit log, one per step per run. Format: Whole number with thousands separator.

```dax
Steps Logged = COUNTROWS ( pipeline_execution_logs )
```

**Successful Steps**. Steps that finished with status SUCCESS. Format: Whole number with thousands separator.

```dax
Successful Steps = COALESCE ( CALCULATE ( [Steps Logged], pipeline_execution_logs[status] = "SUCCESS" ), 0 )
```

**Failed Steps**. Steps that finished with status FAILED. Format: Whole number with thousands separator.

```dax
Failed Steps = COALESCE ( CALCULATE ( [Steps Logged], pipeline_execution_logs[status] = "FAILED" ), 0 )
```

**Skipped Steps**. Steps that had nothing to do (status SKIPPED). Format: Whole number with thousands separator. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Skipped Steps = COALESCE ( CALCULATE ( [Steps Logged], pipeline_execution_logs[status] = "SKIPPED" ), 0 )
```

**Pipeline Success Rate**. Successful steps out of the steps that actually ran (skipped steps are left out). Format: Percentage, 1 decimal.

```dax
Pipeline Success Rate = DIVIDE ( [Successful Steps], [Successful Steps] + [Failed Steps] )
```

**Rows Processed**. Rows the steps read from their source. Format: Whole number with thousands separator.

```dax
Rows Processed = SUM ( pipeline_execution_logs[rows_read] )
```

**Rows Written**. Rows inserted, updated or deleted by the MERGEs. Format: Whole number with thousands separator.

```dax
Rows Written =
    SUM ( pipeline_execution_logs[rows_inserted] )
        + SUM ( pipeline_execution_logs[rows_updated] )
        + SUM ( pipeline_execution_logs[rows_deleted] )
```

**Avg Step Duration (sec)**. Average step duration in seconds. Format: Decimal number, 1 decimal.

```dax
Avg Step Duration (sec) = AVERAGE ( pipeline_execution_logs[duration_sec] )
```

**Total Run Time (min)**. Sum of all step durations in minutes. Format: Decimal number, 1 decimal. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Total Run Time (min) = DIVIDE ( SUM ( pipeline_execution_logs[duration_sec] ), 60 )
```

**Last Run**. When the latest step in the audit log finished (UTC). Format: Custom: `yyyy-mm-dd hh:nn`. *(Optional: not used on a page. Skip it if you are short on time.)*

```dax
Last Run = MAX ( pipeline_execution_logs[end_ts] )
```

**Rows Quarantined**. Rows sitting in the quarantine table. Format: Whole number with thousands separator.

```dax
Rows Quarantined = COALESCE ( COUNTROWS ( quarantine ), 0 )
```

**Drift Events**. Schema drift entries the pipeline logged. Format: Whole number with thousands separator.

```dax
Drift Events = COALESCE ( COUNTROWS ( schema_drift_log ), 0 )
```

### 6.6 Theme

**View > Themes > Browse for themes**, pick `dashboard/theme.json`, **Open**. It sets the colours
(one colour per wiki in the charts), white tiles with a light grey border and rounded corners on a
light grey page, and Segoe UI text.

### 6.7 Pages and visuals

Page size stays the default 16:9 (1280 x 720). The positions below are in pixels (x, y, width,
height) and are the same as in the project, so you can type them in **Format > General >
Properties** if you want it exact. Roughly is fine too.

How to add a visual: click an empty spot on the page, pick the visual type in the
**Visualizations** pane, then drag the fields into the wells named in the table. The title is set
under **Format visual > General > Title**. To sort, click the three dots on the visual,
**Sort axis**, pick the field and **Sort ascending** or **Sort descending**.

Slicers: every page except Pipeline health has the same three slicers at the top right (wiki,
editor type, date range). Make them once on page 1, copy and paste them onto pages 2 and 3, and
choose **Sync** when Power BI asks, so a selection on one page carries over to the others.
Under **Format visual > Visual > Slicer settings > Options > Style** set the first two to
**Dropdown**. The date slicer uses **Between**.

Text boxes for the page titles: **Insert > Text box**, title in 20 pt Segoe UI Semibold, the
subtitle under it in 11 pt grey.

#### Page 1: Overview

| Tile | Visual | Fields | Sort | Position (x, y, w, h) |
| --- | --- | --- | --- | --- |
| Page title | Text box | "Knowledge Integrity Monitor / Edits, editors and reverts on the Urdu, Punjabi, Sindhi and Pashto Wikipedias" | | 16, 8, 640, 60 |
| Wiki (Dropdown) | Slicer | Field: `dim_wiki[language_name]` |  | 664, 8, 196, 60 |
| Editor type (Dropdown) | Slicer | Field: `dim_editor[editor_type]` |  | 868, 8, 186, 60 |
| Date range (Between) | Slicer | Field: `dim_date[date]` |  | 1062, 8, 202, 60 |
| KPI card | Card | Fields: `[Total Edits]` |  | 16, 76, 201, 96 |
| KPI card | Card | Fields: `[Active Editors]` |  | 225, 76, 201, 96 |
| KPI card | Card | Fields: `[New Articles]` |  | 434, 76, 201, 96 |
| KPI card | Card | Fields: `[Revert Rate]` |  | 643, 76, 201, 96 |
| KPI card | Card | Fields: `[Vandalism Rate]` |  | 852, 76, 201, 96 |
| KPI card | Card | Fields: `[Anonymous Share]` |  | 1061, 76, 201, 96 |
| Edits per month by wiki | Line chart | X-axis: `dim_date[year_month]`; Y-axis: `[Total Edits]`; Legend: `dim_wiki[language_name]` | dim_date[year_month] ascending | 16, 184, 828, 260 |
| Edits by editor type | Donut chart | Legend: `dim_editor[editor_type]`; Values: `[Total Edits]` | [Total Edits] descending | 852, 184, 412, 260 |
| New articles per month (Q1) | Line chart | X-axis: `dim_date[year_month]`; Y-axis: `[New Articles]`; Legend: `dim_wiki[language_name]` | dim_date[year_month] ascending | 16, 452, 620, 252 |
| Active editors (5+ edits) per month (Q1) | Line chart | X-axis: `dim_date[year_month]`; Y-axis: `[Active Editors 5+]`; Legend: `dim_wiki[language_name]` | dim_date[year_month] ascending | 644, 452, 620, 252 |

#### Page 2: Wikis and bots

| Tile | Visual | Fields | Sort | Position (x, y, w, h) |
| --- | --- | --- | --- | --- |
| Page title | Text box | "Wikis and bots / How the four wikis compare, and how much of the work bots do (Q4)" | | 16, 8, 640, 60 |
| Wiki (Dropdown) | Slicer | Field: `dim_wiki[language_name]` |  | 664, 8, 196, 60 |
| Editor type (Dropdown) | Slicer | Field: `dim_editor[editor_type]` |  | 868, 8, 186, 60 |
| Date range (Between) | Slicer | Field: `dim_date[date]` |  | 1062, 8, 202, 60 |
| Edits by wiki | Clustered bar chart | Y-axis: `dim_wiki[language_name]`; X-axis: `[Total Edits]` | [Total Edits] descending | 16, 76, 400, 300 |
| Human vs bot edits by wiki | 100% stacked bar chart | Y-axis: `dim_wiki[language_name]`; X-axis: `[Human Edits]`, `[Bot Edits]` | [Human Edits] descending | 424, 76, 420, 300 |
| Active editors (5+ edits) by wiki | Clustered column chart | X-axis: `dim_wiki[language_name]`; Y-axis: `[Active Editors 5+]` | [Active Editors 5+] descending | 852, 76, 412, 300 |
| Human vs bot edits per month (Q4) | 100% stacked column chart | X-axis: `dim_date[year_month]`; Y-axis: `[Human Edits]`, `[Bot Edits]` | dim_date[year_month] ascending | 16, 384, 620, 320 |
| Wiki scorecard | Table | Columns: `dim_wiki[language_name]`, `[Total Edits]`, `[Active Editors]`, `[New Articles]`, `[Bot Edit Share]`, `[Anonymous Share]`, `[Revert Rate]` | [Total Edits] descending | 644, 384, 620, 320 |

#### Page 3: Vandalism and reverts

| Tile | Visual | Fields | Sort | Position (x, y, w, h) |
| --- | --- | --- | --- | --- |
| Page title | Text box | "Vandalism and reverts / How fast bad edits get undone (Q2), edit wars (Q3) and temporary accounts (Q5)" | | 16, 8, 640, 60 |
| Wiki (Dropdown) | Slicer | Field: `dim_wiki[language_name]` |  | 664, 8, 196, 60 |
| Editor type (Dropdown) | Slicer | Field: `dim_editor[editor_type]` |  | 868, 8, 186, 60 |
| Date range (Between) | Slicer | Field: `dim_date[date]` |  | 1062, 8, 202, 60 |
| KPI card | Card | Fields: `[Reverted Edits]` |  | 16, 76, 201, 96 |
| KPI card | Card | Fields: `[Revert Rate]` |  | 225, 76, 201, 96 |
| KPI card | Card | Fields: `[Vandalism Suspects]` |  | 434, 76, 201, 96 |
| KPI card | Card | Fields: `[Median Minutes to Undo Vandalism]` |  | 643, 76, 201, 96 |
| KPI card | Card | Fields: `[Contested Pages]` |  | 852, 76, 201, 96 |
| KPI card | Card | Fields: `[Edit War Pages]` |  | 1061, 76, 201, 96 |
| Minutes to undo vandalism by editor type (Q2) | Clustered column chart | X-axis: `dim_editor[editor_type]`; Y-axis: `[Median Minutes to Undo Vandalism]`, `[P90 Minutes to Undo Vandalism]` | [Median Minutes to Undo Vandalism] descending | 16, 184, 412, 260 |
| Revert rate and vandalism rate per month | Line chart | X-axis: `dim_date[year_month]`; Y-axis: `[Revert Rate]`, `[Vandalism Rate]` | dim_date[year_month] ascending | 436, 184, 412, 260 |
| Before vs after temporary accounts (Q5) | Clustered column chart | X-axis: `fact_revision[Temp Account Era]`; Y-axis: `[Anonymous Share]`, `[Vandalism Rate]` | fact_revision[Temp Account Era] ascending | 856, 184, 408, 260 |
| Most contested pages (Q3) | Table | Columns: `dim_page[page_title]`, `dim_page[wiki]`, `dim_page[namespace_name]`, `[Contested Reverts]`, `[Contested Days]`, `[Edit War Days]`, `[Max Editors Involved]` | [Contested Reverts] descending | 16, 452, 828, 252 |
| Vandalism rate by wiki | Clustered bar chart | Y-axis: `dim_wiki[language_name]`; X-axis: `[Vandalism Rate]` | [Vandalism Rate] descending | 852, 452, 412, 252 |

#### Page 4: Pipeline health

| Tile | Visual | Fields | Sort | Position (x, y, w, h) |
| --- | --- | --- | --- | --- |
| Page title | Text box | "Pipeline health / Audit log, quarantine and schema drift from the pkwiki_ops tables" | | 16, 8, 640, 60 |
| Layer (Dropdown) | Slicer | Field: `pipeline_execution_logs[layer]` |  | 664, 8, 196, 60 |
| Run (Dropdown) | Slicer | Field: `pipeline_execution_logs[run_id]` |  | 868, 8, 196, 60 |
| Status (Dropdown) | Slicer | Field: `pipeline_execution_logs[status]` |  | 1072, 8, 192, 60 |
| KPI card | Card | Fields: `[Pipeline Runs]` |  | 16, 76, 149, 96 |
| KPI card | Card | Fields: `[Steps Logged]` |  | 173, 76, 149, 96 |
| KPI card | Card | Fields: `[Pipeline Success Rate]` |  | 330, 76, 149, 96 |
| KPI card | Card | Fields: `[Failed Steps]` |  | 487, 76, 149, 96 |
| KPI card | Card | Fields: `[Rows Processed]` |  | 644, 76, 149, 96 |
| KPI card | Card | Fields: `[Avg Step Duration (sec)]` |  | 801, 76, 149, 96 |
| KPI card | Card | Fields: `[Rows Quarantined]` |  | 958, 76, 149, 96 |
| KPI card | Card | Fields: `[Drift Events]` |  | 1115, 76, 149, 96 |
| Steps by layer and status | Stacked column chart | X-axis: `pipeline_execution_logs[layer]`; Y-axis: `[Steps Logged]`; Legend: `pipeline_execution_logs[status]` | [Steps Logged] descending | 16, 184, 412, 260 |
| Rows read and written by layer | Clustered column chart | X-axis: `pipeline_execution_logs[layer]`; Y-axis: `[Rows Processed]`, `[Rows Written]` | [Rows Processed] descending | 436, 184, 412, 260 |
| Average step duration by table (sec) | Clustered bar chart | Y-axis: `pipeline_execution_logs[target_table]`; X-axis: `[Avg Step Duration (sec)]` | [Avg Step Duration (sec)] descending | 856, 184, 408, 260 |
| Step log (newest first) | Table | Columns: `pipeline_execution_logs[start_ts]`, `pipeline_execution_logs[run_id]`, `pipeline_execution_logs[layer]`, `pipeline_execution_logs[target_table]`, `pipeline_execution_logs[status]`, `pipeline_execution_logs[rows_read]`, `pipeline_execution_logs[rows_inserted]`, `pipeline_execution_logs[rows_quarantined]`, `pipeline_execution_logs[duration_sec]` | pipeline_execution_logs[start_ts] descending | 16, 452, 828, 252 |
| Quarantined rows by reason | Clustered bar chart | Y-axis: `quarantine[reasons]`; X-axis: `[Rows Quarantined]` | [Rows Quarantined] descending | 852, 452, 412, 252 |

### 6.8 Check before saving

- Every visual shows data (with the sample data some charts have a gap in the years between the
  dump rows from 2002 to 2004 and the recent changes from 2026. That is the data, not a bug).
- Model view matches section 5.
- **File > Save as** `dashboard/pk_wiki_integrity.pbix`.

## 7. CSV by hand (no Databricks, no project)

Same as section 6, with these changes in 6.2:

1. **Home > Get data > Text/CSV**, pick `dashboard/sample_gold/dim_date.csv`, click
   **Transform Data**. Check that the first row became the headers. Repeat for every CSV in the
   folder (11 Gold tables, plus the ops tables if they are there).
2. Fix the types in Power Query, because CSV has none: key and count columns **Whole number**,
   `*_share`, `*_seconds_*`, `*_minutes*`, `duration_sec` **Decimal number**, `date` and
   `month_start_date` **Date**, every `*_ts` column **Date/Time**, every `is_*` column
   **True/False**, the rest **Text**. For a column, click the type icon left of the column name.
   If a date or time column shows errors, use **Change type > Using locale > English (United
   States)**.
3. **Close & Apply**, then carry on with 6.3.

## 8. Publish and export

1. Save the `.pbix` (step 3.9 or 6.8).
2. **Publish to the Power BI service** (needs a work or school account with a Power BI licence):
   **Home > Publish**, sign in, pick **My workspace**, **Select**. When it's done, click **Open
   in Power BI**. In the service, open the semantic model **Settings > Data source credentials**
   and enter the token again if you want scheduled refresh.
3. **Publish to web** (only if the tenant admin allows it, the university tenant may not): in the
   service open the report, **File > Embed report > Publish to web (public)**, **Create embed
   code**, **Publish**, and copy the link. Anyone with the link can see the report, so only do this
   with masked data like ours.
4. If publishing isn't possible, export instead: in Desktop **File > Export > Export to PDF**
   (one page per report page), and take a screenshot of each page and of Model view for the
   report (`Win + Shift + S`).
5. Commit the `.pbix` to `dashboard/` (it is small, the sample data is a few MB).

## 9. If something goes wrong

| Problem | Fix |
| --- | --- |
| "Unable to open document" or a PBIR error when opening the `.pbip` | Update Power BI Desktop (section 2.1). If it still fails, build by hand (section 6). |
| Refresh error about an empty catalog, or ADBC / driver errors | **Transform data**, open any table, **Advanced editor**, and in the `Databricks.Catalogs(...)` line change `Implementation = "2.0"` to `Implementation = null`. It has to be the same in all 14 tables, so it's quicker to edit the `.tmdl` files under `definition/tables/` with Notepad (find and replace) before opening the project. |
| Asked for credentials again and again, or "invalid credentials" | **File > Options and settings > Data source settings**, select the Databricks source, **Clear permissions**, refresh and enter a fresh token. |
| "The column 'x' of the table wasn't found" | The Gold schema changed. Check the table in Databricks against `docs/gold_data_dictionary.md`. Missing columns come in as empty, so this only happens if a table is missing completely. |
| Timeout on the first refresh | The serverless warehouse was starting. Refresh again. |
| CSV project shows "Couldn't find the file" or all visuals empty | The `SampleFolder` parameter is wrong. It must be the full path to `sample_gold` and end with `\`. |
| A chart says "Can't display the visual" | Usually a field was renamed. Click the visual and check the wells for a red field. |

## 10. Time needed

| Path | Time |
| --- | --- |
| Quick way (open the project, sign in, refresh, save as `.pbix`) | about 10 minutes |
| CSV project | about 5 minutes |
| By hand: connect and load (6.1 to 6.2) | 6 minutes |
| By hand: relationships, date table, sort orders (6.3 to 6.4) | 7 minutes |
| By hand: measures and calculated columns (6.5) | 10 minutes |
| By hand: theme and the four pages (6.6 to 6.7) | 15 minutes |
| By hand: checks and save (6.8) | 2 minutes |
| **By hand, total** | **about 40 minutes** |
