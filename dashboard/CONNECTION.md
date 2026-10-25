# Databricks connection for Power BI

What Power BI needs to reach our Gold tables. These values are already filled in as parameters in
`powerbi/databricks/Knowledge Integrity Monitor.pbip`.

| Setting | Value |
| --- | --- |
| Connector in Get data | **Databricks** (our host is Databricks Free Edition; for an Azure workspace use **Azure Databricks**) |
| Server Hostname | `dbc-f8a0f45c-f7da.cloud.databricks.com` |
| HTTP Path | `/sql/1.0/warehouses/9af9b3eed023027a` |
| Warehouse | Serverless Starter Warehouse |
| Catalog | `workspace` |
| Gold schema | `pkwiki_gold` (the 11 tables in `docs/GOLD-SCHEMA.md`) |
| Ops schema | `pkwiki_ops` (`pipeline_execution_logs`, `quarantine`, `schema_drift_log`) |
| Data connectivity mode | Import |
| Authentication | Personal access token |

The token is not stored anywhere in the repo. Each of us makes our own in Databricks (profile
picture > **Settings** > **Developer** > **Access tokens** > **Manage** > **Generate new token**)
and pastes it into Power BI the first time the report refreshes. Power BI keeps it in its own
credential store on that machine, not in the `.pbix` or the project files.

Where the values come from: in Databricks, **SQL Warehouses** > **Serverless Starter Warehouse** >
**Connection details**.

To point the report at another warehouse: **Home > Transform data > Edit parameters** in Power BI
and change `DatabricksHost` and `DatabricksHttpPath`.
