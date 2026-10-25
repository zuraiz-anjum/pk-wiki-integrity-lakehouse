# Gold schema (Phase 3)

Short reference for the Gold layer: table names, grain, keys and which Phase 1 question each table
is for. The full column list with types and descriptions is in `docs/gold_data_dictionary.md`.

Everything lives in the `pkwiki_gold` schema (`workspace.pkwiki_gold.*` on Databricks). It is a
star schema: two event facts and four aggregate facts around five shared dimensions.

## Business questions (from the Phase 1 proposal)

| # | Question |
| --- | --- |
| Q1 | Are the four wikis growing, in new articles and in active editors? |
| Q2 | How long does it take to undo vandalism, and is it different for registered, temporary and IP editors? |
| Q3 | Which pages are the most fought over, and what are they about? |
| Q4 | How much of each wiki is written by bots rather than people? |
| Q5 | Did anonymous editing or vandalism change after IP editing was replaced by temporary accounts? |
| extra | Heat map of edits by hour and day of week in Pakistan time |

## Key conventions

- Surrogate keys of the dimensions and event facts are `bigint` values made with
  `xxhash64(natural key)` with the sign bit cleared, so they are never negative. The same row always gets the same key, so a rerun or a MERGE never
  renumbers anything and facts can be loaded without looking up a sequence.
- `date_key` is an `int` in `yyyymmdd` form (20260927). `month_key` is the `date_key` of the first
  day of the month (20260901), so it joins to `dim_date` too.
- `wiki_key` and `change_type_key` are small fixed `int` codes.
- Every fact row finds a dimension row. When a page or editor version can't be found the fact gets
  the unknown member of its wiki, whose key is minus the `wiki_key` (`-1` for urwiki up to `-4` for
  pswiki). These four rows exist in both `dim_page` and `dim_editor`, so the wiki filter still
  works on them.
- No raw usernames or IPs anywhere. Editors are only known by the salted hash from Silver
  (`user_<hash>`, `ip_<hash>`, `tmp_<hash>`), bot accounts keep their public names. Edit summaries
  are not copied to Gold at all.

## Dimensions

| Table | Grain | PK | Main columns | Questions |
| --- | --- | --- | --- | --- |
| `dim_date` | one row per calendar day, 2001-01-01 to 2030-12-31 | `date_key` | date, year, quarter, month, month_name, year_month, month_start_date, day_of_month, day_of_week (1 = Monday), day_name, week_of_year, is_weekend, is_pk_public_holiday, holiday_name | all (time axis), extra (day of week) |
| `dim_wiki` | one row per wiki (4) | `wiki_key` | wiki (`urwiki`), language_name, language_native, language_code, script, site_url, temp_accounts_first_seen_ts | all (wiki filter), Q5 (rollout date seen in the data) |
| `dim_page` | one row per page version (SCD2, from `silver.page`) | `page_key` | page_sk (Silver key), wiki_key (FK), wiki, page_id, page_title, page_namespace, namespace_name, is_content_page, is_redirect, is_deleted, valid_from, valid_to, is_current | Q1, Q3 (what the page is) |
| `dim_editor` | one row per editor version (SCD2, from `silver.editor`) | `editor_key` | editor_sk (Silver key), wiki_key (FK), wiki, editor_hash (masked), editor_type (registered / temporary / ip / bot), is_bot, is_anonymous, registration_ts, first_seen_ts, valid_from, valid_to, is_current | Q2, Q4, Q5 |
| `dim_change_type` | one row per kind of change (12) | `change_type_key` | entity (`revision` / `page_event`), change_type, description | Q1, Q3 |

`dim_change_type` rows: revision `create`, `revert`, `minor`, `edit` (an edit gets the first that
applies, in that order) and page_event `create`, `move`, `delete`, `restore`, `protect`, `upload`,
`merge`, `other`.

## Event facts

### `fact_revision`
Grain: one row per edit (Silver `(wiki, rev_id)`). PK `revision_key`.

| Kind | Columns |
| --- | --- |
| FKs | `wiki_key` -> dim_wiki, `date_key` -> dim_date (UTC day), `pkt_date_key` -> dim_date (Pakistan day, role playing), `page_key` -> dim_page (version valid at edit time), `editor_key` -> dim_editor (version valid at edit time), `change_type_key` -> dim_change_type |
| Degenerate | `rev_id`, `page_id`, `parent_rev_id`, `reverted_by_rev_id`, `source` (dump / api) |
| Time | `rev_ts` (UTC), `rev_ts_pkt`, `rev_hour_pkt` (0 to 23) |
| Measures | `bytes`, `bytes_diff`, `bytes_added`, `bytes_removed`, `seconds_to_revert` |
| Flags | `is_bot_edit`, `is_anonymous_edit` (ip or temporary), `is_minor`, `is_page_creation`, `is_new_article` (namespace 0, not a redirect), `is_revert`, `is_reverted`, `is_vandalism_suspect` (reverted within 24 hours by a different editor), `is_deleted` |
| Lineage | `row_hash`, `batch_id`, `load_timestamp` |

Questions: Q1 to Q5 and the heat map (`pkt_date_key` -> day_of_week, `rev_hour_pkt`).

### `fact_page_event`
Grain: one row per page log event (Silver `(wiki, event_key)`). PK `page_event_key`.

| Kind | Columns |
| --- | --- |
| FKs | `wiki_key`, `date_key`, `pkt_date_key`, `page_key`, `editor_key` (who did it), `change_type_key` |
| Degenerate | `event_key`, `log_id`, `page_id`, `page_namespace`, `source` |
| Time | `event_ts`, `event_ts_pkt` |
| Lineage | `row_hash`, `batch_id`, `load_timestamp` |

Questions: Q1 (deleted / restored pages), Q3 (moves and protections on contested pages).

## Aggregate facts

Built from the two event facts, so they always agree with them. Each one is recomputed only for the
days or months the current run touched and merged in.

| Table | Grain | PK | Main measures | FKs | Questions |
| --- | --- | --- | --- | --- | --- |
| `agg_wiki_daily` | wiki x day | `wiki_key, date_key` | edits, human_edits, bot_edits, bot_edit_share, anonymous_edits, new_articles, pages_created, pages_deleted, pages_restored, pages_moved, active_editors, active_human_editors, reverts, reverted_edits, vandalism_suspects, median_seconds_to_revert, bytes_added, bytes_removed, net_bytes | wiki_key, date_key | Q1, Q4, Q5 |
| `agg_revert_latency` | wiki x month x editor_type (of the reverted edit) | `wiki_key, month_key, editor_type` | reverted_edits, vandalism_suspects, median_seconds_to_revert, p90_seconds_to_revert, avg_seconds_to_revert, median_minutes_to_revert | wiki_key, month_key -> dim_date | Q2, Q5 |
| `agg_edit_wars` | page x UTC day, only page-days with 2+ reverts (contested pages). `is_edit_war` marks the Phase 1 rule, 3+ reverts by 2+ different editors | `wiki_key, page_id, date_key` | revert_count, distinct_reverters, reverted_edits, edits, distinct_editors, first_revert_ts, last_revert_ts, span_minutes, is_edit_war | wiki_key, date_key, page_key -> dim_page (version at the last revert) | Q3 |
| `agg_editor_monthly` | wiki x month x editor group (`human` / `bot`) | `wiki_key, month_key, editor_group` | editors, active_editors (5+ edits), very_active_editors (100+), new_editors (first edit ever that month), returning_next_month, active_returning_next_month, edits | wiki_key, month_key -> dim_date | Q1, Q4 |

## Refresh behaviour (short version)

Gold runs as the last step of every run (daily, backfill and full), right after the Silver SCD2
step.

- Dimensions and event facts are MERGEd. Incremental runs only read Silver rows whose
  `load_timestamp` is newer than the last Gold run (kept per table in `pkwiki_ops.gold_watermark`),
  plus any fact rows that still point at an unknown member. A full load merges against all of Silver
  and also deletes Gold rows that Silver no longer has (the monthly dump handover).
- `dim_date`, `dim_wiki` and `dim_change_type` are seeded with MERGE (insert if missing), so a
  rerun changes nothing. `dim_wiki.temp_accounts_first_seen_ts` is updated in place (Type 1).
- Aggregates are recomputed for the touched days / months only and MERGEd in, with groups that no
  longer exist removed. A full load recomputes everything.
- Every Gold table write is one row in `pkwiki_ops.pipeline_execution_logs` with
  `layer = 'silver_to_gold'`; OPTIMIZE / clustering steps use `layer = 'gold_optimize'`.

## Relationships (for Power BI)

```
dim_wiki.wiki_key        1 -> * fact_revision, fact_page_event, agg_*, dim_page, dim_editor
dim_date.date_key        1 -> * fact_revision.date_key, fact_page_event.date_key, agg_wiki_daily.date_key, agg_edit_wars.date_key
dim_date.date_key        1 -> * fact_revision.pkt_date_key, fact_page_event.pkt_date_key   (inactive in Power BI, use USERELATIONSHIP)
dim_date.date_key        1 -> * agg_revert_latency.month_key, agg_editor_monthly.month_key
dim_page.page_key        1 -> * fact_revision, fact_page_event, agg_edit_wars
dim_editor.editor_key    1 -> * fact_revision, fact_page_event
dim_change_type.change_type_key 1 -> * fact_revision, fact_page_event
```
