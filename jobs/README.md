# Daily job

`pkwiki_pipeline.json` is the job definition (Jobs API 2.1): bronze -> silver -> scd2 -> gold, each task one of the notebooks, run on serverless (no cluster in the file). The notebooks come straight from the GitHub repo (`git_url`), so whatever is on `main` is what runs. Schedule is 21:00 UTC = 2 AM PKT, created **paused**, unpause it in the UI when you want it to run on its own.

Create it either in the UI (Jobs & Pipelines -> Create -> Job, then the `...` menu -> "Edit as JSON" and paste the file) or with the CLI:

```
databricks jobs create --json @jobs/pkwiki_pipeline.json
```

That prints the `job_id`. All tasks get the same job parameters, and `run_id` defaults to `{{job.run_id}}` so the four steps share one id in `ops.pipeline_execution_logs`. The `gold` task (notebook `04_gold_star`) runs last, so every daily, backfill or full run ends with Gold refreshed from whatever Silver just wrote.

Triggering it (with `--json` the job id goes inside the JSON, not as an argument):

```
# standard daily run, newest two landed days
databricks jobs run-now --json '{"job_id": 123}'

# backfill a date range
databricks jobs run-now --json '{"job_id": 123, "job_parameters": {"start_date": "2026-09-20", "end_date": "2026-09-27"}}'

# full load from the monthly dump
databricks jobs run-now --json '{"job_id": 123, "job_parameters": {"load_type": "full", "snapshot": "2026-08"}}'
```

In PowerShell the quoting gets annoying, easier to put the JSON in a file and pass `--json @backfill.json`. In the UI it's "Run now with different parameters".

Extraction (`ingestion/ingest_wikimedia.py`) isn't a task yet. We run it from a laptop and upload into the volume, since we weren't sure Free Edition serverless can reach the Wikimedia API. If it can, it can be added as a first task writing to `/Volumes/workspace/pkwiki_raw/landing` with `--state` pointing at a file in the volume.
