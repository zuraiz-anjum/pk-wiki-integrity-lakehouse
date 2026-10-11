Deliberately broken pswiki files for showing schema drift handling. Run them into a throwaway prefix (`--prefix pkwiki_drift --landing_root sample_data/drift_demo`) so the real tables stay clean.

- `pswiki_drift_demo.jsonl` (ingest_date 2026-09-28): lines 1-2 are normal, line 3 has a new field `rc_quality_score` (loads fine, logged as new_field), line 4 has `revid: "abc123"` and line 5 `oldlen: "big"` (type_mismatch, quarantined), line 6 is cut off halfway (malformed_json, quarantined).
- `pswiki_drift_demo.tsv` (snapshot 2026-08): rows 1-2 are normal, rows 3-4 have a 79th column and row 6 only 40 (column_count, quarantined), row 5 has `revision_id = not_a_number` (splits fine, quarantined in silver as "bad revision_id").
