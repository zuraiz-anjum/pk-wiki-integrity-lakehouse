# Wikipedia Knowledge Integrity Lakehouse for Pakistan's Languages

Semester project (Phase 1). An automated Spark / Delta Lake pipeline on Databricks that tracks
edits, new pages, deletions and reverts on the Urdu, Punjabi (Shahmukhi), Sindhi and Pashto
Wikipedias, modelled through Bronze, Silver and Gold layers and served to a Power BI dashboard.

**Team:** [Member 1 name, Roll No.] · [Member 2 name, Roll No.]

## Data sources
| Load | Source |
| --- | --- |
| Full | Wikimedia MediaWiki History dumps, `https://dumps.wikimedia.org/other/mediawiki_history/` |
| Incremental | MediaWiki Action API `list=recentchanges`, pulled daily from a saved watermark |

## Repository layout
| Path | What it is |
| --- | --- |
| `docs/phase1_proposal.pdf` | Phase 1 project proposal |
| `ingestion/ingest_wikimedia.py` | Full-load and incremental extractor |
| `common/pii_policy.py` | PII masking (hash IPs, usernames, temporary accounts; mask edit summaries) |
| `sample_data/raw/mediawiki_history/load_type=full/` | Full-load sample: 5,000 rows per wiki, snapshot 2026-08 |
| `sample_data/raw/recentchanges/load_type=incremental/` | Incremental sample: 24 hours of changes (17,405 events), 27 Sep 2026 |
| `notebooks/`, `dashboard/` | Phase 2 and 3 |

All sample files are **PII-masked**: editor names, IP addresses and temporary account names are
replaced by salted SHA-256 hashes (bot accounts are kept). The salt is never committed.

## Reproduce the samples
```bash
pip install requests
export WIKI_CONTACT_EMAIL=you@example.com
python ingestion/ingest_wikimedia.py full --snapshot 2026-08 --sample-lines 5000 --out sample_data/raw_unmasked
python ingestion/ingest_wikimedia.py incremental --out sample_data/raw_unmasked
PII_SALT=<long random string> python common/pii_policy.py sample_data/raw_unmasked sample_data/raw
```
