"""
Pakistan Language Wikipedias - Knowledge Integrity Lakehouse: raw ingestion (Bronze landing).

FULL LOAD   MediaWiki History dump (monthly, whole history since 2001, TSV.bz2)
            https://dumps.wikimedia.org/other/mediawiki_history/<snapshot>/<wiki>/<snapshot>.<wiki>.all-time.tsv.bz2
INCREMENTAL MediaWiki Action API list=recentchanges per wiki, from the saved watermark to now
            (edits, page creations and log events: deletions, moves, blocks, protections)

Usage
  python ingest_wikimedia.py full --snapshot 2026-08 --sample-lines 5000     # sample
  python ingest_wikimedia.py full --snapshot 2026-08                          # real baseline
  python ingest_wikimedia.py incremental                                      # daily run
  python ingest_wikimedia.py incremental --start 2026-09-20                   # re-pull a past window
"""
import argparse, bz2, datetime as dt, json, os, pathlib
import requests

WIKIS = {"urwiki": "ur", "pnbwiki": "pnb", "sdwiki": "sd", "pswiki": "ps"}
DUMP = "https://dumps.wikimedia.org/other/mediawiki_history/{s}/{w}/{s}.{w}.all-time.tsv.bz2"
# Contact e-mail comes from an env var / Databricks secret so it is not published in the repo
CONTACT = os.getenv("WIKI_CONTACT_EMAIL", "set-WIKI_CONTACT_EMAIL")
UA = {"User-Agent": f"PK-Wiki-Lakehouse/0.1 (university data engineering project; contact: {CONTACT})"}
RC_PROPS = "title|ids|sizes|flags|user|userid|timestamp|comment|sha1|loginfo|tags|redirect"


def utcnow():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def api_ts(value):
    # 2026-09-20 or 2026-09-20T06:00:00Z -> the form the api wants
    t = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def full_load(snapshot, out, sample_lines):
    for wiki in WIKIS:
        url = DUMP.format(s=snapshot, w=wiki)
        folder = pathlib.Path(out, "mediawiki_history", "load_type=full", f"snapshot={snapshot}", f"wiki={wiki}")
        folder.mkdir(parents=True, exist_ok=True)
        with requests.get(url, headers=UA, stream=True, timeout=120) as r:
            r.raise_for_status()
            if not sample_lines:  # real run: keep the compressed file, Spark reads .bz2 in parallel splits
                with open(folder / f"{wiki}.all-time.tsv.bz2", "wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
                print(f"{wiki}: downloaded full dump"); continue
            # sample run: decompress on the fly and keep only the first N rows
            dec, buf, lines = bz2.BZ2Decompressor(), b"", []
            for chunk in r.iter_content(1 << 16):
                buf += dec.decompress(chunk)
                *done, buf = buf.split(b"\n")
                lines += done
                if len(lines) >= sample_lines:
                    break
            (folder / f"{wiki}_sample.tsv").write_bytes(b"\n".join(lines[:sample_lines]) + b"\n")
            print(f"{wiki}: wrote {min(len(lines), sample_lines)} sample rows")


def incremental(out, max_rows, state_file="watermarks.json", start=None):
    state_file = pathlib.Path(state_file)
    state = json.loads(state_file.read_text()) if state_file.exists() else {}
    run_end = utcnow()
    for wiki, lang in WIKIS.items():
        # --start wins over the watermark (backfill), otherwise carry on from the last run
        since = start or state.get(wiki, (run_end - dt.timedelta(days=1)).isoformat().replace("+00:00", "Z"))
        params = {"action": "query", "list": "recentchanges", "format": "json", "formatversion": 2,
                  "rcdir": "newer", "rcstart": since, "rcend": run_end.isoformat().replace("+00:00", "Z"),
                  "rcprop": RC_PROPS, "rclimit": 500}
        rows, api = [], f"https://{lang}.wikipedia.org/w/api.php"
        while True:
            r = requests.get(api, params=params, headers=UA, timeout=60)
            r.raise_for_status()  # fail loudly on 403/5xx instead of a confusing JSON error
            data = r.json()
            rows += data["query"]["recentchanges"]
            if "continue" not in data or (max_rows and len(rows) >= max_rows):
                break
            params.update(data["continue"])
        folder = pathlib.Path(out, "recentchanges", "load_type=incremental",
                              f"ingest_date={run_end.date()}", f"wiki={wiki}")
        folder.mkdir(parents=True, exist_ok=True)
        with open(folder / f"{wiki}_{run_end:%H%M%S}.jsonl", "w", encoding="utf-8") as f:
            for rc in rows:  # envelope keeps lineage next to the untouched record
                f.write(json.dumps({"_wiki": wiki, "_source": api, "_ingested_at": run_end.isoformat(),
                                    "_window_start": since, "record": rc}, ensure_ascii=False) + "\n")
        print(f"{wiki}: {len(rows)} changes since {since}")
        state[wiki] = run_end.isoformat().replace("+00:00", "Z")
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["full", "incremental"])
    ap.add_argument("--snapshot", help="dump month YYYY-MM (full mode)")
    ap.add_argument("--sample-lines", type=int, default=0)
    ap.add_argument("--max-rows", type=int, default=0, help="cap rows per wiki (samples/testing)")
    ap.add_argument("--out", default="sample_data/raw")
    ap.add_argument("--start", type=api_ts,
                    help="incremental: pull from this date/time instead of the saved watermark (backfill). "
                         "the api only keeps ~30 days of recent changes, older needs the dump")
    ap.add_argument("--state", default="watermarks.json", help="watermark file, can live in the volume")
    a = ap.parse_args()
    if a.mode == "full":
        if not a.snapshot:
            ap.error("--snapshot is required for a full load")
        full_load(a.snapshot, a.out, a.sample_lines)
    else:
        incremental(a.out, a.max_rows, a.state, a.start)
