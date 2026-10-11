"""Table names, landing paths and parameter parsing. No Spark in here."""
import datetime as dt
import os
import uuid
from dataclasses import dataclass

WIKIS = ["urwiki", "pnbwiki", "sdwiki", "pswiki"]
PKT = "Asia/Karachi"


@dataclass
class Config:
    catalog: str = "workspace"   # Unity Catalog name on Databricks, "" when running locally
    prefix: str = "pkwiki"       # schema prefix, lets us run a throwaway copy (e.g. pkwiki_test)
    landing_root: str = ""       # where the raw files land, defaults to the UC volume

    def __post_init__(self):
        if not self.landing_root:
            self.landing_root = f"/Volumes/{self.catalog}/{self.prefix}_raw/landing"

    def schema(self, layer):
        s = f"{self.prefix}_{layer}"
        return f"{self.catalog}.{s}" if self.catalog else s

    def table(self, layer, name):
        return f"{self.schema(layer)}.{name}"

    # landing layout is the same one ingestion/ingest_wikimedia.py writes
    def history_path(self, snapshot, wiki):
        return f"{self.landing_root}/mediawiki_history/load_type=full/snapshot={snapshot}/wiki={wiki}"

    def rc_path(self, ingest_date, wiki):
        return f"{self.landing_root}/recentchanges/load_type=incremental/ingest_date={ingest_date}/wiki={wiki}"

    def landed_ingest_dates(self):
        root = f"{self.landing_root}/recentchanges/load_type=incremental"
        if not os.path.isdir(root):
            return []
        return sorted(d.split("=", 1)[1] for d in os.listdir(root) if d.startswith("ingest_date="))


def parse_wikis(value):
    if not value or value.strip().lower() == "all":
        return list(WIKIS)
    wikis = [w.strip() for w in value.split(",") if w.strip()]
    unknown = set(wikis) - set(WIKIS)
    if unknown:
        raise ValueError(f"unknown wiki(s): {sorted(unknown)}, expected some of {WIKIS}")
    return wikis


def parse_snapshot(value):
    try:
        dt.datetime.strptime(value, "%Y-%m")
    except (TypeError, ValueError):
        raise ValueError(f"snapshot must look like YYYY-MM, got {value!r}")
    return value


def snapshot_cutoff(snapshot):
    """The dump for 2026-08 covers everything before 2026-09-01 00:00 UTC."""
    y, m = map(int, snapshot.split("-"))
    y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{y:04d}-{m:02d}-01 00:00:00"


def date_range(start, end):
    """Inclusive list of YYYY-MM-DD strings. end defaults to start."""
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end or start)
    if d1 < d0:
        raise ValueError(f"end_date {end} is before start_date {start}")
    if (d1 - d0).days > 366:
        raise ValueError("backfill window longer than a year, split it up")
    return [(d0 + dt.timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def new_run_id(value=""):
    # jobs pass {{job.run_id}}, interactive runs get a random one
    return value.strip() or f"manual-{dt.datetime.now(dt.timezone.utc):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"
