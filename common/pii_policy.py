"""
PII policy for the Pakistan Language Wikipedias lakehouse.

Same rules as the Bronze -> Silver step (proposal, section 3), applied here to the raw
sample files so they can be published in the public GitHub repo.

  - IP addresses (logged-out editors before temporary accounts)  -> "ip_<hash>"
  - Temporary account names (~YYYY-NNNNN-NN)                     -> "tmp_<hash>"
  - Registered usernames                                          -> "user_<hash>"
  - Bot accounts (public service accounts)                        -> kept as-is
  - User / User talk page titles (they contain usernames or IPs) -> hashed
  - Edit summaries: IPs and e-mail addresses masked
Hashes are salted SHA-256; the salt comes from the PII_SALT environment variable and is
never committed (in Databricks it lives in a secret scope).

Usage:  PII_SALT=... python pii_policy.py <raw_root> <masked_root>
"""
import hashlib, json, os, re, sys, pathlib

SALT = os.environ.get("PII_SALT")
IP_RE = re.compile(r"^(\d{1,3}(\.\d{1,3}){3}|[0-9A-Fa-f]{0,4}(:[0-9A-Fa-f]{0,4}){2,7})$")
IP_IN_TEXT = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b|\b(?:[0-9A-Fa-f]{1,4}:){3,7}[0-9A-Fa-f]{1,4}\b")
EMAIL_IN_TEXT = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
TEMP_RE = re.compile(r"^~\d{4}-\d+-\d+$")
USER_NAMESPACES = {"2", "3"}  # User, User talk


def h(value):
    return hashlib.sha256((SALT + value).encode("utf-8")).hexdigest()[:16]


def mask_user(name, is_bot=False):
    if not name:
        return name
    if IP_RE.match(name):
        return "ip_" + h(name)
    if TEMP_RE.match(name):
        return "tmp_" + h(name)
    if is_bot or name.lower().endswith("bot"):
        return name
    return "user_" + h(name)


def mask_text(text):
    if not text:
        return text
    return EMAIL_IN_TEXT.sub("[email]", IP_IN_TEXT.sub("[ip]", text))


def mask_title(title):
    """'User:Someone/sandbox' -> 'User:user_ab12...' (prefix kept, it is the localized namespace name)."""
    if not title:
        return title
    prefix, sep, rest = title.partition(":")
    name = (rest if sep else title).split("/")[0]
    return (prefix + sep if sep else "") + mask_user(name)


# ---- MediaWiki history dump (TSV, no header) -------------------------------------------
# Column positions found by profiling the 2026-08 snapshot (78 columns).
C_COMMENT, C_USER_HIST, C_USER = 5, 8, 9
C_BOT_BY_HIST, C_BOT_BY = 13, 14
C_TITLE_HIST, C_TITLE, C_NS_HIST, C_NS = 29, 30, 31, 33
C_TARGET_USER_HIST, C_TARGET_USER = 43, 44


def mask_tsv_row(cols):
    bot = bool(cols[C_BOT_BY_HIST] or cols[C_BOT_BY])
    cols[C_COMMENT] = mask_text(cols[C_COMMENT])
    for c in (C_USER_HIST, C_USER):
        cols[c] = mask_user(cols[c], bot)
    for c in (C_TARGET_USER_HIST, C_TARGET_USER):
        cols[c] = mask_user(cols[c])
    if cols[C_NS_HIST] in USER_NAMESPACES:
        cols[C_TITLE_HIST] = mask_user(cols[C_TITLE_HIST])  # dump titles have no namespace prefix
    if cols[C_NS] in USER_NAMESPACES:
        cols[C_TITLE] = mask_user(cols[C_TITLE])
    return cols


# ---- Recent changes API (JSONL with envelope) ------------------------------------------
def mask_rc(rec):
    rec["user"] = mask_user(rec.get("user", ""), rec.get("bot", False))
    rec["comment"] = mask_text(rec.get("comment", ""))
    if str(rec.get("ns")) in USER_NAMESPACES:
        rec["title"] = mask_title(rec["title"])
    lp = rec.get("logparams") or {}
    for k in ("olduser", "newuser"):
        if k in lp:
            lp[k] = mask_user(lp[k])
    if str(lp.get("target_ns")) in USER_NAMESPACES:
        lp["target_title"] = mask_title(lp["target_title"])
    return rec


def apply_pii_policy(src, dst):
    """Mask every raw sample file under src and write the result to dst (same layout)."""
    stats = {"rows": 0}
    for f in pathlib.Path(src).rglob("*"):
        if f.is_dir():
            continue
        out = pathlib.Path(dst, f.relative_to(src))
        out.parent.mkdir(parents=True, exist_ok=True)
        if f.suffix == ".tsv":
            with open(f, encoding="utf-8") as i, open(out, "w", encoding="utf-8") as o:
                for line in i:
                    cols = mask_tsv_row(line.rstrip("\n").split("\t"))
                    o.write("\t".join(cols) + "\n")
                    stats["rows"] += 1
        elif f.suffix == ".jsonl":
            with open(f, encoding="utf-8") as i, open(out, "w", encoding="utf-8") as o:
                for line in i:
                    env = json.loads(line)
                    env["record"] = mask_rc(env["record"])
                    o.write(json.dumps(env, ensure_ascii=False) + "\n")
                    stats["rows"] += 1
        else:
            out.write_bytes(f.read_bytes())
    return stats


if __name__ == "__main__":
    if not SALT:
        sys.exit("Set PII_SALT first (a long random string, never committed).")
    print(apply_pii_policy(sys.argv[1], sys.argv[2]))
