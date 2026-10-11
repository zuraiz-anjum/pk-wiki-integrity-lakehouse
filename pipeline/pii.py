"""
Spark version of common/pii_policy.py. Same regexes (imported from there) and the same hash, so
an editor gets the same editor_key whether the row came through the sample masking script or
through the Bronze -> Silver step.

    hash = "user_" + sha256(salt + name)[:16]     (ip_ / tmp_ for IPs and temporary accounts)
"""
import os

from pyspark.sql import functions as F

from common.pii_policy import (ALREADY_MASKED, BOT_NAME_RE, EMAIL_IN_TEXT, IP_IN_TEXT, IP_RE,
                               SPECIAL_SUBPAGE_IN_TEXT, SYSTEM_ACCOUNTS, TEMP_RE,
                               USER_LINK_IN_TEXT)

# python -> java regex. the patterns are plain enough that only the flags need moving inline
IP_RX = IP_RE.pattern
TEMP_RX = TEMP_RE.pattern
BOT_RX = "(?i)" + BOT_NAME_RE.pattern
MASKED_RX = ALREADY_MASKED.pattern
IP_TEXT_RX = IP_IN_TEXT.pattern
EMAIL_TEXT_RX = EMAIL_IN_TEXT.pattern
USER_LINK_RX = "(?i)" + USER_LINK_IN_TEXT.pattern
SPECIAL_RX = "(?i)" + SPECIAL_SUBPAGE_IN_TEXT.pattern


def get_salt(dbutils=None, scope="pkwiki", key="pii_salt"):
    """Databricks secret scope first, then the PII_SALT env var (local runs)."""
    if dbutils is not None:
        try:
            return dbutils.secrets.get(scope, key)
        except Exception as e:  # scope not created yet
            print(f"secret {scope}/{key} not readable ({type(e).__name__}), trying PII_SALT env var")
    salt = os.environ.get("PII_SALT")
    if not salt:
        raise RuntimeError("No PII salt. Create the secret (see README) or set PII_SALT.")
    return salt


def _h(salt, name):
    return F.substring(F.sha2(F.concat(F.lit(salt), name), 256), 1, 16)


def editor_type(name, is_anon, is_temp, is_bot):
    """registered | temporary | ip | bot. Flags first, the name patterns are a fallback
    because the api only sends anon/temp when they are true."""
    return (F.when(is_anon | name.rlike(IP_RX) | name.startswith("ip_"), "ip")
            .when(is_temp | name.rlike(TEMP_RX) | name.startswith("tmp_"), "temporary")
            .when(is_bot | name.rlike(BOT_RX) | name.isin(*SYSTEM_ACCOUNTS), "bot")
            .when(name.isNotNull(), "registered"))


def mask_name(name, etype, salt):
    """Hash the name unless it is a bot account or system account (public by design)."""
    return (F.when(name.isNull() | (F.trim(name) == ""), F.lit(None))
            .when(name.rlike(MASKED_RX), name)                      # already masked (samples)
            .when(name.rlike(BOT_RX) | name.isin(*SYSTEM_ACCOUNTS), name)
            .when(etype == "ip", F.concat(F.lit("ip_"), _h(salt, name)))
            .when(etype == "temporary", F.concat(F.lit("tmp_"), _h(salt, name)))
            .otherwise(F.concat(F.lit("user_"), _h(salt, name))))


def mask_text(text):
    t = F.regexp_replace(text, IP_TEXT_RX, "[ip]")
    t = F.regexp_replace(t, EMAIL_TEXT_RX, "[email]")
    t = F.regexp_replace(t, USER_LINK_RX, "$1[user]")
    return F.regexp_replace(t, SPECIAL_RX, "$1[user]")


def mask_user_page_title(title, ns, salt):
    """User:/User talk: pages are named after the user. Keep only the masked user part."""
    base = F.regexp_extract(title, r"^([^/]+)", 1)
    etype = editor_type(base, F.lit(False), F.lit(False), F.lit(False))
    return F.when(ns.isin(2, 3), mask_name(base, etype, salt)).otherwise(title)
