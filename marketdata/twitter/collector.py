"""Twitter/X feed collector using twikit authenticated Client.

The GuestClient approach no longer works (Twitter removed the guest
activation endpoint).  This module uses the authenticated Client with
cookie-based session persistence so login only happens once.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from marketdata.config import PipelineConfig
from marketdata.pipeline.storage import TWEET_SCHEMA, tweets_partition_path
from marketdata.utils.log import get_logger

log = get_logger(__name__)

# Back-off when rate-limited (seconds)
_RATE_LIMIT_WAIT = 60
_MAX_RETRIES = 3


class TweetCollector:
    """Fetches tweets from configured accounts and writes to Parquet."""

    def __init__(self, cfg: PipelineConfig) -> None:
        self._cfg = cfg
        self._twitter_cfg = cfg.twitter
        self._client = None  # lazy init
        self._hooks: list[Callable] = []
        self._running = False
        # Cache user_id lookups to avoid repeated API calls
        self._user_id_cache: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Authenticate with Twitter/X via twikit Client.

        Priority:
        1. Saved cookies file (fastest — no network call).
        2. Browser cookie import (auth_token + ct0 from config).
        3. Full login with username/password (may hit Cloudflare).
        """
        from twikit import Client

        self._client = Client(language="en-US")
        cookies_path = self._cookies_path
        tcfg = self._twitter_cfg

        # 1. Try saved cookies file
        if cookies_path.exists():
            try:
                self._client.load_cookies(str(cookies_path))
                log.info("Loaded Twitter session from %s", cookies_path)
                self._running = True
                return
            except Exception:
                log.warning("Saved cookies invalid — trying other auth methods")

        # 2. Try browser cookie import (auth_token + ct0)
        if tcfg.auth_token and tcfg.ct0:
            self._set_browser_cookies(tcfg.auth_token, tcfg.ct0)
            self._save_cookies()
            log.info("Imported browser cookies (auth_token + ct0)")
            self._running = True
            return

        # 3. Fall back to full login
        await self._login()
        self._running = True

    def _set_browser_cookies(self, auth_token: str, ct0: str) -> None:
        """Set session cookies directly from browser-exported values.

        The user copies auth_token and ct0 from:
        DevTools → Application → Cookies → https://x.com
        """
        self._client.set_cookies(
            {
                "auth_token": auth_token,
                "ct0": ct0,
            }
        )

    async def _login(self) -> None:
        """Log in with username/password and persist cookies."""
        tcfg = self._twitter_cfg
        if not tcfg.username or not tcfg.password:
            raise RuntimeError(
                "Twitter credentials not configured.\n\n"
                "Option 1 (recommended): Log in to x.com in your browser, then\n"
                "  DevTools → Application → Cookies → https://x.com\n"
                "  Copy 'auth_token' and 'ct0' values into default.yaml:\n"
                "    twitter:\n"
                "      auth_token: \"...\"\n"
                "      ct0: \"...\"\n\n"
                "Option 2: Set MD_TWITTER__USERNAME and MD_TWITTER__PASSWORD\n"
                "  environment variables (may be blocked by Cloudflare)."
            )

        log.info("Logging in to Twitter/X as @%s …", tcfg.username)
        await self._client.login(
            auth_info_1=tcfg.username,
            auth_info_2=tcfg.email or None,
            password=tcfg.password,
        )
        self._save_cookies()
        log.info("Login successful — cookies saved")

    def _save_cookies(self) -> None:
        """Persist current session cookies to disk."""
        cookies_path = self._cookies_path
        cookies_path.parent.mkdir(parents=True, exist_ok=True)
        self._client.save_cookies(str(cookies_path))
        log.debug("Cookies saved to %s", cookies_path)

    @property
    def _cookies_path(self) -> Path:
        """Resolve cookies file path relative to project root."""
        raw = self._twitter_cfg.cookies_file
        p = Path(raw)
        if not p.is_absolute():
            p = Path(self._cfg.data_dir).resolve().parent / raw
            # If data_dir is relative, resolve from project root
            from marketdata.config import _PROJECT_ROOT
            p = _PROJECT_ROOT / raw
        return p

    async def stop(self) -> None:
        """Stop the collector."""
        self._running = False
        log.info("TweetCollector stopped")

    def add_hook(self, callback: Callable) -> None:
        """Register a notification hook called for each new tweet."""
        self._hooks.append(callback)

    # ------------------------------------------------------------------
    # Collection loop
    # ------------------------------------------------------------------

    async def collect_all(self) -> int:
        """Fetch tweets from all accounts and flush to Parquet.

        Returns the total number of new tweets written.
        """
        total_new = 0
        for account in self._twitter_cfg.accounts:
            try:
                n = await self._collect_account(account)
                total_new += n
            except Exception:
                log.exception("Failed to collect tweets for @%s", account)
                await asyncio.sleep(2)
        if total_new > 0:
            log.info("Collected %d new tweets across all accounts", total_new)
        return total_new

    async def run_forever(self) -> None:
        """Main loop: collect tweets, sleep, repeat."""
        while self._running:
            try:
                await self.collect_all()
            except Exception:
                log.exception("Error in tweet collection cycle")

            interval = self._twitter_cfg.fetch_interval_min * 60
            log.debug("Sleeping %d seconds until next collection", interval)
            for _ in range(interval):
                if not self._running:
                    return
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # Per-account collection
    # ------------------------------------------------------------------

    async def _collect_account(self, account: str) -> int:
        """Fetch tweets for a single account, deduplicate, and write new ones."""
        user_id = await self._resolve_user_id(account)
        if not user_id:
            return 0

        tweets = await self._fetch_tweets_with_retry(user_id)
        if not tweets:
            log.debug("No tweets returned for @%s", account)
            return 0

        records = []
        for tweet in tweets:
            try:
                records.append(self._to_record(tweet, account))
            except Exception:
                log.debug("Failed to parse tweet from @%s", account, exc_info=True)

        if not records:
            return 0

        new_df = pd.DataFrame(records)
        new_df["created_at"] = pd.to_datetime(new_df["created_at"], utc=True)

        # Group by date and flush
        new_df["_date"] = new_df["created_at"].dt.date
        total_new = 0
        for date_val, group in new_df.groupby("_date"):
            date_str = date_val.isoformat()
            path = tweets_partition_path(
                self._cfg.parquet_root, account.lower(), date_str,
            )

            write_df = group.drop(columns=["_date"])
            existing_ids: set[str] = set()

            if path.exists():
                try:
                    existing = pd.read_parquet(str(path))
                    existing_ids = set(existing["tweet_id"].tolist())
                    # Only keep truly new tweets
                    write_df = write_df[~write_df["tweet_id"].isin(existing_ids)]
                    if write_df.empty:
                        continue
                    combined = pd.concat([existing, write_df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=["tweet_id"], keep="last")
                    combined = combined.sort_values("created_at").reset_index(drop=True)
                    write_df = combined
                except Exception:
                    log.warning("Corrupted tweet parquet at %s — overwriting", path)
                    write_df = write_df.sort_values("created_at").reset_index(drop=True)
            else:
                write_df = write_df.sort_values("created_at").reset_index(drop=True)

            path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pandas(write_df, schema=TWEET_SCHEMA, preserve_index=False)
            pq.write_table(table, str(path), compression="zstd")

            n_new = len(write_df) - len(existing_ids & set(write_df["tweet_id"].tolist()))
            total_new += max(n_new, 0)
            log.debug(
                "Flushed %d tweets for @%s %s (%d new)",
                len(write_df), account, date_str, n_new,
            )

        # Fire hooks for new tweets
        if total_new > 0:
            new_tweets = new_df[~new_df["tweet_id"].isin(set())].to_dict("records")
            for hook in self._hooks:
                try:
                    for rec in new_tweets[:total_new]:
                        await hook(rec) if asyncio.iscoroutinefunction(hook) else hook(rec)
                except Exception:
                    log.debug("Hook failed", exc_info=True)

        return total_new

    # ------------------------------------------------------------------
    # Twitter API helpers
    # ------------------------------------------------------------------

    async def _resolve_user_id(self, account: str) -> str | None:
        """Resolve a screen name to a user ID, with caching."""
        if account in self._user_id_cache:
            return self._user_id_cache[account]

        try:
            user = await self._client.get_user_by_screen_name(account)
            self._user_id_cache[account] = user.id
            log.debug("Resolved @%s -> id=%s", account, user.id)
            return user.id
        except Exception as exc:
            log.warning("Could not resolve @%s: %s", account, exc)
            return None

    async def _fetch_tweets_with_retry(self, user_id: str) -> list:
        """Fetch user tweets with rate-limit retry and cookie refresh."""
        for attempt in range(_MAX_RETRIES):
            try:
                tweets = await self._client.get_user_tweets(
                    user_id, tweet_type="Tweets",
                    count=self._twitter_cfg.tweets_per_account,
                )
                return tweets or []
            except Exception as exc:
                msg = str(exc).lower()
                if "rate" in msg or "429" in msg or "limit" in msg:
                    wait = _RATE_LIMIT_WAIT * (attempt + 1)
                    log.warning(
                        "Rate limited fetching user %s, waiting %ds (attempt %d/%d)",
                        user_id, wait, attempt + 1, _MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    continue
                if "401" in msg or "unauthorized" in msg or "403" in msg:
                    log.warning("Auth error — attempting re-login")
                    try:
                        await self._login()
                    except Exception:
                        log.exception("Re-login failed")
                    continue
                raise
        log.warning("Exhausted retries for user %s", user_id)
        return []

    # ------------------------------------------------------------------
    # Record mapping
    # ------------------------------------------------------------------

    @staticmethod
    def _to_record(tweet: object, account: str) -> dict:
        """Convert a twikit Tweet object to a flat dict matching TWEET_SCHEMA."""
        # Extract media URLs
        media_list = []
        raw_media = getattr(tweet, "media", None)
        if raw_media:
            for m in raw_media:
                url = getattr(m, "media_url_https", None) or getattr(m, "url", None)
                if url:
                    media_list.append(url)

        # Parse created_at — twikit returns e.g. "Mon Feb 19 12:34:56 +0000 2026"
        created_at_str = getattr(tweet, "created_at", "")
        try:
            ts = pd.Timestamp(created_at_str)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
        except Exception:
            ts = pd.Timestamp.now(tz="UTC")

        user = getattr(tweet, "user", None)
        display_name = getattr(user, "name", account) if user else account

        return {
            "tweet_id": str(getattr(tweet, "id", "")),
            "created_at": ts,
            "account": account.lower(),
            "display_name": display_name,
            "text": getattr(tweet, "full_text", "") or getattr(tweet, "text", ""),
            "lang": getattr(tweet, "lang", ""),
            "retweet_count": int(getattr(tweet, "retweet_count", 0) or 0),
            "like_count": int(getattr(tweet, "favorite_count", 0) or 0),
            "reply_count": int(getattr(tweet, "reply_count", 0) or 0),
            "view_count": int(getattr(tweet, "view_count", 0) or 0),
            "media_urls": json.dumps(media_list),
            "source": "twikit",
        }
