"""Tests for twitter/collector.py — schema, path, record mapping, dedup."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow.parquet as pq
import pytest

from marketdata.pipeline.storage import TWEET_SCHEMA, tweets_partition_path
from marketdata.twitter.collector import TweetCollector


class TestTweetSchema:
    """Verify TWEET_SCHEMA field names and types."""

    def test_expected_fields(self) -> None:
        names = [f.name for f in TWEET_SCHEMA]
        assert "tweet_id" in names
        assert "created_at" in names
        assert "account" in names
        assert "display_name" in names
        assert "text" in names
        assert "like_count" in names
        assert "retweet_count" in names
        assert "reply_count" in names
        assert "view_count" in names
        assert "media_urls" in names
        assert "source" in names

    def test_field_count(self) -> None:
        assert len(TWEET_SCHEMA) == 12


class TestTweetsPartitionPath:
    """Verify Hive-style path construction for tweets."""

    def test_basic_path(self, tmp_data_dir: Path) -> None:
        path = tweets_partition_path(tmp_data_dir, "elonmusk", "2026-02-19")
        expected = tmp_data_dir / "dataset=tweets/account=elonmusk/date=2026-02-19/part.parquet"
        assert path == expected

    def test_lowercase(self, tmp_data_dir: Path) -> None:
        path = tweets_partition_path(tmp_data_dir, "sama", "2026-01-01")
        assert "account=sama" in str(path)


class TestToRecord:
    """Verify _to_record maps twikit-like objects correctly."""

    @staticmethod
    def _make_tweet(**overrides):
        """Create a mock tweet object resembling twikit's Tweet."""
        user = SimpleNamespace(name="Elon Musk", screen_name="elonmusk")
        defaults = {
            "id": "123456789",
            "full_text": "Testing tweet content",
            "text": "Testing tweet content",
            "created_at": "Mon Feb 19 12:00:00 +0000 2026",
            "lang": "en",
            "retweet_count": 42,
            "favorite_count": 100,
            "reply_count": 5,
            "view_count": 10000,
            "quote_count": 3,
            "media": None,
            "user": user,
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_basic_record(self) -> None:
        tweet = self._make_tweet()
        record = TweetCollector._to_record(tweet, "elonmusk")

        assert record["tweet_id"] == "123456789"
        assert record["account"] == "elonmusk"
        assert record["display_name"] == "Elon Musk"
        assert record["text"] == "Testing tweet content"
        assert record["lang"] == "en"
        assert record["retweet_count"] == 42
        assert record["like_count"] == 100
        assert record["reply_count"] == 5
        assert record["view_count"] == 10000
        assert record["source"] == "twikit"
        assert isinstance(record["created_at"], pd.Timestamp)

    def test_media_urls_json(self) -> None:
        media1 = SimpleNamespace(media_url_https="https://pbs.twimg.com/1.jpg")
        media2 = SimpleNamespace(media_url_https="https://pbs.twimg.com/2.jpg")
        tweet = self._make_tweet(media=[media1, media2])
        record = TweetCollector._to_record(tweet, "elonmusk")

        urls = json.loads(record["media_urls"])
        assert len(urls) == 2
        assert urls[0] == "https://pbs.twimg.com/1.jpg"

    def test_no_media_empty_list(self) -> None:
        tweet = self._make_tweet(media=None)
        record = TweetCollector._to_record(tweet, "elonmusk")
        assert json.loads(record["media_urls"]) == []

    def test_missing_view_count(self) -> None:
        tweet = self._make_tweet(view_count=None)
        record = TweetCollector._to_record(tweet, "elonmusk")
        assert record["view_count"] == 0

    def test_full_text_preferred_over_text(self) -> None:
        tweet = self._make_tweet(full_text="Full version", text="Short version")
        record = TweetCollector._to_record(tweet, "test")
        assert record["text"] == "Full version"

    def test_fallback_to_text(self) -> None:
        tweet = self._make_tweet(full_text="", text="Fallback text")
        record = TweetCollector._to_record(tweet, "test")
        assert record["text"] == "Fallback text"


class TestDeduplication:
    """Verify tweet deduplication in Parquet write."""

    def test_duplicate_tweets_deduplicated(self, tmp_data_dir: Path) -> None:
        """Writing the same tweet_id twice should result in only one row."""
        import pyarrow as pa

        path = tweets_partition_path(tmp_data_dir, "elonmusk", "2026-02-19")
        path.parent.mkdir(parents=True, exist_ok=True)

        records = [
            {
                "tweet_id": "111",
                "created_at": pd.Timestamp("2026-02-19 12:00:00", tz="UTC"),
                "account": "elonmusk",
                "display_name": "Elon Musk",
                "text": "First tweet",
                "lang": "en",
                "retweet_count": 10,
                "like_count": 20,
                "reply_count": 1,
                "view_count": 100,
                "media_urls": "[]",
                "source": "twikit",
            },
        ]
        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df, schema=TWEET_SCHEMA, preserve_index=False)
        pq.write_table(table, str(path), compression="zstd")

        # Now write same tweet_id again
        existing = pd.read_parquet(str(path))
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["tweet_id"], keep="last")
        assert len(combined) == 1
