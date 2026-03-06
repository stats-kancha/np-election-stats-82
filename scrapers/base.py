"""Shared utilities for election scrapers."""

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
MERGED_DIR = DATA_DIR / "merged"

NPT = timezone(timedelta(hours=5, minutes=45))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def now_npt() -> datetime:
    return datetime.now(NPT)


def timestamp_str() -> str:
    return now_npt().strftime("%Y-%m-%dT%H-%M-%S")


def fetch_url(url: str, params: dict = None, timeout: int = 30) -> str:
    """Fetch a URL with retries and return the response text."""
    logger = get_logger("fetch")
    headers = {
        "User-Agent": "ElectionStats/1.0 (github.com/thapakazi/election-stats)",
    }
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url} after 3 attempts")


def fetch_json(url: str, params: dict = None, timeout: int = 30) -> dict:
    """Fetch a URL and parse as JSON."""
    text = fetch_url(url, params=params, timeout=timeout)
    return json.loads(text)


def save_snapshot(source: str, data: dict) -> Path:
    """Save a timestamped snapshot for a source."""
    ts = timestamp_str()
    out_dir = SNAPSHOTS_DIR / source
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ts}.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    get_logger("snapshot").info("Saved %s", out_path)
    return out_path


def save_merged(data: dict) -> Path:
    """Save the merged latest.json."""
    MERGED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = MERGED_DIR / "latest.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    get_logger("merged").info("Saved %s", out_path)
    return out_path


def make_constituency_record(
    source: str,
    scraped_at: str,
    province_id: int,
    province_name: str,
    district: str,
    constituency_slug: str,
    candidates: list[dict],
) -> dict:
    """Build a normalized constituency record."""
    return {
        "source": source,
        "scraped_at": scraped_at,
        "province": {"id": province_id, "name": province_name},
        "district": district,
        "constituency": constituency_slug,
        "candidates": candidates,
    }
