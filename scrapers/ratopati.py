"""Scraper for election.ratopati.com.

Data extraction strategy:
- Use /api/address/district?province_id={1-7} to build constituency index
- Scrape each constituency page HTML for candidate vote data
- Parse party-container divs for candidate name, party, votes, winner status
- Nepali numerals are converted to integers
"""

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.base import (
    DATA_DIR,
    fetch_json,
    fetch_url,
    get_logger,
    make_constituency_record,
    now_npt,
    save_snapshot,
)

LOG = get_logger("ratopati")
BASE_URL = "https://election.ratopati.com"
SOURCE = "ratopati"
MAX_WORKERS = 10

PROVINCE_NAMES = {
    1: "Koshi",
    2: "Madhesh",
    3: "Bagmati",
    4: "Gandaki",
    5: "Lumbini",
    6: "Karnali",
    7: "Sudurpaschim",
}

# Nepali digit mapping
NEPALI_DIGITS = str.maketrans("०१२३४५६७८९", "0123456789")


def _nepali_to_int(text: str) -> int:
    """Convert Nepali numeral string to integer."""
    converted = text.translate(NEPALI_DIGITS).replace(",", "").strip()
    return int(converted) if converted.isdigit() else 0


def build_constituency_index() -> list[dict]:
    """Fetch the full constituency index from the ratopati district API."""
    index_path = DATA_DIR / "index" / "constituencies.json"

    constituencies = []
    for pid in range(1, 8):
        data = fetch_json(
            f"{BASE_URL}/api/address/district", params={"province_id": pid}
        )
        for district in data.get("data", []):
            for c in district.get("f_constituencies", []):
                constituencies.append(
                    {
                        "province_id": pid,
                        "province_name": PROVINCE_NAMES[pid],
                        "district": district["name"],
                        "district_slug": district["slug"],
                        "constituency_alias": c["alias"],
                        "constituency_num": c.get("f_const", 0),
                    }
                )

    # Cache the index
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps(constituencies, ensure_ascii=False, indent=2))
    LOG.info("Built constituency index: %d entries", len(constituencies))
    return constituencies


def _scrape_constituency_page(constituency: dict) -> dict | None:
    """Scrape a single constituency page and return normalized record."""
    alias = constituency["constituency_alias"]
    url = f"{BASE_URL}/constituency/{alias}"

    try:
        html = fetch_url(url, timeout=15)
    except RuntimeError:
        LOG.warning("Failed to fetch %s", url)
        return None

    soup = BeautifulSoup(html, "html.parser")
    containers = soup.find_all("div", class_="party-container")

    candidates = []
    for container in containers:
        is_winner = "candidate-win" in (container.get("class") or [])

        # Candidate name from image alt text
        logo_link = container.find("a", class_="party-logo")
        img = logo_link.find("img") if logo_link else None
        name = img["alt"].strip() if img and img.get("alt") else ""

        # Party name
        party_info = container.find("div", class_="party-info")
        party_name_el = party_info.find("span", class_="party-name") if party_info else None
        party = party_name_el.get_text(strip=True) if party_name_el else ""

        # Votes
        votes_el = container.find("span", class_="votes")
        votes_text = votes_el.get_text(strip=True) if votes_el else "0"
        votes = _nepali_to_int(votes_text)

        # Party logo
        party_sign = container.find("span", class_="party-sign")
        party_flag = ""
        if party_sign:
            sign_img = party_sign.find("img")
            if sign_img and sign_img.get("src"):
                party_flag = sign_img["src"]

        if name:
            candidates.append(
                {
                    "name": name,
                    "party": party,
                    "votes": votes,
                    "is_winner": is_winner,
                    "is_leading": is_winner and not any(
                        c["is_winner"] for c in candidates
                    ),
                    "margin": 0,
                    "image": img["src"] if img and img.get("src") else "",
                    "party_flag": party_flag,
                }
            )

    # Sort by votes descending
    candidates.sort(key=lambda c: c["votes"], reverse=True)

    # Calculate margin for leading candidate
    if len(candidates) >= 2:
        candidates[0]["margin"] = candidates[0]["votes"] - candidates[1]["votes"]
        candidates[0]["is_leading"] = True

    return make_constituency_record(
        source=SOURCE,
        scraped_at=now_npt().isoformat(),
        province_id=constituency["province_id"],
        province_name=constituency["province_name"],
        district=constituency["district"],
        constituency_slug=alias,
        candidates=candidates,
    )


def scrape() -> dict:
    """Scrape all constituencies from ratopati."""
    LOG.info("Building constituency index...")
    index = build_constituency_index()
    LOG.info("Scraping %d constituency pages (max %d workers)...", len(index), MAX_WORKERS)

    constituencies = []
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_scrape_constituency_page, c): c
            for c in index
        }
        for future in as_completed(futures):
            result = future.result()
            if result:
                constituencies.append(result)
            else:
                failed += 1

    LOG.info("Scraped %d constituencies (%d failed)", len(constituencies), failed)

    snapshot = {
        "source": SOURCE,
        "scraped_at": now_npt().isoformat(),
        "total_constituencies_scraped": len(constituencies),
        "total_failed": failed,
        "constituencies": constituencies,
    }
    return snapshot


def run():
    """Scrape and save snapshot."""
    snapshot = scrape()
    save_snapshot(SOURCE, snapshot)
    LOG.info(
        "Done: %d constituencies scraped", snapshot["total_constituencies_scraped"]
    )
    return snapshot


if __name__ == "__main__":
    run()
