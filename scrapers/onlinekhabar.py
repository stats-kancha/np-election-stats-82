"""Scraper for election.onlinekhabar.com.

Data extraction strategy:
- WordPress REST API at /wp-json/okelapi/v1/2082/home/election-results
  for party-level aggregates (seats won, leading, proportional votes)
- Homepage HTML for 24 hot-seat constituency cards with candidate votes
"""

import re

from bs4 import BeautifulSoup

from scrapers.base import (
    fetch_json,
    fetch_url,
    get_logger,
    make_constituency_record,
    now_npt,
    save_snapshot,
)

LOG = get_logger("onlinekhabar")
BASE_URL = "https://election.onlinekhabar.com"
API_URL = f"{BASE_URL}/wp-json/okelapi/v1/2082/home/election-results"
SOURCE = "onlinekhabar"

NEPALI_DIGITS = str.maketrans("०१२३४५६७८९", "0123456789")

# Onlinekhabar uses different spellings for some districts
SLUG_FIXES = {
    "dhanusa": "dhanusha",
    "kabrepalanchok": "kavrepalanchok",
    "kapilbstu": "kapilvastu",
    "nawalpur": "nawalparasi-east",
    "rukum": "rukum-east",
    "rupendehi": "rupandehi",
    "sindhupalchowk": "sindhupalchok",
    "tanahu": "tanahun",
    "terathum": "terhathum",
}


def _nepali_to_int(text: str) -> int:
    converted = text.translate(NEPALI_DIGITS).replace(",", "").strip()
    return int(converted) if converted.isdigit() else 0


def _slug_from_url(href: str) -> str:
    """Extract constituency slug from onlinekhabar URL.

    e.g. 'https://election.onlinekhabar.com/central-chetra/kathmandu5' -> 'kathmandu-5'
    """
    # Get the last path segment
    segment = href.rstrip("/").rsplit("/", 1)[-1]
    # Insert hyphen before trailing digits: 'kathmandu5' -> 'kathmandu-5'
    match = re.match(r"^([a-z-]+?)(\d+)$", segment)
    if match:
        district = match.group(1)
        district = SLUG_FIXES.get(district, district)
        return f"{district}-{match.group(2)}"
    return segment


def _scrape_hot_seats(html: str) -> list[dict]:
    """Parse hot-seat constituency cards from homepage HTML."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("div", class_="okel-candidate-card")
    scraped_at = now_npt().isoformat()

    constituencies = []
    for card in cards:
        header = card.find("div", class_="okel-candidate-card-header")
        link = header.find("a") if header else None
        if not link:
            continue

        href = link.get("href", "")
        slug = _slug_from_url(href)

        # Status
        status_el = card.find("div", class_="okel-result-status")
        status_text = status_el.get_text(strip=True) if status_el else ""
        is_declared = "declared" in status_text.lower()

        # Parse candidates
        rows = card.find_all("div", class_="okel-candidate-row")
        candidates = []
        for row in rows:
            name_el = row.find(class_="okel-candidate-name")
            vote_els = row.find_all(class_="vote")
            other_info = row.find(class_="okel-candidate-other-info")

            name = name_el.get_text(strip=True) if name_el else ""
            votes = _nepali_to_int(vote_els[0].get_text(strip=True)) if vote_els else 0
            party = other_info.get_text(strip=True) if other_info else ""

            if name:
                candidates.append(
                    {
                        "name": name,
                        "party": party,
                        "votes": votes,
                        "is_winner": False,
                        "is_leading": False,
                        "margin": 0,
                        "image": "",
                        "party_flag": "",
                    }
                )

        # Sort by votes and set leading/winner
        candidates.sort(key=lambda c: c["votes"], reverse=True)
        if candidates:
            if is_declared:
                candidates[0]["is_winner"] = True
            candidates[0]["is_leading"] = True
            if len(candidates) >= 2:
                candidates[0]["margin"] = candidates[0]["votes"] - candidates[1]["votes"]

        # We don't have province/district info from the card, leave empty
        record = make_constituency_record(
            source=SOURCE,
            scraped_at=scraped_at,
            province_id=0,
            province_name="",
            district="",
            constituency_slug=slug,
            candidates=candidates,
        )
        constituencies.append(record)

    return constituencies


def scrape() -> dict:
    """Fetch party aggregates from API + hot-seat constituencies from HTML."""
    # Party aggregates
    LOG.info("Fetching party results from API")
    data = fetch_json(API_URL, params={"limit": "200"})
    party_results = data.get("data", {}).get("party_results", [])
    LOG.info("Got %d party results", len(party_results))

    parties = []
    for p in party_results:
        parties.append(
            {
                "party_name": p.get("party_name", ""),
                "party_slug": p.get("party_slug", ""),
                "party_image": p.get("party_image", ""),
                "party_color": p.get("party_color", ""),
                "leading_count": p.get("leading_count", 0),
                "winner_count": p.get("winner_count", 0),
                "total_seat": p.get("total_seat", 0),
                "seat_bar_percentage": p.get("seat_bar_percentage", 0),
                "proportional_vote_percentage": p.get(
                    "proportional_vote_percentage", 0
                ),
            }
        )

    # Hot-seat constituencies from homepage
    LOG.info("Fetching homepage for hot-seat cards")
    html = fetch_url(BASE_URL)
    constituencies = _scrape_hot_seats(html)
    LOG.info("Got %d hot-seat constituencies", len(constituencies))

    snapshot = {
        "source": SOURCE,
        "scraped_at": now_npt().isoformat(),
        "total_parties": len(parties),
        "total_constituencies_scraped": len(constituencies),
        "parties": parties,
        "constituencies": constituencies,
    }
    return snapshot


def run():
    """Scrape and save snapshot."""
    snapshot = scrape()
    save_snapshot(SOURCE, snapshot)
    LOG.info(
        "Done: %d parties, %d constituencies",
        snapshot["total_parties"],
        snapshot["total_constituencies_scraped"],
    )
    return snapshot


if __name__ == "__main__":
    run()
