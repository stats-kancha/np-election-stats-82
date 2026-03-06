"""Scraper for election.onlinekhabar.com.

Data extraction strategy:
- WordPress REST API at /wp-json/okelapi/v1/2082/home/election-results
- Returns party-level aggregates: seats won, leading, proportional votes
- Single API call, no HTML scraping needed
"""

from scrapers.base import (
    fetch_json,
    get_logger,
    now_npt,
    save_snapshot,
)

LOG = get_logger("onlinekhabar")
BASE_URL = "https://election.onlinekhabar.com"
API_URL = f"{BASE_URL}/wp-json/okelapi/v1/2082/home/election-results"
SOURCE = "onlinekhabar"


def scrape() -> dict:
    """Fetch party-level aggregate results from onlinekhabar API."""
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

    snapshot = {
        "source": SOURCE,
        "scraped_at": now_npt().isoformat(),
        "total_parties": len(parties),
        "parties": parties,
    }
    return snapshot


def run():
    """Scrape and save snapshot."""
    snapshot = scrape()
    save_snapshot(SOURCE, snapshot)
    LOG.info("Done: %d parties", snapshot["total_parties"])
    return snapshot


if __name__ == "__main__":
    run()
