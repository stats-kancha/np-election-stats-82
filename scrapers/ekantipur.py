"""Scraper for election.ekantipur.com.

Data extraction strategy:
- The main page embeds all competitive constituency data in a JS object `competitiveDist`
- District metadata is in `dists['slug'] = {name, pid, pname}` assignments
- Constituency counts are in `regions['slug'] = N` assignments
- All data is inline in <script> tags, no API needed
"""

import json
import re

from scrapers.base import (
    fetch_url,
    get_logger,
    make_constituency_record,
    now_npt,
    save_snapshot,
)

LOG = get_logger("ekantipur")
BASE_URL = "https://election.ekantipur.com"
SOURCE = "ekantipur"

# Ekantipur uses slightly different slugs than ratopati for some districts
SLUG_FIXES = {
    "rauthat": "rautahat",
    "rukumeast": "rukum-east",
    "rukumwest": "rukum-west",
    "nawalparasieast": "nawalparasi-east",
    "nawalparasiwest": "nawalparasi-west",
}


def _extract_competitive_dist(html: str) -> dict:
    """Extract the competitiveDist/competiviveDist JSON object from page HTML.

    Note: the variable name on ekantipur has a typo ('competiviveDist')
    but we match both spellings for resilience.

    Uses brace-counting to find the matching closing brace since the object
    is too large for non-greedy regex to handle correctly.
    """
    # Find the assignment start
    match = re.search(r"const\s+competiv[ive]+Dist\s*=\s*\{", html)
    if not match:
        raise ValueError("Could not find competitiveDist in page HTML")

    # Find the start of the JSON object (the opening brace)
    obj_start = match.end() - 1  # -1 to include the {

    # Count braces to find the matching closing brace
    depth = 0
    for i in range(obj_start, len(html)):
        if html[i] == "{":
            depth += 1
        elif html[i] == "}":
            depth -= 1
            if depth == 0:
                raw = html[obj_start : i + 1]
                break
    else:
        raise ValueError("Could not find closing brace for competitiveDist")

    # Unescape JS forward slashes
    raw = raw.replace(r"\/", "/")
    return json.loads(raw)


def _extract_dists(html: str) -> dict:
    """Extract district metadata from dists['slug'] = {...} assignments."""
    dists = {}
    for match in re.finditer(
        r"dists\[(['\"])(.+?)\1\]\s*=\s*(\{.+?\})\s*;", html
    ):
        slug = match.group(2)
        try:
            dists[slug] = json.loads(match.group(3))
        except json.JSONDecodeError:
            LOG.warning("Failed to parse dists entry for %s", slug)
    return dists


def _extract_regions(html: str) -> dict:
    """Extract regions['slug'] = N assignments (constituency counts per district)."""
    regions = {}
    for match in re.finditer(
        r"regions\[(['\"])(.+?)\1\]\s*=\s*(\d+)\s*;", html
    ):
        regions[match.group(2)] = int(match.group(3))
    return regions


def _normalize_candidate(raw: dict) -> dict:
    """Normalize a single candidate entry from competitiveDist."""
    return {
        "name": raw.get("name", ""),
        "party": raw.get("party_name", ""),
        "votes": raw.get("vote_count", 0),
        "is_winner": bool(raw.get("is_win", 0)),
        "is_leading": bool(raw.get("is_lead", 0)),
        "margin": _parse_int(raw.get("diff", 0)),
        "image": raw.get("image", ""),
        "party_flag": raw.get("flag", ""),
    }


def _parse_int(val) -> int:
    """Parse an int from a value that might be a comma-formatted string."""
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        return int(val.replace(",", "").strip() or "0")
    return 0


def scrape() -> dict:
    """Scrape ekantipur election page and return normalized data."""
    LOG.info("Fetching %s", BASE_URL)
    html = fetch_url(f"{BASE_URL}/?lng=eng")

    competitive_dist = _extract_competitive_dist(html)
    dists = _extract_dists(html)
    regions = _extract_regions(html)

    LOG.info(
        "Extracted %d constituencies, %d districts, %d regions",
        len(competitive_dist),
        len(dists),
        len(regions),
    )

    scraped_at = now_npt().isoformat()
    constituencies = []

    # Process competitive districts from the main object
    for slug, candidates_raw in competitive_dist.items():
        # slug format: "district-N" e.g. "kathmandu-1"
        parts = slug.rsplit("-", 1)
        district_slug = parts[0] if len(parts) == 2 else slug

        # Normalize slug to match ratopati naming
        fixed_district = SLUG_FIXES.get(district_slug, district_slug)
        if fixed_district != district_slug:
            slug = f"{fixed_district}-{parts[1]}" if len(parts) == 2 else fixed_district

        dist_meta = dists.get(district_slug, {})
        province_id = dist_meta.get("pid", 0)
        province_name = dist_meta.get("pname", "")
        district_name = dist_meta.get("name", district_slug.title())

        candidates = [_normalize_candidate(c) for c in candidates_raw]
        # Sort by votes descending
        candidates.sort(key=lambda c: c["votes"], reverse=True)

        record = make_constituency_record(
            source=SOURCE,
            scraped_at=scraped_at,
            province_id=province_id,
            province_name=province_name,
            district=district_name,
            constituency_slug=slug,
            candidates=candidates,
        )
        constituencies.append(record)

    # Build full snapshot
    snapshot = {
        "source": SOURCE,
        "scraped_at": scraped_at,
        "total_constituencies_scraped": len(constituencies),
        "total_districts": len(dists),
        "districts_meta": dists,
        "regions": regions,
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
