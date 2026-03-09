"""Orchestrator: run all available scrapers and produce merged output."""

import json
import re
import sys
import traceback
from pathlib import Path

from scrapers.base import (
    get_logger,
    is_devanagari,
    normalize_text,
    now_npt,
    save_merged,
    slug_to_title,
    DATA_DIR,
    MERGED_DIR,
    SNAPSHOTS_DIR,
)

LOG = get_logger("run_all")

SCRAPERS = {
    "ekantipur": "scrapers.ekantipur",
    "ratopati": "scrapers.ratopati",
    "onlinekhabar": "scrapers.onlinekhabar",
}


def run_scraper(name: str, module_path: str) -> dict | None:
    """Import and run a single scraper, returning its snapshot or None on failure."""
    try:
        LOG.info("Running %s scraper...", name)
        mod = __import__(module_path, fromlist=["run"])
        snapshot = mod.run()
        LOG.info("%s: OK (%d constituencies)", name, snapshot.get("total_constituencies_scraped", 0))
        return snapshot
    except Exception:
        LOG.error("Scraper %s failed:\n%s", name, traceback.format_exc())
        return None


def _source_quality(record: dict) -> tuple:
    """Score a constituency record for merge priority.

    Returns a tuple for sorting (higher = better):
    - number of candidates with non-zero votes
    - whether party names are populated
    - total vote count
    """
    candidates = record.get("candidates", [])
    votes_reported = sum(1 for c in candidates if c.get("votes", 0) > 0)
    parties_filled = sum(1 for c in candidates if c.get("party"))
    total_votes = sum(c.get("votes", 0) for c in candidates)
    return (votes_reported, parties_filled, total_votes)


def _strip_parens(text: str) -> str:
    """Strip parenthesis characters but keep their content, normalize whitespace.

    This lets 'नेकपा (एमाले)' match 'नेकपा एमाले' after stripping.
    """
    text = text.replace("(", " ").replace(")", " ")
    text = text.replace("（", " ").replace("）", " ")
    return re.sub(r"\s+", " ", text).strip()


def _extract_name_from_image_url(url: str) -> str:
    """Extract romanized candidate name from an image URL.

    e.g. '.../rabi-lamichhane_ZcjOIBS4PL.jpg' -> 'rabi lamichhane'
    """
    if not url:
        return ""
    # Get filename without extension
    filename = url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    # Remove random hash suffix after underscore (e.g. '_ZcjOIBS4PL')
    name_part = re.sub(r"_[A-Za-z0-9]{6,}$", "", filename)
    return name_part.replace("-", " ").strip().lower()


def _names_match(slug_name: str, full_name: str) -> bool:
    """Check if a name extracted from URL slug fuzzy-matches a full name.

    Handles truncation, romanization variants, etc.
    """
    if not slug_name or not full_name:
        return False

    s_words = slug_name.lower().split()
    f_words = full_name.lower().split()
    if not s_words or not f_words:
        return False

    # Count matching words (prefix matching handles truncation/romanization)
    overlap = 0
    for sw in s_words:
        for fw in f_words:
            if sw == fw or (
                len(sw) >= 3
                and (fw.startswith(sw) or sw.startswith(fw))
            ):
                overlap += 1
                break

    min_words = min(len(s_words), len(f_words))
    # Require at least 2 matching words (or all if fewer than 2)
    required = min(2, min_words)
    return overlap >= required and overlap / min_words >= 0.5


def _build_party_mapping(snapshots: dict[str, dict]) -> dict[str, str]:
    """Dynamically build Nepali->English party name mapping from overlapping data.

    Uses two methods for matching candidates across sources:
    1. Vote count matching (when both sources have non-zero votes)
    2. Image URL slug matching (when one source has 0 votes)

    From matched candidates, extracts {nepali_party_name: english_party_name}.
    """
    LOG.info("Building dynamic party name mapping from overlapping data...")
    mapping: dict[str, str] = {}  # normalized_nepali -> english

    # Index candidates by constituency slug and source
    by_slug: dict[str, dict[str, list[dict]]] = {}
    for source, snapshot in snapshots.items():
        if not snapshot:
            continue
        for record in snapshot.get("constituencies", []):
            slug = record["constituency"]
            by_slug.setdefault(slug, {})[source] = record.get("candidates", [])

    # Find overlapping constituencies with ekantipur (the English source)
    english_source = "ekantipur"
    for slug, sources in by_slug.items():
        if english_source not in sources:
            continue

        en_candidates = sources[english_source]

        for other_source, ne_candidates in sources.items():
            if other_source == english_source:
                continue

            matched_en: set[str] = set()  # track matched ekantipur candidates

            for ne_cand in ne_candidates:
                ne_party = normalize_text(ne_cand.get("party", ""))
                if not ne_party or not is_devanagari(ne_party):
                    continue
                if ne_party in mapping:
                    continue  # already have this mapping

                best_match = None

                # Method 1: Vote count matching (most reliable)
                ne_votes = ne_cand.get("votes", 0)
                if ne_votes > 0:
                    for en_cand in en_candidates:
                        en_name = en_cand.get("name", "")
                        if en_name in matched_en:
                            continue
                        if en_cand.get("votes", 0) == ne_votes:
                            best_match = en_cand
                            break

                # Method 2: Image URL slug matching (fallback)
                if not best_match:
                    ne_name_slug = _extract_name_from_image_url(
                        ne_cand.get("image", "")
                    )
                    if ne_name_slug:
                        for en_cand in en_candidates:
                            en_name = en_cand.get("name", "")
                            if en_name in matched_en:
                                continue
                            if _names_match(ne_name_slug, en_name):
                                best_match = en_cand
                                break

                if best_match:
                    en_party = normalize_text(best_match.get("party", ""))
                    if en_party and not is_devanagari(en_party):
                        mapping[ne_party] = en_party
                        matched_en.add(best_match.get("name", ""))

    # Build extended mapping with parentheses-stripped variants
    # This lets 'नेकपा एमाले' find the mapping for 'नेकपा (एमाले)'
    extended: dict[str, str] = dict(mapping)
    for ne, en in mapping.items():
        stripped = _strip_parens(ne)
        if stripped != ne and stripped not in extended:
            extended[stripped] = en

    LOG.info(
        "Built party mapping: %d direct + %d variant = %d total entries",
        len(mapping),
        len(extended) - len(mapping),
        len(extended),
    )
    return extended


def _translate_party(party: str, mapping: dict[str, str]) -> str:
    """Look up a Nepali party name in the mapping.

    Returns English name if found, original party name otherwise.
    """
    if not party or not is_devanagari(party):
        return party

    norm = normalize_text(party)
    if norm in mapping:
        return mapping[norm]

    # Try with parentheses stripped
    stripped = _strip_parens(norm)
    if stripped in mapping:
        return mapping[stripped]

    return party


def merge_snapshots(snapshots: dict[str, dict]) -> dict:
    """Merge snapshots from multiple sources into a single view.

    For each constituency, picks the source with the best data quality
    (most non-zero vote counts, most party names filled, highest total votes).
    Other sources are preserved in alt_sources for cross-validation.

    Translates Nepali party names to English using a dynamically-built mapping
    derived from overlapping constituency data across sources.
    Enriches candidate names with English equivalents where available.
    """
    # Step 1: Build party mapping dynamically from overlapping data
    party_mapping = _build_party_mapping(snapshots)

    # Step 2: Collect all records per constituency, translating party names
    by_slug: dict[str, list[dict]] = {}

    for source, snapshot in snapshots.items():
        if not snapshot:
            continue
        for record in snapshot.get("constituencies", []):
            slug = record["constituency"]
            # Translate Nepali party names to English for all candidates
            for cand in record.get("candidates", []):
                original_party = cand.get("party", "")
                translated = _translate_party(original_party, party_mapping)
                if translated != original_party:
                    cand["party_ne"] = original_party
                    cand["party"] = translated
            by_slug.setdefault(slug, []).append(record)

    # Step 3: Pick best source as primary, enrich with English candidate names
    merged_constituencies = {}
    for slug, records in by_slug.items():
        records.sort(key=_source_quality, reverse=True)
        primary = records[0]

        # Build English candidate name lookup from ekantipur data
        # key = normalized party name, value = English candidate name
        # Skip independents (multiple per constituency, can't match by party)
        en_name_by_party: dict[str, str] = {}
        for r in records:
            if r.get("source") == "ekantipur":
                for c in r.get("candidates", []):
                    party = normalize_text(c.get("party", ""))
                    name = c.get("name", "")
                    if (
                        party
                        and party.lower() != "independent"
                        and name
                        and not is_devanagari(name)
                    ):
                        en_name_by_party[party] = name

        # Enrich primary record candidates with English names
        if primary.get("source") != "ekantipur" and en_name_by_party:
            for cand in primary.get("candidates", []):
                party = normalize_text(cand.get("party", ""))
                if (
                    party in en_name_by_party
                    and is_devanagari(cand.get("name", ""))
                ):
                    cand["name_ne"] = cand["name"]
                    cand["name"] = en_name_by_party[party]

        # Build alt_sources, also enriching their candidates
        alt_records = records[1:]
        if alt_records:
            primary["alt_sources"] = []
            for r in alt_records:
                if r.get("source") != "ekantipur" and en_name_by_party:
                    for cand in r.get("candidates", []):
                        party = normalize_text(cand.get("party", ""))
                        if (
                            party in en_name_by_party
                            and is_devanagari(cand.get("name", ""))
                        ):
                            cand["name_ne"] = cand["name"]
                            cand["name"] = en_name_by_party[party]
                primary["alt_sources"].append(
                    {
                        "source": r["source"],
                        "candidates": r["candidates"],
                        "scraped_at": r["scraped_at"],
                    }
                )

        merged_constituencies[slug] = primary

    # Group by province
    by_province: dict[str, dict] = {}
    for record in merged_constituencies.values():
        pid = record["province"]["id"]
        pname = record["province"]["name"]
        key = f"{pid}-{pname}"
        if key not in by_province:
            by_province[key] = {
                "province_id": pid,
                "province_name": pname,
                "constituencies": [],
            }
        by_province[key]["constituencies"].append(record)

    # Sort constituencies within each province
    for prov in by_province.values():
        prov["constituencies"].sort(key=lambda r: r["constituency"])

    # Include party aggregates from onlinekhabar, enriched with English names
    party_summary = []
    ok_snapshot = snapshots.get("onlinekhabar")
    if ok_snapshot and "parties" in ok_snapshot:
        for p in ok_snapshot["parties"]:
            entry = dict(p)  # copy to avoid mutating snapshot
            slug = entry.get("party_slug", "")
            if slug:
                entry["party_name_en"] = slug_to_title(slug)
            party_summary.append(entry)

    return {
        "scraped_at": now_npt().isoformat(),
        "sources": list(snapshots.keys()),
        "total_constituencies": len(merged_constituencies),
        "party_mapping_count": len(party_mapping),
        "party_summary": party_summary,
        "provinces": sorted(by_province.values(), key=lambda p: p["province_id"]),
    }


def generate_manifest() -> Path:
    """Generate data/manifest.json listing all available snapshot and merged files."""
    manifest = {"snapshots": {}, "merged": []}

    for source_dir in sorted(SNAPSHOTS_DIR.iterdir()):
        if source_dir.is_dir():
            files = sorted(f.name for f in source_dir.glob("*.json"))
            if files:
                manifest["snapshots"][source_dir.name] = files

    merged_files = sorted(f.name for f in MERGED_DIR.glob("*.json"))
    manifest["merged"] = merged_files

    manifest_path = DATA_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    LOG.info("Generated manifest: %s", manifest_path)
    return manifest_path


def load_latest_snapshots() -> dict[str, dict]:
    """Load the most recent snapshot file for each source from disk."""
    snapshots = {}
    for source in SCRAPERS:
        source_dir = SNAPSHOTS_DIR / source
        if not source_dir.exists():
            continue
        files = sorted(source_dir.glob("*.json"))
        if files:
            snapshots[source] = json.loads(files[-1].read_text())
            LOG.info("Loaded %s snapshot: %s", source, files[-1].name)
    return snapshots


def main():
    merge_only = "--merge-only" in sys.argv

    if merge_only:
        LOG.info("=== Merge-only mode ===")
        snapshots = load_latest_snapshots()
    else:
        LOG.info("=== Election Stats Scraper Run ===")
        snapshots = {}
        for name, module_path in SCRAPERS.items():
            snapshots[name] = run_scraper(name, module_path)

    successful = {k: v for k, v in snapshots.items() if v is not None}
    if not successful:
        LOG.error("All scrapers failed!")
        sys.exit(1)

    merged = merge_snapshots(successful)
    save_merged(merged)

    generate_manifest()

    LOG.info(
        "=== Done: %d sources, %d constituencies ===",
        len(successful),
        merged["total_constituencies"],
    )


if __name__ == "__main__":
    main()
