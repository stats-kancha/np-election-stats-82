"""Orchestrator: run all available scrapers and produce merged output."""

import json
import sys
import traceback
from pathlib import Path

from scrapers.base import get_logger, save_merged, now_npt, SNAPSHOTS_DIR, DATA_DIR, MERGED_DIR

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


def merge_snapshots(snapshots: dict[str, dict]) -> dict:
    """Merge snapshots from multiple sources into a single view.

    For each constituency, picks the source with the best data quality
    (most non-zero vote counts, most party names filled, highest total votes).
    Other sources are preserved in alt_sources for cross-validation.

    No attempt is made to match candidates across languages (English vs Nepali).
    """
    # Collect all records per constituency slug
    by_slug: dict[str, list[dict]] = {}

    for source, snapshot in snapshots.items():
        if not snapshot:
            continue
        for record in snapshot.get("constituencies", []):
            slug = record["constituency"]
            if slug not in by_slug:
                by_slug[slug] = []
            by_slug[slug].append(record)

    # Pick the best source as primary, rest as alt_sources
    merged_constituencies = {}
    for slug, records in by_slug.items():
        records.sort(key=_source_quality, reverse=True)
        primary = records[0]
        if len(records) > 1:
            primary["alt_sources"] = [
                {
                    "source": r["source"],
                    "candidates": r["candidates"],
                    "scraped_at": r["scraped_at"],
                }
                for r in records[1:]
            ]
        merged_constituencies[slug] = primary

    # Group by province
    by_province = {}
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

    # Include party aggregates from onlinekhabar if available
    party_summary = []
    ok_snapshot = snapshots.get("onlinekhabar")
    if ok_snapshot and "parties" in ok_snapshot:
        party_summary = ok_snapshot["parties"]

    return {
        "scraped_at": now_npt().isoformat(),
        "sources": list(snapshots.keys()),
        "total_constituencies": len(merged_constituencies),
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
