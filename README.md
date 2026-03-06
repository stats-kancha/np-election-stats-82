# np-election-stats-82

Aggregated Nepal Election 2082 results scraped from multiple portals every few minutes.

## Sources

| Portal | Method | Coverage |
|---|---|---|
| election.ekantipur.com | Embedded JS extraction | 17 competitive seats |
| election.ratopati.com | District API + HTML scraping | All 165 constituencies |
| election.onlinekhabar.com | REST API | 50 party aggregates |

## Usage

```bash
just setup          # install deps with uv
just scrape         # run all scrapers once
just scrape-loop    # run every 60s

# individual scrapers
just scrape-ekantipur
just scrape-ratopati
just scrape-onlinekhabar
```

## Data

- `data/snapshots/{source}/{timestamp}.json` — raw snapshots per source
- `data/merged/latest.json` — unified view across all sources
- `data/index/constituencies.json` — full constituency index (165 seats, 7 provinces)

## GitHub Actions

Scrapers run as parallel matrix jobs every 5 minutes, with a merge step that combines all snapshots into `latest.json`.
