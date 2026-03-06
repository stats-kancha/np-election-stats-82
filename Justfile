default:
    @just --list

# Install dependencies
setup:
    uv sync

# Run all scrapers once
scrape:
    uv run python -m scrapers.run_all

# Run scrapers in a loop every 60 seconds
scrape-loop:
    #!/usr/bin/env bash
    echo "Scraping every 60 seconds. Ctrl+C to stop."
    while true; do
        uv run python -m scrapers.run_all
        echo "--- sleeping 60s ---"
        sleep 60
    done

# Run individual scrapers
scrape-ekantipur:
    uv run python -m scrapers.ekantipur

scrape-ratopati:
    uv run python -m scrapers.ratopati

scrape-onlinekhabar:
    uv run python -m scrapers.onlinekhabar

# Clean all snapshot data
clean:
    rm -rf data/snapshots/*/*.json data/merged/*.json
