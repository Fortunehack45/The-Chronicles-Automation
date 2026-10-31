# The Chronicles — Automated 24/7 Newsroom Wire

Autonomous real-time journalism ingestion daemon for **The Chronicles** broadsheet news platform.

## Features
- **Continuous 1-Minute Polling Daemon**: Scans verified Nigerian and global wire feeds every 60 seconds.
- **High-Throughput Publishing**: Publishes up to 10 articles per minute in parallel across 10 worker threads.
- **Dynamic Content Classification**: Dynamically classifies dispatches across all 11 broadsheet beats (`NIGERIA`, `WORLD`, `POLITICS`, `FINANCE`, `TECHNOLOGY`, `SCIENCE`, `SPORT`, `EDUCATION`, `LIFESTYLE`, `FASHION`, `CELEBRITY`).
- **Wire Freshness & Lifecycle Detection**: Detects breaking events and computes story age in minutes to assign accurate lifecycle stages (`BREAKING`, `HEADLINE`, `DEVELOPING`, `LATEST`, `REGULAR`).
- **Signature Broadsheet Formatting**: 1,000+ words per dispatch with 2-line red drop caps, beat-tailored executive summaries, real quote attributions, and authentic high-resolution watermarked press photos.
- **Multi-Layer Anti-Duplicate Engine**: 5-tier fuzzy matching and persistent ledger preventing duplicate coverage.
