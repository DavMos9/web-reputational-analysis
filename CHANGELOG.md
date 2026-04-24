# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Fixed

- `aggregator.py` — `EntitySummary` now includes a `queries` field: the sorted list of
  distinct queries used during collection. Provides context in the summary JSON about
  what was searched for, not just the target entity.
- `cleaner.py` — added `_truncate_text()`: the `text` field is now capped at
  `MAX_TEXT_LENGTH` characters (default 1500) after cleaning, truncated at the nearest
  word boundary. XLM-RoBERTa's effective limit is ~512 tokens (≈350–600 chars); longer
  texts were silently truncated by the tokenizer anyway. Configurable in `config.py`
  (set to 0 to disable). Does not affect `title` or any other field.
- `config.py` — added `MAX_TEXT_LENGTH = 1500` constant with validation.
- `wikitalk_collector.py` — `_clean_wikitext()` now strips HTML tags from wikitext.
  `<ref>...</ref>` and `<ref .../>` are removed with their content (citations).
  All other inline tags (`<br>`, `<small>`, `<nowiki>`, `<s>`, etc.) are stripped
  while preserving their text content. Sub-section heading markers (`== ... ==`)
  within a section are also removed.
- Added `tests/test_wikitalk_collector.py` with 17 unit tests for `_clean_wikitext`.

---

## [1.0.0] — 2026-04-20

First stable release. The pipeline is complete end-to-end: collection, normalization,
NLP enrichment, deduplication, date filtering, language filtering, and export.
433 unit and integration tests, all passing.

### Added

**Pipeline**
- `pipeline/language_filter.py` — post-enrichment language filter (`--languages`).
  Accepts one or more ISO 639-1 codes; records without a detected language are always kept.
- CLI parameter `--languages` in `main.py` and `languages` field in `PipelineConfig`.

**Collectors (18 sources)**
- `news` (NewsAPI), `gdelt` (GDELT DOC 2.0), `wikipedia`, `wikitalk`
- `youtube`, `youtube_comments` (YouTube Data API v3)
- `guardian` (The Guardian Open Platform), `nyt` (New York Times)
- `bluesky`, `mastodon`, `lemmy`, `reddit`
- `brave` (Brave Search), `gnews_it` (Google News Italy RSS)
- `bbc` (BBC News RSS), `ansa` (ANSA RSS)
- `stackexchange`, `hackernews` — **opt-in** sources (excluded from default run)
- `collectors/retry.py` — shared `http_get_with_retry` with exponential backoff,
  anti-thundering-herd jitter, and status-code-aware retry logic.

**Pipeline modules**
- `pipeline/normalizer.py` — RawRecord → Record (YYYY-MM-DD dates, URL, domain extraction)
- `pipeline/cleaner.py` — HTML entity decoding, control char removal, U+2028/U+2029, NFC normalization
- `pipeline/deduplicator.py` — deduplication by canonical URL and title+domain fingerprint
- `pipeline/enricher.py` — language detection (langdetect) + XLM-RoBERTa sentiment,
  batch inference, configurable confidence threshold (`NLP_LANG_DETECT_MIN_CONFIDENCE`)
- `pipeline/date_filter.py` — date range filter (`--since YYYY-MM-DD`)
- `pipeline/aggregator.py` — composite reputation score, linear regression trend,
  `source_distribution`, weighted `sentiment_std`
- `pipeline/runner.py` — orchestrator with `PipelineConfig`, parallel collection
  (`ThreadPoolExecutor`), fail-fast on unknown sources

**Storage & Export**
- `storage/raw_store.py` — RawRecord persistence in `data/raw/`, cleanup via `--keep-raw-days`
- `exporters/json_exporter.py`, `csv_exporter.py`, `summary_json_exporter.py`

**Data models**
- `models/record.py` — `RawRecord` and `Record` with unified 15-field schema
- `normalizers/utils.py` — shared `normalize_language_code` and `HTML_TAG_RE`

### Fixed

- `reddit_collector.py` — replaced manual retry logic with centralized `http_get_with_retry`
- `bluesky_collector.py` — added `threading.Lock` on JWT token for concurrent use
- `cleaner.py` — removed C0/C1 Unicode control characters + U+2028 LINE SEPARATOR
  and U+2029 PARAGRAPH SEPARATOR (caused VS Code warnings in exported JSON files)
- `deduplicator.py` — added `wikitalk` to `TITLE_DEDUP_EXCLUDED_SOURCES` to prevent
  false deduplication (talk pages have structurally repeated titles)
- `enricher.py` — removed `_default_enricher` singleton and module-level wrappers;
  refactored to batch NLP inference (2–10x faster); replaced `detect()` with
  `detect_langs()` + confidence threshold (`NLP_LANG_DETECT_MIN_CONFIDENCE=0.80`)
  to eliminate false positives on short and ambiguous texts
- `main.py` — added `min=1` validation for `--keep-raw-days`

### Technical notes

- **Sentiment model:** `cardiffnlp/twitter-xlm-roberta-base-sentiment` (XLM-RoBERTa base, ~1.1 GB).
  Supported languages: ar, de, en, es, fr, hi, it, pt.
  Score in `[-1.0, 1.0]` computed as `P(positive) − P(negative)`.
- **Reputation score:** composite in `[0.0, 1.0]`, weighted average of sentiment (0.40),
  source trust (0.30), recency (0.20), volume (0.10).
- **Tests:** 433 unit and integration tests (pytest). Coverage: collectors, normalizer,
  cleaner, deduplicator, enricher, aggregator, exporters, runner, end-to-end pipeline.

---

## [0.5.0] — 2026-04-01

Development pre-release. Working pipeline with 16 sources, NLP enrichment,
and initial test suite. Not intended for public use.
