---
name: Metrics cache & re-scoring
description: How the company_metrics_cache interacts with re-scoring, and why correlation/perf endpoints can show empty data after a re-score.
---

# Metrics cache & re-scoring (Glassdoor Culture dashboard)

Re-scoring reviews for a company (`/api/score-company/<name>`) calls `invalidate_cache(company)`, which **DELETEs** that company's row from `company_metrics_cache`. It does not rebuild it.

The performance/correlation endpoints (`/api/performance-correlation`, scatter) are **cache-only** — they read `get_cached_metrics_batch(...)` with no live-DB fallback (deliberate, to avoid N×3 query timeouts). So a company whose cache row was just deleted is **silently excluded** from those endpoints until its cache is rebuilt.

**How to apply:** After re-scoring (or any change to `get_company_metrics` output shape, e.g. adding a new framework), the cache must be repopulated before correlation/perf data reappears. Hitting `/api/culture-profile/<name>` (or the other endpoints that call `cache_metrics(...)`) rebuilds the row. For a full dataset, the existing cache-warming / re-score UI repopulates everything.

**Why:** Caused confusing "0 dimensions with data" results during the Schroders rollout — the new framework's correlations were empty not because of a wiring bug but because re-scored companies' cache rows were deleted and stale rows lacked the new keys. Old cached rows still serve old shapes until rebuilt.

**Update (2026-07-24):** The prewarm endpoint's background thread only caches the 'all' variant; `/api/culture-profile` caches both 'all' and 'current'. A cache row counts as fully warm only when BOTH `metrics_json` and `metrics_json_current` are non-null — otherwise selecting 'current employees' still recomputes live. Full-fleet warming (~2k companies) is a Heroku one-off dyno job, not a web-dyno thread.
