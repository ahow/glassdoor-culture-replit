---
name: Adding a culture framework
description: Touch-points required to add a new culture-scoring framework (like Hofstede/MIT/Schroders) end-to-end.
---

# Adding a culture framework to the dashboard

To add a new framework (Schroders was the third, after Hofstede and MIT), mirror the existing two across **every** layer — missing one layer produces silently-empty data, not an error:

1. **Keyword module** — dimension list, per-dimension pos/neg weighted keywords, and a DIM_INFO dict (title, left/right labels, bipolar vs attribute).
2. **Scoring engine** (`culture_scoring.py`) — return the new framework's `{dim:{score,evidence}}` from `score_review_with_dictionary`. Bipolar score = (pos-neg)/(pos+neg); strength weights High=1.0/Medium=0.75/Low=0.25.
3. **DB columns** — add per-dimension score columns to `review_culture_scores` (init + ALTER for existing tables).
4. **Write path** (`extraction_manager._score_company_reviews` INSERT) — extend column list, placeholders, and values in lockstep.
5. **Aggregation** (`get_company_metrics`) — AVG/COUNT per dim; add the framework key to the metrics dict. Use `value:0` for unscored dims to match Hofstede (the UI renders 0 as neutral; don't switch to None — it diverges and breaks correlation arithmetic).
6. **API endpoints** — culture-profile, industry-average, company-analysis (nested `culture_scores`), culture-performance-scatter. Combined score formula: `hofstede*5 + mit + schroders*5`.
7. **Performance correlation** — TWO places build `culture_data`/payload in `app.py` (the per-company list around the perf-correlation endpoint AND another builder). Both must include the new framework key, and `performance_analysis.calculate_correlation` must loop the new dims.
8. **UI** (`templates/index.html`) — main tab button, tab content div, `switchMainTab` tabs array + tabMapping + load handler, `loadCultureCompanies`/`loadCultureProfile` prefix handling, a `display<Framework>Profile` render fn, inject DIM_INFO/DIMENSIONS via Jinja `|tojson`, and add the new selects to the cross-tab `_companySelectIds`/`_comparatorSelectIds` sync lists.

**Why:** During Schroders rollout the perf-correlation endpoint had two payload builders; only one was updated at first, so correlations were empty. Search for ALL `culture_data.append` / framework-key dicts when wiring a new framework.

**Caveat:** Existing scored reviews lack the new columns; `score_unscored` only scores NEW rows. Full population needs delete + re-score per company. See metrics-cache-rescoring.md for the cache rebuild step.

**Dictionary-swap gotcha (2026-07-24):** `schroders_v2_keywords.py` is not just the keyword dictionary — the app also imports `SCHRODERS_V2_DIM_INFO` (chart labels/descriptions) from it, and app.py silently falls back to an EMPTY dimension list if that import fails. Any regenerated/replacement dictionary file must preserve all exported names (`SCHRODERS_V2_DIMENSIONS`, `SCHRODERS_V2_DIM_INFO`, `DICTIONARY_VERSION`), or every v2 chart goes blank with no server error.
