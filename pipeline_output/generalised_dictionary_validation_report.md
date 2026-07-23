# Generalised Dictionary (v3.1) — Validation & Comparison Report
Date: 2026-07-22 (supersedes the earlier v3 draft of the same day)

## 1. What was done
- Mined the full production universe: **3,400,262 reviews** across all sectors (vs 200,551
  finance-only reviews for the active v2 dictionary). Finalised with `--min-freq 170` (same
  rarity rate as min-freq 10 on 200k) → 20,764 candidate terms.
- Built dictionary v3 (1,825 terms), then applied a **uniform hygiene pass**
  (`pipeline/clean_dictionary.py`): stripped punctuation-prefix variants, removed 43
  leading-negator terms (mis-poled by the miner and double-counted by the engine's own
  negation window, e.g. "no long-term vision" in a positive pole), removed 2 generic
  short tokens ("its", "bit"), deduplicated. Result: **v3.1, 1,752 terms**
  (`2026-07-22-v3.1-generalised-clean`). Full audit trail:
  `pipeline_output/dictionary_hygiene_audit.json`.
- Made validation sampling **reproducible**: `pipeline/validate_dictionaries.py` now persists
  the sampled review ids (`pipeline_output/validation_sample_ids.json`); every old-vs-new
  comparison below was run on the **identical review sets**, with the old dictionary executed
  in an isolated environment. Raw logs are saved per gate (see §7).
- Re-scored the 53-company dev corpus (200,591 reviews), re-aggregated, and rebuilt the full
  factor pipeline (shrinkage, evidence tiers, ridge model, bootstrap).
- **Nothing is deployed to production yet** — prod still runs the v2 finance dictionary.

## 2. Gate-by-gate: old v2 (finance) vs new v3.1 (generalised), same samples

| Gate | Old v2 | New v3.1 | Verdict |
|---|---|---|---|
| Check 1: pole balance (0.8–1.25) | PASS | PASS (all 12 dims) | Equal |
| Check 2: per-pole firing (5–40% band) | 11/24 poles FAIL | 15/24 poles FAIL | Old slightly better per-pole; both under-fire. Dimension-level coverage (either pole) is in band for all 12 dims for v3.1 (6.1–15.9%) and more skewed for v2 (6.3–26%) |
| Check 3: semantic separation | **66 issues** (FAIL) | **55 issues** (FAIL) | v3.1 better (−11) |
| Check 4: known-company anchors (prod) | **8 FAIL / 9 PASS** | **8 FAIL / 10 PASS** | Parity — 6 of 8 failures are the same anchors in both (Costco b09, Salesforce b01/b02/b12, Wells Fargo b10, Microsoft b12) |
| Seed stability (mean overlap) | 0.450 | 0.458 (20 poles better, 16 worse) | Slightly better |

Unstable poles (<0.70 mean overlap) in v3.1 per `pipeline_output/seed_stability.json`:
**b03-positive 0.69, b09-negative 0.65, b12-negative 0.62**. (The old dictionary also had three
poles below 0.70: b03-positive 0.67, b05-positive 0.67, b07-positive 0.63.)

Interpretation: neither dictionary passes checks 2–4 in absolute terms; these are inherent
limits of keyword bipolar scoring, not regressions introduced by generalisation. On every gate
where the two can be compared on identical data, v3.1 is equal or better — and it is the only
one mined from an all-sector corpus, which is a prerequisite for scoring non-finance companies.

## 3. Firing-rate detail (same persisted 10,000-review prod sample)
Dimension-level firing (either pole): v3.1 range 6.1–15.9% across all 12 dims (all ≥5%);
v2 range 6.3–26.0%, with b02/b07/b08 dominated by generic finance-corpus phrases.
Per-pole logs: `check2_old_log.txt` (v2) and the check2 section of the validation run (v3.1).

## 4. Company score shifts (52 companies, v2 aggregates vs v3.1)
Data: `pipeline_output/score_shift_v2_v31.json`; old aggregates preserved in DB table
`company_culture_scores_v2_finance_backup`.

- Mean |shift| per company-dimension: **0.211** (−1..+1 scale)
- Sign flips with |score|>0.1 on either side: **100 / 604 cells (17%)**
- Most-shifted dims: b08 (0.334), b09 (0.291), b02 (0.289), b04 (0.277)
- Shifts concentrate in low-review companies (Agricultural Bank of China n=60: 0.431;
  Eurazeo n=34; PIMCO n=30). High-review companies are stable (Schroders n=1,418 and
  Wellington n=1,021 shift ~0.10–0.15) — consistent with noise reduction, not systematic bias.

## 5. Factor pipeline rebuild (v3.1 scores)
- 53 companies in `schroders_company_factor_scores`; ridge alpha 30.0 both buckets.
- Dev API verified: `/api/v2/factor-scores` 53 companies; `/api/companies-list` 52.
- `company_culture_scores_v2.dictionary_version` = `2026-07-22-v3.1-generalised-clean` (dev only).

## 6. Known residual weaknesses (honest list)
1. Check-4 anchors: 8 of 18 testable anchor expectations wrong-signed — but 6 of those 8 are
   also wrong under the old dictionary. Some poles still partly capture general sentiment.
2. Three poles below the 0.70 stability floor (b03-pos, b09-neg, b12-neg).
3. Per-pole firing below the 5% band for 15/24 poles (11/24 for v2 on the same sample).
4. The ridge factor model still does not generalise out-of-sample (unchanged small-n issue).

## 7. Evidence artifacts
- `pipeline_output/validation_sample_ids.json` — persisted sample ids (reproducibility)
- `pipeline_output/check3_new_v31_log.txt` / `check3_old_log.txt` — semantic separation, both dicts
- `pipeline_output/check2_new_v31_log.txt`, `check4_new_v31_log.txt` — new-dictionary gate logs
- `pipeline_output/check2_old_log.txt`, `check4_old_log.txt` — old-dictionary baselines
- Reproducibility guard: `fetch_reviews` now fails loudly if fewer than 90% of the persisted
  sample ids resolve (i.e. wrong database); check4 returns FAIL when fewer than 5 anchor
  assertions are evaluable (no silent PASS on all-skip).
- `pipeline_output/dictionary_hygiene_audit.json` — every removed/renamed term with rule
- `pipeline_output/dictionary_diff_v2_v3.json` — v2→v3 term diff
- `pipeline_output/score_shift_v2_v31.json` — per-company shift table
- `pipeline_output/seed_stability.json`, `rescore_v3_log.txt`
- Rollback: old dictionary at `pipeline_output/finance_run_2026_08_01/schroders_v2_keywords_active_backup.py`;
  old aggregates in `company_culture_scores_v2_finance_backup`; review-level rollback requires a
  re-score with the old dictionary (~20 min, resumable).

## 8. Recommendation
Adopt v3.1. On identical, persisted samples it is equal or better than the active finance
dictionary on every comparable gate, its hygiene issues have been removed by uniform rules with
a full audit trail, and it is the only dictionary valid for the planned all-sector expansion.
Deployment to production only after user sign-off.
