# Culture Factor Methodology — Implementation Specification

**System:** Culture Analytics Dashboard (culturescoring.com)
**Framework:** Schroders v2 (12 bipolar culture dimensions)
**Pipeline:** `pipeline/factor_build.py` (Python, PostgreSQL, NumPy)
**Last updated:** 26 July 2026 (per-group peer-level assignment)

This document describes, end to end, how a company's Culture Factor score is
produced from raw Glassdoor reviews. It is written for a developer who needs
to reproduce, audit, or extend the implementation. Section numbers (§5–§17)
refer to the internal reviewer brief the pipeline implements.

---

## 1. Overview of the pipeline

```
raw reviews (reviews table, ~3.4M rows)
   │  dictionary scoring (per review x dimension)
   ▼
review_culture_scores  (schroders_v2_d01..d18 score + evidence columns)
   │  §5-§6  aggregation + evidence tiers          → factor_build.py evidence
   ▼
schroders_company_dimension_evidence  (company x dimension)
   │  §7     per-group peer bucket assignment
   │  §8     shrinkage toward peer-group mean       → factor_build.py shrink
   ▼
shrunk dimension scores (same table, extra columns)
   │  §9     per-bucket ridge model vs performance
   │  §10    factor score (raw / z / percentile)
   │  §11-§14 reliability, concentration, overlap,  → factor_build.py model
   │          bootstrap stability
   ▼
schroders_sector_model_weights, schroders_company_factor_scores,
schroders_overlap_diagnostics, pipeline_output/factor_run_log.json
```

Run commands (each stage is idempotent; `model` re-runs `shrink` internally):

```
python pipeline/factor_build.py settings    # create/refresh settings table
python pipeline/factor_build.py termscan    # resumable per-company term scan
python pipeline/factor_build.py evidence    # §5-§6
python pipeline/factor_build.py shrink      # §7-§8
python pipeline/factor_build.py model       # §9-§14
python pipeline/factor_build.py all         # evidence → shrink → model
```

All tunable parameters live in the `schroders_factor_settings` table
(key/JSONB value), seeded from `DEFAULT_SETTINGS` in the script. Values in
the table override the code defaults, so behaviour can be changed without a
deploy.

---

## 2. Review-level scoring (input to the pipeline)

Each review's text (summary + pros + cons + advice-to-management,
concatenated and lower-cased) is scored against a mined keyword dictionary
(`schroders_v2_keywords.py`, versioned via `DICTIONARY_VERSION`) by the
scoring engine (`scoring_engine_v2.py`, versioned via
`SCORING_ENGINE_VERSION`).

- 12 bipolar dimensions; each has a *positive* and a *negative* pole with
  weighted terms/phrases.
- Phrase matching is token-based (`compile_phrase`,
  `find_matches_with_negation`) with a 5-token negation window: a negated
  match ("not collaborative") flips its contribution to the opposite pole.
- Per review x dimension the engine produces a score in **[−1, +1]**
  (net weighted positive vs negative evidence) and an evidence magnitude.
  A review with no matching terms for a dimension gets NULL (not zero).
- Results are stored in `review_culture_scores` as
  `schroders_v2_<dim>_score` and `schroders_v2_<dim>_evidence`.

The **term scan** (`termscan` command) additionally accumulates, per
company x dimension, the weighted contribution of every individual term
(used later for concentration checks). It is resumable: state in
`pipeline_output/termscan_state.json` + `termscan_acc.pkl`, processed in
batches of 4,000 reviews ordered by review id.

## 3. §5–§6 Company x dimension evidence table

`build_evidence()` drops and rebuilds
`schroders_company_dimension_evidence` (primary key: company, dimension).
For every company and each of the 12 dimensions it computes from
`review_culture_scores`:

- `n_scored_reviews_dimension` (count of non-NULL scores), company total
  review count, share scored;
- mean / sample stddev / standard error of the raw dimension score;
- counts of positive-scoring and negative-scoring reviews;
- `top_5_reviews_contribution_share` — share of total evidence magnitude
  carried by the 5 highest-evidence reviews;
- `top_5_terms_contribution_share` — share of total weighted term
  contribution carried by the top 5 terms (from the term scan).

**Evidence tiers (§6)** per company x dimension, with the Tier D rule taking
precedence:

| Tier | Rule |
|------|------|
| D | fewer than 5 scored reviews for the dimension |
| A | ≥ 50 scored reviews for the dimension AND ≥ 150 total reviews |
| B | 20–49 scored reviews, OR total reviews 50–149 |
| C | everything else |

GICS classification (sector / industry / sub-industry) is joined from
`extraction_queue` on the Glassdoor name; companies not present there
(the 14 unlisted asset managers) get `Asset Management/Other` at every
level.

## 4. §7 Peer-group assignment (per-group, since 2026-07-26)

Implemented in `pick_bucket(companies_info, settings, adequate=None)`.
Parameters: `peer_hierarchy = [gics_sub_industry, gics_industry, gics_sector]`,
`min_companies_per_bucket = 8`.

Algorithm — greedy, finest-first, **residual counting**:

1. Start with all companies unassigned.
2. At the sub-industry level, count unassigned **adequate** companies per
   sub-industry (see below). Every company — adequate or not — whose
   sub-industry has ≥ 8 unassigned adequate members is assigned a bucket
   named after that sub-industry.
3. Repeat at industry level for the companies still unassigned, then at
   sector level.
4. Anything still unassigned goes to a single `global` bucket.

**Adequacy (added 2026-07-26, same day):** only *model-estimable* companies
count toward the ≥ 8 threshold — those with a composite performance target
(financial data) **and** ≥ `min_dims_medium` (6) Tier A/B culture
dimensions. `build_shrinkage()` computes this set and passes it as
`adequate`. Consequence: buckets are coarser on average, but every named
bucket is guaranteed ≥ 8 model-usable companies, so **every named bucket
estimates its own group-specific model** — the `global_fallback` model
level is eliminated. Only the residual `global` bucket (companies that are
not estimable and whose groups never reached 8 adequate members) uses the
globally estimated model. When `adequate=None`, all companies count
(pre-update behaviour).

Properties (unit-tested in `pipeline/test_pick_bucket.py`):

- Buckets are **disjoint** — each company belongs to exactly one bucket, at
  exactly one level.
- Every non-global bucket has **≥ 8 members**, guaranteeing an adequate
  peer set for ranking.
- **Residual counts are deliberate.** Counting full group sizes instead
  would let leftover companies claim an industry bucket whose other members
  were already taken at sub-industry level, leaving the actual ranking peer
  set below 8. The trade-off (occasionally coarser assignment) is accepted.
- On the rare GICS name collision across levels (e.g. "Distributors" is
  both an industry and a sub-industry name), the later bucket is renamed
  `"<name> (<level>)"`.
- Iteration over companies is in sorted order, so assignment is
  deterministic.

The function returns `(buckets: {company → bucket_name},
bucket_level: {bucket_name → level})`; the level is stored per bucket and
propagated to every output table (`classification_level`). There is no
longer a single global "classification level used" — the run log records
`classification_level_used: "per_group_mixed"` plus per-level company
counts.

Production result (26 Jul 2026, 1,957 companies): 87 sub-industry buckets,
11 industry, 8 sector, 1 global.

## 5. §8 Shrinkage toward the peer-group mean

`build_shrinkage()` computes, per bucket x dimension, the **prior**: the
mean raw dimension score over bucket members that have any scored reviews.
Each company's raw mean is then shrunk toward that prior with an
empirical-Bayes weight:

```
w      = n / (n + k)              # n = scored reviews for the dimension
shrunk = w * raw_mean + (1 − w) * prior
```

`k = 50` for every dimension (configurable per dimension via the
`shrinkage_k` setting). If a company has no scored reviews for a dimension,
`w = 0` and the shrunk value equals the prior.

Two columns are written:

- `mean_dimension_score_shrunk` — the **published** value; NULL for Tier D
  (too little evidence to publish);
- `mean_dimension_score_shrunk_internal` — always populated; used
  internally as model input.

## 6. §9 Per-bucket ridge model

`build_model()` links culture to financial performance.

**Target variable** (`perf_targets`): a composite performance score per
company from `fmp_performance_metrics` (joined case-insensitively on
company name): z-score each of 5-yr avg ROE, 5-yr revenue growth, 5-yr TSR,
5-yr avg operating margin across all available companies, clamp each z to
[−2, +2], then take the weighted mean with weights 0.30 / 0.25 / 0.25 /
0.20 (renormalised over the metrics the company actually has).

**Estimation set:** a company enters model estimation only if it has a
performance target AND ≥ 6 dimensions at Tier A/B (`min_dims_medium`).
Exclusion reasons are recorded in the run log.

**Design matrix:** 12 columns = shrunk-internal dimension scores; missing
cells imputed with the column mean; columns standardized (mean 0, sd 1)
using the estimation set's own mu/sd.

**Fit:** ridge regression with intercept (intercept not penalized), alpha
selected by leave-one-out CV over `[0.1, 0.3, 1.0, 3.0, 10.0, 30.0]`.
Reported metrics: in-sample R² and LOO cross-validated R².

**Hierarchy of models:**

1. A **global model** is fitted first on all eligible companies — this is
   the fallback anchor.
2. For each bucket with ≥ 8 eligible companies (`min_companies_model`), a
   bucket-specific ridge is fitted, then its coefficients are shrunk toward
   the global coefficients:

   ```
   lambda = n_est / (n_est + m)        # m = coef_shrink_m = 10
   coef   = lambda * coef_bucket + (1 − lambda) * coef_global
   ```

3. Buckets with < 8 eligible companies use the global model directly;
   their `sector_model_level_used` is recorded as `global_fallback`
   (vs `gics_sub_industry` / `gics_industry` / `gics_sector` / `global`).

Coefficient stability is estimated by bootstrap (up to 100 resamples of the
estimation set, refitting at the selected alpha) and stored as
`coefficient_stability_sd` per dimension.

Output table: `schroders_sector_model_weights` — one row per bucket x
dimension with the final weight, predictor mu/sd, alpha, R², CV-R², lambda,
level metadata.

## 7. §10 Factor score

For each company, using its bucket's model:

```
x_j       = (shrunk_internal_j − mu_j) / sd_j     # per dimension
factor_raw = coef · x
```

Dimensions that are NULL or Tier D are set to the bucket mean (i.e. a
neutral 0 after standardization) — missing evidence neither helps nor
hurts.

Within each bucket, `factor_raw` is converted to:

- `schroders_factor_sector_z` — z-score within the bucket;
- `schroders_factor_sector_pctile` — mid-rank percentile
  `100 * (rank + 0.5) / n_bucket`.

Ranking is always **within the company's own peer bucket**, even when the
model weights came from the global fallback.

## 8. §12 Concentration checks

Per company, count Tier A/B dimensions where evidence is over-concentrated:
top-5 reviews carry > 30% of evidence (`concentration_share_threshold`), or
top-5 terms carry > 80% of weighted term contribution
(`concentration_term_share_threshold` — deliberately higher because with
125–158 terms per dimension the top 5 naturally carry ~60% corpus-wide;
documented deviation from the brief's 30%). A company with **more than 3**
flagged dimensions (`concentration_max_flagged_dims`) is "severely
concentrated" and gets a reliability downgrade (below).

## 9. §14 Bootstrap rank stability

200 bootstrap replicates (`bootstrap_reps`) per bucket: resample the
bucket's estimation set (or the global set for fallback buckets), refit at
the selected alpha (re-applying coefficient shrinkage toward the global
model), re-score and re-rank all bucket members. Recorded per company:
mean rank, rank SD, percentile SD, and frequency of landing in the top /
bottom quintile.

## 10. §11 Reliability tiers

Per company (n_AB = number of Tier A/B dimensions; n_bucket = peer-group
size):

1. **Insufficient** if no score, n_AB < 4, or n_bucket < 3.
2. **High** if n_AB ≥ 8, not severely concentrated, and n_bucket ≥ 8.
3. **Medium** if n_AB ≥ 6 and not severely concentrated.
4. **Low** otherwise.
5. Downgrade one tier if severely concentrated (High→Medium etc.).
6. Downgrade one more tier if bootstrap percentile SD > 15
   (`rank_pctile_sd_downgrade`).

## 11. §13 Overlap / multicollinearity diagnostics

Per bucket (and the pooled "ALL" set), on the standardized design matrix:
pairwise dimension correlations flagged at |r| > 0.75, and VIF per
dimension (regress it on the other 11). Flags: REVIEW at VIF > 4,
STRONG_WARNING at > 5, FAIL at > 10, with suggested actions. Written to
`schroders_overlap_diagnostics`. These are diagnostics only — they do not
automatically alter the model.

## 12. §15 / §17 Outputs and run log

| Table / file | Contents |
|---|---|
| `schroders_company_dimension_evidence` | company x dimension metrics, tiers, shrunk scores |
| `schroders_sector_model_weights` | per bucket x dimension model weights + fit stats |
| `schroders_company_factor_scores` | factor raw/z/percentile, reliability, bootstrap stats |
| `schroders_overlap_diagnostics` | correlation/VIF flags |
| `pipeline_output/factor_run_log.json` | full audit log: versions, per-level company counts, per-bucket levels and alphas, exclusions with reasons, reliability distribution, overlap flags |

Dashboard API endpoints (Flask, `app.py`): `/api/v2/factor-scores`,
`/api/v2/evidence/<company>`, `/api/v2/model-weights`,
`/api/v2/overlap-diagnostics`, `/api/v2/culture-performance-groups`.

## 13. Operational notes

- **Determinism:** fixed RNG seed (7) for bootstraps; sorted iteration in
  bucket assignment; LOO-CV is deterministic. Re-running `model` on the
  same data reproduces identical outputs.
- **Deploy/rebuild:** after any code or dictionary change, production
  scores are refreshed by running `python pipeline/factor_build.py model`
  on a Heroku one-off dyno (~1 minute; it reads the existing evidence
  table). A full `all` run (including review re-scoring / termscan) is only
  needed when the dictionary or scoring engine changes.
- **Versioning:** every evidence row carries `dictionary_version` and
  `scoring_engine_version`; the run log snapshots all settings used.
