# Diagnostic Pack — Schroders v2 Framework (Mined vs Expert Dictionaries)

Corpus: 200,551 Glassdoor reviews, 52 companies. Both dictionaries were scored
with the identical engine (see `scoring_logic.md`), after the hyphen-handling
fix of 2026-07-21, so the comparison is apples-to-apples.

Dictionaries:
- `mined` — corpus-mined dictionary, 1,731 terms (version 2026-08-01-v2)
- `expert` — expert-seed dictionary as delivered, 2,242 terms (v2.1.0-expert-seeds)

## Files (mapped to the reviewer's request)

**1. Term-level** — `term_level_diagnostics.csv`
One row per term per dictionary (3,973 rows): dimension/pole, normalized form,
n-gram length, weight, match type, corpus match count, negated-match count,
document frequency (n and %), company frequency (n and %), sector frequency,
top-3 co-occurring dimensions, PMI vs own dimension, PMI vs best other
dimension, distinctiveness margin (PMI own − PMI best-other), and up to 3
example snippets per term.

**2. Dimension-level** — `dimension_level_summary.csv` (coverage, matches per
review distribution, % companies non-zero, score distribution, VIF),
`pairwise_correlations_{mined,expert}.csv` (company-level correlation
matrices), `pca_explained_variance_{mined,expert}.csv`,
`bootstrap_stability.csv` (200-rep company bootstrap of the pairwise
correlation matrix), `dimension_cooccurrence.csv` (review-level dimension ×
dimension co-occurrence with Jaccard %).

**3. Review- and company-level** — `review_level_scores.csv` (200,551 rows:
per-review scores + evidence mass for all 12 bipoles under BOTH dictionaries,
with company, sector, timestamp, employment status, current/former flag,
location) and `company_panel_side_by_side.csv` (company means under both
dictionaries with contributing review counts — same normalization).

**4. Scoring logic** — `scoring_logic.md` (preprocessing, tokenization,
hyphen handling, negation rules, matching, weighting, aggregation — exact).

**5. Duplicates** — `duplicate_terms.csv` (normalized terms appearing in more
than one pole within a dictionary).

**6. Top terms** — `top_terms_by_contribution.csv` (top 100 terms per pole by
weighted contribution = weight × total matches; cross-pole co-occurrence is
covered per-term in file 1 and per-dimension in `dimension_cooccurrence.csv`).

**7. Sector / time slices** — `sector_time_slice_correlations.csv`
(max and mean |pairwise r| by sector and by period: pre-2019, 2019-2022, 2023+).

## Notes for interpretation
- Sector granularity is limited in the current dev corpus: 52 companies map to
  "Financials" plus the unlisted "Asset Management/Other" group. The full
  2,442-company universe (11 GICS sectors) has not yet been extracted, so
  sector-slice power is thin.
- Company-level statistics (correlations, VIF, PCA, bootstrap) for the expert
  dictionary rest on far fewer complete companies (14) than the mined
  dictionary (49), because expert-dictionary coverage is so sparse that many
  companies have at least one dimension with zero mentions. That sparsity —
  visible directly in `dimension_level_summary.csv` (reviews_hit_pct: mined
  5–37% per dimension vs expert 0.1–1.2%) — is itself the headline finding.
- Scores are "mean of mentions": reviews where a dimension does not fire are
  excluded from that dimension's mean, not counted as zero.
