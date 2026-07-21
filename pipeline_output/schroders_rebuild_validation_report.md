# Schroders v2 Sector-Relative Rebuild — Validation Report
Date: 2026-07-21

## 1. Scope
Implements the 18-point developer brief: Schroders v2 (mined dictionary 2026-08-01-v2) is now the ONLY active
framework; Hofstede and MIT are retired from scoring, aggregation, dashboard and API outputs.
Rollback snapshot: `snapshots/pre_schroders_sector_relative_rebuild_2026_07_21/` (see ROLLBACK.md; ~10 min restore).

## 2. Corpus & scoring
- Reviews re-scored with mined dictionary: 200,551 (all reviews)
- Reviews with at least one v2 dimension scored: 51,674
- Engine: regex + negation, bipolar −1..+1 per dimension; expert dictionary archived at
  `pipeline_output/schroders_v2_keywords_expert_2026-07-21.py`.

## 3. Evidence tiers (company × dimension, 53 companies × 12 dims = 636 rows)
- Tier A (deep): 437   Tier B: 107   Tier C: 35   Tier D (thin): 57
- Review-concentration flags (top-5 reviews > 30% of dimension evidence): 143 rows

## 4. Sector-relative shrinkage
- Shrinkage k = 50 scored reviews per dimension (settings table `schroders_factor_settings`)
- Shrunk score = w·raw + (1−w)·sector mean, w = n/(n+k); sector = peer bucket below.

## 5. Peer buckets & classification
- Hierarchy tried: sub-industry → industry → sector; min 8 companies per bucket.
- Resolved level: gics_sector. Buckets: Financials (37 companies), Asset Management/Other (16).
- Wellington Mgmt. NULL sector coalesced into Asset Management/Other.

## 6. Ridge factor model (target = composite performance: ROE 5y 30%, revenue growth 5y 25%, TSR 5y 25%, op margin 5y 20%, z-clamped ±2)
- Asset Management/Other: level=global_fallback, n=23, alpha=30.0, in-sample R²=0.236, CV R²=-0.207
- Financials: level=gics_sector, n=19, alpha=30.0, in-sample R²=0.267, CV R²=-0.197
- Only 23 of 53 companies have performance data; Asset Management/Other bucket had n<8 with
  performance data, so it uses the global fallback model (documented per brief §10).
- Negative cross-validated R² means the model does NOT generalise out-of-sample at current n —
  factor weights should be treated as descriptive, not predictive.

## 7. Reliability tiers (companies)
- High: 12   Medium: 23   Low: 12   Insufficient: 6
- Bootstrap: 500 resamples; percentile SD and top/bottom-quintile frequencies stored per company.

## 8. Overlap / multicollinearity diagnostics (24 bucket × dimension gates)
- Flags: {"STRONG_WARNING": 5, "REVIEW": 3, "OK": 28}
- Governed review workflow only — no dimensions were auto-merged or deleted (brief §12).

## 9. Top-10 companies (High/Medium reliability only, sector percentile)
| Company | Peer group | %ile | Reliability | ±%ile SD |
|---|---|---|---|---|
| Goldman Sachs Group | Financials | 99 | Medium | 20.6 |
| Fidelity Investments | Asset Management/Other | 97 | Medium | 18.5 |
| AllianceBernstein | Asset Management/Other | 91 | Medium | 25.0 |
| Ameriprise Financial | Financials | 91 | High | 6.4 |
| Morgan Stanley Inv. Mgmt. | Financials | 88 | Medium | 19.6 |
| J.P. Morgan Chase | Financials | 85 | Medium | 16.0 |
| Apollo Global Management | Financials | 82 | Medium | 21.1 |
| Brookfield | Financials | 80 | Medium | 18.7 |
| UBS Group | Financials | 77 | High | 14.9 |
| Aflac | Financials | 72 | Medium | 19.9 |

## 10. Dashboard & API changes
- Active tabs: Overview, Companies, Company Analysis, Quarterly Trends, Schroders Framework,
  Culture Factor (new), Export Data, Data Status, Extraction Manager.
- Retired tabs (Hofstede, MIT, Performance Insights, Culture vs Performance, Correlation Analysis)
  are hidden; markup retained for rollback.
- Framework toggle locked to v2 (`/api/v2/framework-toggle` rejects anything else).
- New endpoints: `/api/v2/factor-scores`, `/api/v2/evidence/<company>`, `/api/v2/model-weights`,
  `/api/v2/overlap-diagnostics`.

## 11. Documented deviations from the brief
1. Term-concentration threshold set to 0.80 (brief suggested 0.30 for both reviews and terms):
   with a mined dictionary the top-5 terms naturally carry ~64% of matches on average, so 0.30
   would flag nearly every cell. Review-concentration threshold stays 0.30 per the brief.
2. Operating margin retained in the composite performance target ("current target unchanged").
3. Asset Management/Other uses global fallback model (insufficient companies with performance data).

## 12. Readiness recommendation
**Internal research use only.** Rationale: only 23/53 companies have performance data; cross-validated
R² is negative for both models; 18 of 53 companies are Low or Insufficient reliability. The evidence,
shrinkage and reliability plumbing is sound, but factor rankings should not be shown to clients until
performance coverage improves and CV R² turns positive.

## 13. Rollback
See `snapshots/pre_schroders_sector_relative_rebuild_2026_07_21/ROLLBACK.md` — restores git state,
7 database tables (incl. 321,066 review_culture_scores rows) and dictionaries in ~10 minutes.
