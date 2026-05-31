---
name: Combined culture-score formula conventions
description: Two intentionally-different "combined" score formulas across the dashboard's framework endpoints
---

The app computes a "combined" culture score in two different contexts, using two
different formulas **on purpose**:

- Company Analysis (`/api/company-analysis/<name>`) and Culture-vs-Performance
  scatter (`/api/culture-performance-scatter`): `combined = (hofstede * 5) + mit + (schroders * 5)`
  — Hofstede & Schroders raw values are -1..+1, MIT is 0..10, so ×5 brings them to
  comparable magnitudes for a balanced displayed score.
- Correlation endpoints (`/api/correlation-analysis`, `/api/correlation-matrix`):
  `combined = h_score + m_score + s_score` (raw, no ×5). Here the per-framework
  scores are correlation-weighted deviation sums (Σ correlation × (value − group_avg)),
  a different quantity used only as the x-axis of a regression, so absolute scale
  doesn't matter.

**Why:** This split predates the Schroders work — Hofstede+MIT already followed it.
When adding a framework, match the convention of the endpoint you're editing; do NOT
unify them without explicit sign-off, since that changes existing Hofstede/MIT output.

**How to apply:** Any new culture framework added to these endpoints must be folded
into combined following the local convention (scaled in company/scatter, raw in
correlation endpoints).

Related gotchas when adding a framework to the correlation endpoints:
- Companies lacking that framework's data score 0 → a whole GICS group can have
  identical x values → scipy `linregress` raises "all x values are identical".
  Guard with `if len(set(culture_scores)) < 2 or len(set(perf_scores)) < 2: continue`
  before the regression.
- Low-sample dimensions can yield NaN Pearson correlations; coerce NaN/None→0 when
  reading per-dimension correlations or the per-company score becomes NaN and renders
  as "NaN" in the UI.
