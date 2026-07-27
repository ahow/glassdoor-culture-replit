# Developer Specification — Backtest Weight-Estimation Variants

**System:** Culture Analytics Dashboard (culturescoring.com), `pipeline/backtest.py` / `factor_build.py`
**Purpose:** Compare three ways of setting the ridge model weights used to rank companies in the quarterly quartile backtest, to quantify how much of the current Q1−Q4 spread depends on weights that were not knowable at the time of each rebalancing.
**Requested by:** [User], 27 July 2026

---

## 0. Background and goal

The current backtest (§13 of the methodology) uses **point-in-time culture evidence** (reviews dated on or before each quarterly snapshot) but **today's production model weights**, fitted once on a composite performance target measured over the trailing five years as of today (~2021–2026, including 25% five-year TSR). Splitting the published series at end-2020 shows the Q1−Q4 spread is −1.0pp pa in 2016–2020 and +14.1pp pa in 2021–2026, which suggests the headline spread partly reflects weights "knowing" the evaluation-period returns.

We want to run the backtest three ways and compare:

- **Variant C (Current):** today's production weights applied at all snapshots — the existing implementation, unchanged, as the baseline.
- **Variant A (Averaged weights):** a single fixed weight vector per peer bucket, formed by averaging the weights estimated over *every* rolling 5-year window available in the data, confidence-weighted by review volume. Same weights applied at all snapshots.
- **Variant W (Walk-forward):** at each snapshot T, weights formed by averaging only the rolling-window estimates from windows that **ended on or before T**. Weights therefore evolve through the backtest and never use information unavailable at T.

Everything else — point-in-time evidence, tiers, shrinkage, bucket assignment, eligibility, quartile formation, equal weighting, quarterly rebalancing, price data — is **identical across the three variants** so that any difference in results is attributable to the weights alone.

---

## 1. Common building block: per-window weight estimation

Both Variant A and Variant W require ridge weights estimated separately for each rolling 5-year window. Define:

**Windows.** Let window `k` be the 5-year period ending at calendar year-end `E_k`, for every `E_k` from the earliest feasible year to the present (e.g. windows ending 2015-12-31, 2016-12-31, …, 2025-12-31, plus optionally the latest half-year 2026-06-30). Quarterly-ending windows are acceptable if financial data supports them; annual is sufficient and cheaper.

**Per-window performance target.** For window `k`, rebuild the composite target exactly as `perf_targets()` does today, but with all four metrics measured **inside window k only**: 5-yr average ROE, 5-yr revenue growth, 5-yr TSR, 5-yr average operating margin, each z-scored across companies *within window k*, clamped to [−2, +2], combined 0.30/0.25/0.25/0.20 with renormalisation over available metrics. This requires per-company financials for each window (e.g. FMP historical fundamentals and adjusted prices). Where a company lacks data for a window (not yet listed, missing statements), it is simply excluded from that window's estimation set — do **not** impute.

**Per-window culture inputs.** Use the **point-in-time** shrunk-internal dimension scores as of `E_k` (the same quarterly point-in-time evidence machinery the backtest already builds): reviews dated ≤ `E_k`, tiers per §6, shrinkage w = n/(n+50) toward the point-in-time bucket prior. This matters: pairing window-k financials with today's culture scores would reintroduce look-ahead on the culture side.

**Per-window estimation.** For each window `k`, run the existing §9 model stage unchanged in structure: estimation set = companies with a window-k target AND ≥ 6 Tier A/B dimensions as of `E_k`; standardise the 12 shrunk-internal columns on the estimation set's own mu/sd; fit the global ridge, then per-bucket ridges for buckets with ≥ 8 eligible companies, shrinking bucket coefficients toward that window's global coefficients with lambda = n/(n+10); LOO-CV alpha selection over the existing grid. **Buckets:** to keep the comparison clean, freeze the bucket assignment to today's production buckets (77 named + global) for all windows and all variants, rather than re-running `pick_bucket` per window. Record per window and bucket: coefficient vector, mu/sd, alpha, n_est, in-sample R², CV-R².

**Per-window confidence weight.** For window `k` and bucket `b`, define the confidence weight as the total number of scored reviews underlying that bucket's estimation set as of `E_k`:
`conf(k, b) = Σ over companies in estimation set of n_scored_reviews as of E_k` (sum across the 12 dimensions, or equivalently the company total of dimension-scored reviews — pick one and log it). If a bucket has < 8 eligible companies in window k, no bucket model is estimated for that window; the window contributes only to the global average for that bucket (see §2–3).

**Storage.** Persist everything to a new table `schroders_rolling_window_weights` (window_end, bucket, dimension, coef, mu, sd, alpha, n_est, r2, cv_r2, conf) so both variants and any future diagnostics read from one artefact.

---

## 2. Variant A — confidence-weighted average of all windows

For each bucket `b` and dimension `j`, the fixed weight is the confidence-weighted mean over **all** windows:

```
coef_A(b, j) = Σ_k conf(k, b) · coef(k, b, j) / Σ_k conf(k, b)
```

Averaging must be done on coefficients expressed in a **common scale**. Because each window standardises predictors on its own mu/sd, average the *standardised* coefficients and also compute the confidence-weighted average mu and sd per dimension; at scoring time, standardise each snapshot's shrunk-internal scores using those averaged mu/sd, then apply `coef_A`. (Alternative: convert each window's coefficients to raw-scale before averaging; either is fine, but do one consistently and log the choice.)

Where a bucket has bucket-specific models in some windows only, average over the windows that have them; if a bucket never reaches 8 eligible companies in any window, use the confidence-weighted average of the **global** coefficients. Do not mix bucket and global coefficients within one average — the existing per-window lambda-shrinkage toward global already handles small-sample buckets inside each window.

Then run the backtest exactly as today, at every snapshot ranking companies by `factor_raw = coef_A · x` within their bucket, forming quartiles, compounding equal-weighted quarterly returns.

**Note for interpretation (please surface this in the output):** Variant A is *not* free of look-ahead — every evaluation quarter lies inside at least one estimation window that fed the average. It is included as a robustness comparison, not as the honest benchmark.

## 3. Variant W — walk-forward (windows ending on or before T only)

At each backtest snapshot `T`, compute the weights as in §2 **but restricting the average to windows with `E_k ≤ T`**:

```
coef_W(b, j, T) = Σ_{k: E_k ≤ T} conf(k, b) · coef(k, b, j) / Σ_{k: E_k ≤ T} conf(k, b)
```

Same averaging conventions (standardised coefficients + averaged mu/sd) and same fallback rules as Variant A, evaluated per T. The weight vector therefore updates once a year (or per quarter if quarterly windows are built) and uses strictly pre-T information: this is the honest, walk-forward variant.

**Start date.** Require at least one completed window before scoring: the first usable snapshot is the first quarter-end ≥ the earliest `E_k`. If the earliest feasible window ends 2015-12-31 the backtest starts as today (2015-12-31), but note the first 1–2 years rest on a single window; optionally require ≥ 2 windows (start 2016-12-31) and report both.

**Efficiency.** Weights only change when a new window completes, so cache `coef_W` per (bucket, E_k) and reuse across the intervening quarters. The heavy cost is §1 (per-window estimation), shared with Variant A.

## 4. What to hold identical across all three variants

Point-in-time evidence and shrinkage; bucket assignment (today's production buckets); eligibility rule (≥ 4 Tier A/B dimensions at snapshot); quartile formation (percentile within bucket, pooled to global quartiles as today); equal weighting; quarterly rebalancing; the price series (`backtest_quarter_prices`); the benchmark (all eligible companies); no transaction costs; local-currency price-only returns. Any deviation makes the three-way comparison uninterpretable.

## 5. Required outputs

Produce one payload (JSON and CSV) and charts covering all three variants:

1. **Cumulative return chart** (indexed to 100 at the common start): Q1, Q4 and benchmark for each variant — either a 3-panel chart or two overlay charts (one for Q1s, one for Q1−Q4 spreads). Include per-quarter membership counts in tooltips as now.
2. **Headline statistics table**, per variant: Q1–Q4 annualised and cumulative returns, benchmark, Q1−Q4 spread (annualised and cumulative), average companies per quartile.
3. **Split-period table**, per variant: the same statistics computed separately for 2016–2020 and 2021–present (split at 2020-12-31). This is the decisive diagnostic: Variant C is expected to show the current −1.0pp / +14.1pp asymmetry; the question is what Variants A and W show in each sub-period.
4. **Rolling 3-year Q1−Q4 spread** (annualised, per quarter-end) for each variant on one chart — shows when and how persistently each variant discriminates.
5. **Weight-stability diagnostics** (Variant W): per bucket (or at least global), a heatmap or line chart of the 12 walk-forward coefficients over time, plus the year-over-year mean absolute coefficient change. This tests the expectation that weights stabilise as windows accumulate.
6. **Per-window estimation log**: table of window_end × bucket with n_est, alpha, R², CV-R², conf — so we can see which windows/buckets are thin.
7. **Turnover**, per variant: average quarterly one-way name turnover of the Q1 portfolio (needed later for any capacity/cost discussion, and to check Variant W's changing weights don't cause excessive churn).

Please also log any windows where financial-data coverage is materially thinner (e.g. pre-2018 fundamentals coverage in FMP), and flag if per-window estimation sets fall below ~60% of the current estimation set size, since that would qualify the early-window results.

## 6. Acceptance criteria and expected interpretation

The deliverable is the comparison, not a particular result. But for orientation: if Variant W shows a positive, reasonably stable Q1−Q4 spread in both sub-periods (even if smaller than Variant C's headline), that supports a real, investable culture signal and becomes the citable "achievable performance" exhibit. If Variant W's spread is ~zero throughout while Variant C's is concentrated post-2021, the current headline is substantially a look-ahead artefact and only the measurement (not the fitted weighting) should be claimed. Variant A is expected to fall between the two; if it is materially higher than W, the difference itself quantifies the look-ahead embedded in averaging across future windows.

## 7. Estimated scope

The main new work is §1 (per-window targets require historical fundamentals and prices per window — check FMP coverage first; everything else reuses existing pipeline stages) and the payload/chart assembly in §5. Suggest implementing §1 with the same resumable/cached pattern as `termscan`, since ~11 windows × 78 buckets × ridge fits are cheap, but the per-window point-in-time evidence joins are the slow part and are already built quarterly by the current backtest.
