# Developer Specification — Fundamental Delivery, Valuation, and Weight-Consistency Analyses

**System:** Culture Analytics Dashboard (culturescoring.com), `pipeline/backtest.py` / `backtest_payloads.py` / `schroders_rolling_window_weights`
**Purpose:** The three-variant study showed the walk-forward (Variant W) Q1−Q4 price-return spread was +7.9pp pa in 2016–2020 but ~0 from 2021. These three analyses discriminate between the two candidate explanations: (a) the culture–performance relationship itself weakened ("signal decay / crowding"), versus (b) the relationship persists in *fundamentals* but stopped being expressed in *share prices* because market return dispersion post-2021 was dominated by themes orthogonal to culture ("regime / valuation compression"). If (b), top-quartile culture companies should show continued superior earnings delivery and progressively cheaper relative valuations — a positive forward-looking message.
**Requested by:** [User], 27 July 2026

**Common foundation for all three analyses:** the existing Variant-W backtest artefacts. Quartile memberships per formation quarter (Q1–Q4, 43 snapshots 2015-12-31 → 2026-06-30) must be **loaded from the stored Variant-W run and reused unchanged**. Do not re-rank, re-score, or re-estimate anything for analyses (i) and (ii) — they replace only the *outcome variable* measured on fixed memberships. This is essential: it isolates "what happened to these companies" from "how they were selected".

---

## Analysis (i) — Fundamental delivery: EPS-based quartile tracking

**Question:** Did Q1 (strong-culture) companies keep delivering better earnings growth than Q4 after 2021, even while their relative share-price performance stalled?

### Data

Per company and calendar quarter, from FMP (or existing fundamentals tables):

- **EPS (diluted, before extraordinary items), trailing twelve months (TTM)** — construct as the sum of the four most recent *reported* quarterly diluted EPS values as of each backtest snapshot date. Use the report/filing date, not the fiscal period end, to decide availability at a snapshot (point-in-time discipline: an EPS figure reported 2021-02-15 is not knowable at the 2020-12-31 snapshot).
- Fallbacks and hygiene: exclude company-quarters where TTM EPS is unavailable; for companies with only annual data, interpolate availability from annual filing dates and flag them; log per-snapshot coverage counts per quartile.

### Metrics per quartile per formation snapshot T

For each quartile portfolio formed at T (equal-weighted over members with valid data):

1. **Forward 12-month EPS growth:** median across members of `EPS_TTM(T+4q) / EPS_TTM(T) − 1`, computed only over members where both values are positive; separately report the share of members with negative or NULL EPS at T and the share whose EPS declined. Use the **median**, not the mean — EPS growth is heavy-tailed and a mean would be dominated by near-zero denominators. As a robustness line, also compute the aggregate (portfolio-sum) EPS growth: `Σ EPS(T+4q) / Σ EPS(T) − 1` over the same members.
2. **Forward 12-month revenue growth** (same construction) — EPS can be flattered by buybacks; revenue provides a cleaner volume check.
3. **Forward 12-month margin change:** median change in TTM operating margin.

### Outputs

1. **Time-series chart:** median forward EPS growth per quartile per snapshot (four lines), plus a Q1−Q4 EPS-growth spread line, 2016 → mid-2025 (the last snapshot with a complete forward year).
2. **Split-period table** (same 2020-12-31 split as the variant study): median forward EPS growth, revenue growth and margin change per quartile and the Q1−Q4 spread, for 2016–2020 and 2021–present.
3. **The decisive comparison:** a two-panel exhibit showing, for 2021–present, the Q1−Q4 spread in (a) price returns (already computed: −0.9pp pa) and (b) fundamental delivery (EPS growth spread). Interpretation key: fundamentals spread positive while price spread ~zero ⇒ supports the valuation-compression story; both ~zero ⇒ supports genuine signal decay.
4. CSV of the full per-snapshot per-quartile panel.

### Optional extension (only if time permits)

Re-run the **rolling-window weight estimation** (§1 of the previous spec) with the composite target's 25% TSR component replaced by 5-year EPS growth (weights: ROE 0.30, revenue growth 0.25, EPS growth 0.25, margin 0.20), then repeat Variant W. This tests whether removing price-based information from the *target* changes the fitted weights and the fade profile. Label it Variant W-F. It is secondary: analysis (i) on fixed memberships answers the main question without re-estimation.

---

## Analysis (ii) — Valuation: have strong-culture companies become cheaper?

**Question:** Track the relative valuation of Q1 vs Q4 through time. If Q1's superior fundamentals stopped being rewarded in prices post-2021, its relative valuation must have de-rated — quantifying that gives both the explanation for the price fade and a forward-looking "culture leaders are now cheap" exhibit.

### Data

Per company and snapshot: **trailing P/E** = quarter-end adjusted price (already in `backtest_quarter_prices`) ÷ point-in-time TTM diluted EPS (from analysis (i)); and **forward earnings yield proxy** E/P = TTM EPS ÷ price. Work primarily in **earnings-yield (E/P)** space rather than P/E: E/P is defined for negative earnings, roughly normally distributed, and averages sensibly. Report P/E medians for presentation but compute spreads on E/P.

### Metrics per quartile per snapshot

1. **Median trailing P/E** (members with positive EPS only) and **median E/P** (all members with EPS data; negative allowed).
2. **Relative valuation ratio:** median Q1 P/E ÷ median Q4 P/E, per snapshot (and equivalently the Q1−Q4 median E/P gap).
3. **Sector-neutral variant (important):** because quartiles are formed within peer buckets but pooled globally, sector mix can drive raw P/E gaps. Also compute, per company, its E/P minus its peer bucket's median E/P at that snapshot; report the median of this *excess* E/P per quartile. This is the cleaner series — trends in it cannot be explained by sector rotation.
4. **Return decomposition (ties (i) and (ii) together):** for each quartile and each period (2016–2020, 2021–present), decompose annualised price return ≈ fundamental growth (EPS CAGR) + re-rating (P/E CAGR), using portfolio medians. Present as a stacked-bar chart per quartile per period. This makes the story visible in one picture: if post-2021 Q1 shows positive EPS growth + negative re-rating while Q4 shows the reverse, the compression hypothesis is confirmed.

### Outputs

1. Time-series chart: median P/E per quartile (or Q1/Q4 relative ratio) with the sector-neutral excess-E/P series alongside, 2015 → present.
2. Split-period valuation table: start-of-period and end-of-period median P/E and sector-neutral excess E/P per quartile, for both sub-periods.
3. The return-decomposition stacked bars (2 periods × 4 quartiles: growth vs re-rating components).
4. **Current-state table:** as of the latest snapshot, Q1 vs Q4 median P/E, sector-neutral excess E/P, and the percentile of Q1's relative valuation versus its own 10-year history — the "how cheap are culture leaders now, versus their own past" number.
5. CSV of the full panel.

---

## Analysis (iii) — Weight consistency across eras

**Question:** Do the same culture dimensions carry the culture–performance relationship in different periods? If early windows reward one set of dimensions and late windows another, the fitted weighting is regime-specific — which explains why each variant performed best in its own estimation era, and tells us *which* aspects of culture mattered when.

### Inputs

`schroders_rolling_window_weights` — already built: per window (12 windows, ending 2015-12-31 → 2026-06-30) × bucket × dimension: standardised ridge coefficient, n_est, conf, CV-R². No new estimation required.

### Computations

Work with the **global-model coefficient vectors** primarily (12 windows × 12 dimensions); bucket-level vectors are too noisy individually but are used in step 4.

1. **Window-by-window coefficient matrix and heatmap:** 12×12 matrix (windows × dimensions) of global standardised coefficients, rendered as a signed heatmap with dimensions ordered by average absolute weight. Annotate each cell.
2. **Pairwise consistency matrix:** Pearson and Spearman correlations between every pair of window coefficient vectors (12×12 correlation matrix, heatmap). The key read-outs: average correlation between adjacent windows (should be high — windows share 4 of 5 years), and correlation between the average early vector (windows ending 2015–2019) and the average late vector (windows ending 2022–2026). Report both with n=12 caveat.
3. **Per-dimension trajectory chart:** each of the 12 dimensions' global coefficient across the 12 windows (small-multiples line grid). Flag dimensions that change sign between early and late eras, and rank dimensions by |early mean − late mean| change. Overlay each window's bootstrap coefficient stability SD (already computed per run) as an error band so genuine drift is distinguishable from estimation noise: a dimension has "materially changed" only if |early mean − late mean| > 2× pooled stability SD.
4. **Bucket-level generalisation check:** for the ~20 buckets estimable in both an early window (ending ≤2018) and a late window (ending ≥2023), compute the same early-vs-late coefficient correlation per bucket and plot the distribution. This tests whether era-instability is a global phenomenon or concentrated in particular industries.
5. **Era-swap portfolio test (the decisive economic check, cheap to run):** re-run the Variant-style backtest twice with **fixed** weight vectors: (a) the confidence-weighted average of *early* windows (ending 2015–2019) applied to *all* 43 snapshots, and (b) the average of *late* windows (ending 2022–2026) applied to all snapshots. Report each variant's Q1−Q4 spread in the 2016–2020 and 2021–present sub-periods (a 2×2 table). If early-weights work early but not late, and late-weights work late but not early, regime-specificity is confirmed economically, not just statistically. (Note: each cell where the estimation era overlaps the evaluation era is in-sample and should be shaded as such in the output — the informative cells are the off-diagonal ones.)

### Outputs

1. Coefficient heatmap (windows × dimensions), pairwise consistency matrix, per-dimension trajectory small-multiples with stability bands and a ranked "most changed dimensions" table.
2. Bucket-level early-vs-late correlation histogram.
3. The 2×2 era-swap spread table with in-sample cells shaded.
4. CSV exports of all coefficient matrices.

---

## Reporting and general requirements

Package results as before: a short PDF/HTML summary leading with (a) the two-panel price-vs-fundamentals comparison from (i), (b) the return decomposition and current-valuation table from (ii), and (c) the era-swap 2×2 and most-changed-dimensions table from (iii); plus all charts as PNG and all panels as CSV/JSON. State point-in-time conventions used for EPS availability, per-snapshot coverage counts per quartile (flag any quartile-snapshot with <60% EPS coverage), and the same survivorship caveat as the variant study. Keep everything equal-weighted, local-currency, using the stored Variant-W memberships; log and version the run alongside the previous package.

**Interpretation guide for the summary page.** Three broad outcomes: (1) fundamentals spread stays positive post-2021 and Q1 de-rates → valuation-compression story confirmed; the culture premium is latent, culture leaders are historically cheap, and the forward-looking case strengthens. (2) Fundamentals spread also collapses post-2021 → genuine weakening of the culture–performance link in the recent period; report should frame culture as risk/quality lens with a documented decay. (3) Mixed (e.g. EPS spread positive but shrinking, partial de-rating) → both forces at work; report both with magnitudes. Analysis (iii) qualifies any of these: strong era-instability in the weights means claims should emphasise the robust dimensions (those stable across eras) rather than the fitted composite.
