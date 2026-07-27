---
name: Backtest weight-estimation variants
description: How the C/A/W weight-variant backtest study works, its results, and how to re-run it
---

- `pipeline/backtest_variants.py` compares weight regimes over the same PIT quartile backtest: C=today's prod weights, A=conf-weighted average of all rolling 5-yr window ridge fits, W=walk-forward (windows ending ≤ T only). Report-only study — no dashboard/app.py changes.
- Stages are resumable: `fundamentals` / `earlyprices` (FMP fetch → prod tables `fmp_annual_fundamentals`, `backtest_variants_early_prices`), then `pit` (PIT states → /tmp pickle, input queries checkpointed per-dim in /tmp/bv_pit_inputs), `windows` (per-window ridge → `schroders_rolling_window_weights`, skips done windows), `variants` (scoring + outputs). `run` chains pit→windows→variants.
- **Why staged:** agent-env background processes get reaped and bash calls cap ~110s; each stage fits in foreground chunks and resumes on rerun.
- Result (2026-07-27, 43 quarters from 2015-12-31): Q1−Q4 spread pa — C 6.69pp (−1.0 pre-2021 / +13.4 post), A 9.83pp (11.1/8.7), W 3.18pp (+7.9 pre-2021 / **−0.9 post-2021**). Walk-forward signal decayed as universe grew (window global CV-R² fell 0.21→0.04); much of C's post-2021 headline is look-ahead in the weights. Variant A retains residual look-ahead by construction.
- Outputs: `pipeline_output/backtest_variants/` (results.json, 4 CSVs, 3 PNGs). matplotlib is pipeline-only (pipeline/requirements.txt, NOT root — keep out of Heroku slug).
