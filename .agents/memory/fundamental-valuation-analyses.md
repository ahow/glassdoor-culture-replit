---
name: Fundamental/valuation analyses on backtest quartiles
description: Conventions and findings for EPS/valuation follow-up studies on frozen backtest memberships
---
- Freeze quartile memberships to a CSV before any outcome-variable study; rebuild from the PIT states pickle + stored window weights reproduces the run exactly (verify Q1/Q4 annualised returns match, not the "spread" number — the study's spread convention is arithmetic Q1−Q4 of annualised returns, not compounded).
- **PIT TTM convention:** a quarterly figure is knowable only if filing date ≤ snapshot (missing filing date → period end + 75 days); TTM = 4 latest quarters, newest within 380 days, span ≤ 400 days.
- **Valuation must use E/P = TTM net income / market cap**, not price/EPS: FMP adjusted closes are split/dividend adjusted while reported EPS is not, so P/E from those two is wrong for split companies. Fetch fmp_quarter_mcap (historical-market-capitalization endpoint) instead.
- **Why:** avoids silent split distortion; E/P also handles negative earners and averages sensibly.
- Findings (2026-07): fundamentals spread Q1−Q4 widened post-2021 (+1.1pp→+3.4pp fwd EPS growth) while price spread fell +7.9→−0.9pp pa; Q1 de-rated (−3.3%pa) but still trades at 77th-percentile relative P/E; era-swap shows late-era weights work in both periods — the culture signal rotated dimensions rather than dying.
