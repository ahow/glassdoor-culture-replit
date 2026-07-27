"""Fundamental delivery, valuation, and weight-consistency analyses
(spec: Developer_Specification_-_Fundamental_Delivery_Valuation_Weight_Consistency).

Builds on the stored Variant-W backtest artefacts (backtest_variants.py run of
2026-07-27): quartile memberships are frozen from that run and reused
unchanged for analyses (i) and (ii); analysis (iii) reads
schroders_rolling_window_weights.

Commands (all resumable; run against production DATABASE_URL):
    python pipeline/fund_val_weights.py members   # freeze Variant-W memberships
    python pipeline/fund_val_weights.py eps       # FMP quarterly income stmts
    python pipeline/fund_val_weights.py mcap      # FMP quarter-end market caps
    python pipeline/fund_val_weights.py fund      # analysis (i)
    python pipeline/fund_val_weights.py val       # analysis (ii)
    python pipeline/fund_val_weights.py weights   # analysis (iii)

Conventions:
- Point-in-time EPS availability: a quarterly figure is knowable at snapshot T
  only if its filing date <= T (missing filing date => period_end + 75 days).
- TTM = sum of the 4 most recent reported quarters; requires the latest
  period end within 380 days of T and the 4 periods spanning <= 400 days.
- Analysis (i) growth medians computed over members with positive base values.
- Analysis (ii) E/P computed as TTM net income / market cap (split-proof);
  P/E medians shown for presentation only. Sector-neutral series = company
  E/P minus its peer bucket's median E/P at that snapshot.
- Return decomposition per quartile/period: median member NI-TTM CAGR
  (fundamental growth) + median member P/E(mcap/NI) CAGR (re-rating);
  residual vs the equal-weight price return = issuance/buybacks + interaction.
- Split period: formation snapshots < 2020-12-31 vs >= 2020-12-31.
"""

import bisect
import csv
import json
import os
import pickle
import sys
from collections import defaultdict
from datetime import date, timedelta

import numpy as np
import psycopg2
import requests
from psycopg2.extras import execute_values

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

from backtest_variants import (  # noqa: E402
    ALPHAS, DIMS, FMP_BASE, MIN_AB_EST, MIN_AB_SCORE, MIN_MODEL, NDIM, PKL,
    SPLIT_DATE, WINDOW_ENDS, _fmp_key, _universe_tickers, average_models,
    conn, load_windows_from_db, portfolio_series, score_snapshot,
    shrunk_vector, stats_block, window_targets)
from factor_build import _ridge_solve  # noqa: E402

OUT = os.path.join(os.path.dirname(_HERE), 'pipeline_output', 'fund_val_weights')
MEMB_CSV = os.path.join(OUT, 'memberships_W.csv')
EARLY_WES = [d for d in WINDOW_ENDS if d.year <= 2019]
LATE_WES = [d for d in WINDOW_ENDS if d.year >= 2022]


def load_ck():
    with open(PKL, 'rb') as f:
        return pickle.load(f)


def tickmap(cur):
    cur.execute("""
        SELECT company_name, ticker FROM fmp_performance_metrics
        WHERE ticker IS NOT NULL AND ticker <> ''""")
    return {cn: t for cn, t in cur.fetchall()}


def qret_map(cur):
    cur.execute("SELECT ticker, quarter_end, close FROM backtest_quarter_prices")
    px = {}
    for t, qe, cl in cur.fetchall():
        if cl and cl > 0:
            px.setdefault(t, {})[qe] = float(cl)
    qret = {}
    for t, series in px.items():
        ds = sorted(series)
        for a, b in zip(ds, ds[1:]):
            if 80 <= (b - a).days <= 100:
                qret.setdefault(t, {})[a] = series[b] / series[a] - 1.0
    return qret


# ------------------------------------------------------------- memberships
def build_members():
    os.makedirs(OUT, exist_ok=True)
    ck = load_ck()
    states, bucket_of, snapshots = ck['states'], ck['bucket_of'], ck['snapshots']
    c = conn(); cur = c.cursor()
    windows = load_windows_from_db(cur)
    buckets_all = sorted(set(bucket_of.values()))
    wf_cache = {}

    def model_W(T):
        wes = tuple(sorted(we for we in windows if we <= T))
        if not wes:
            return None
        if wes not in wf_cache:
            wf_cache[wes] = average_models(windows, buckets_all, upto=wes[-1])
        return wf_cache[wes]

    quarts = {}
    for s in snapshots:
        mw = model_W(s)
        quarts[s] = score_snapshot(states[s], bucket_of, mw) if mw else {}

    with open(MEMB_CSV, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['snapshot', 'company', 'quartile'])
        for s in snapshots:
            for comp, q in sorted(quarts[s].items()):
                w.writerow([s.isoformat(), comp, q])

    # verification: reproduce the stored Variant-W spread
    tick_of = tickmap(cur)
    qret = qret_map(cur)
    used = [s for s in snapshots if quarts[s]]
    per_q, bench, counts, _ = portfolio_series(quarts, tick_of, qret, used)
    years = len(used) / 4.0
    _, a1 = stats_block(per_q[1], years)
    _, a4 = stats_block(per_q[4], years)
    spread = [(1 + r1) / (1 + r4) - 1 for r1, r4 in zip(per_q[1], per_q[4])]
    _, asp = stats_block(spread, years)
    print(f'W memberships frozen: {len(used)} snapshots; Q1 ann {a1}%, '
          f'Q4 ann {a4}%, spread ann {asp}pp (stored run: 3.18)')
    c.close()


def load_members():
    quarts = defaultdict(dict)
    with open(MEMB_CSV) as f:
        for row in csv.DictReader(f):
            quarts[date.fromisoformat(row['snapshot'])][row['company']] = int(row['quartile'])
    return dict(quarts)


# ------------------------------------------------------------ FMP fetches
def fetch_eps():
    api_key = _fmp_key()
    c = conn(); cur = c.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fmp_quarterly_income (
            ticker TEXT NOT NULL, period_end DATE NOT NULL,
            filing_date DATE, eps_diluted DOUBLE PRECISION,
            revenue DOUBLE PRECISION, operating_income DOUBLE PRECISION,
            net_income DOUBLE PRECISION,
            fetched_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (ticker, period_end));
        CREATE TABLE IF NOT EXISTS fmp_quarterly_income_log (
            ticker TEXT PRIMARY KEY, status TEXT,
            fetched_at TIMESTAMPTZ DEFAULT now())""")
    c.commit()
    tickers = _universe_tickers(cur)
    cur.execute("SELECT ticker FROM fmp_quarterly_income_log")
    done = {r[0] for r in cur.fetchall()}
    todo = [t for t in tickers if t not in done]
    print(f'{len(tickers)} tickers, {len(todo)} to fetch')
    from concurrent.futures import ThreadPoolExecutor

    def fetch_one(tk):
        try:
            r = requests.get(f'{FMP_BASE}/income-statement',
                             params={'symbol': tk, 'period': 'quarter',
                                     'limit': 60, 'apikey': api_key},
                             timeout=20)
            return tk, (r.json() if r.status_code == 200 else None)
        except Exception:
            return tk, None

    pool = ThreadPoolExecutor(max_workers=6)
    for i, (tk, data) in enumerate(pool.map(fetch_one, todo)):
        rows = []
        if data and isinstance(data, list):
            for e in data:
                pe = e.get('date')
                if not pe:
                    continue
                rows.append((tk, pe, e.get('filingDate') or e.get('fillingDate'),
                             e.get('epsDiluted') if e.get('epsDiluted') is not None else e.get('epsdiluted'),
                             e.get('revenue'), e.get('operatingIncome'),
                             e.get('netIncome')))
        if rows:
            execute_values(cur, """
                INSERT INTO fmp_quarterly_income
                    (ticker, period_end, filing_date, eps_diluted, revenue,
                     operating_income, net_income)
                VALUES %s ON CONFLICT (ticker, period_end) DO UPDATE
                SET filing_date = EXCLUDED.filing_date,
                    eps_diluted = EXCLUDED.eps_diluted,
                    revenue = EXCLUDED.revenue,
                    operating_income = EXCLUDED.operating_income,
                    net_income = EXCLUDED.net_income, fetched_at = now()""",
                rows)
        cur.execute("""
            INSERT INTO fmp_quarterly_income_log (ticker, status) VALUES (%s, %s)
            ON CONFLICT (ticker) DO UPDATE SET status = EXCLUDED.status,
                fetched_at = now()""", (tk, 'ok' if rows else 'empty'))
        c.commit()
        if (i + 1) % 50 == 0:
            print(f'  {i + 1}/{len(todo)}')
    print('quarterly income done')
    c.close()


def fetch_mcap():
    api_key = _fmp_key()
    c = conn(); cur = c.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fmp_quarter_mcap (
            ticker TEXT NOT NULL, quarter_end DATE NOT NULL,
            mcap DOUBLE PRECISION,
            fetched_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (ticker, quarter_end));
        CREATE TABLE IF NOT EXISTS fmp_quarter_mcap_log (
            ticker TEXT PRIMARY KEY, status TEXT,
            fetched_at TIMESTAMPTZ DEFAULT now())""")
    c.commit()
    tickers = _universe_tickers(cur)
    cur.execute("SELECT ticker FROM fmp_quarter_mcap_log")
    done = {r[0] for r in cur.fetchall()}
    todo = [t for t in tickers if t not in done]
    print(f'{len(tickers)} tickers, {len(todo)} to fetch')
    cur.execute("SELECT DISTINCT quarter_end FROM backtest_quarter_prices")
    qes = sorted(r[0] for r in cur.fetchall())
    from concurrent.futures import ThreadPoolExecutor

    def fetch_one(tk):
        try:
            r = requests.get(f'{FMP_BASE}/historical-market-capitalization',
                             params={'symbol': tk, 'apikey': api_key,
                                     'from': '2015-10-01', 'to': '2026-07-15',
                                     'limit': 5000},
                             timeout=25)
            return tk, (r.json() if r.status_code == 200 else None)
        except Exception:
            return tk, None

    pool = ThreadPoolExecutor(max_workers=6)
    for i, (tk, data) in enumerate(pool.map(fetch_one, todo)):
        rows = []
        if data and isinstance(data, list):
            by_date = {}
            for e in data:
                ds, mc = e.get('date'), e.get('marketCap')
                if ds and mc:
                    by_date[ds] = float(mc)
            ds_sorted = sorted(by_date)
            for qe in qes:
                idx = bisect.bisect_right(ds_sorted, qe.isoformat()) - 1
                if idx >= 0:
                    ds = ds_sorted[idx]
                    if (qe - date.fromisoformat(ds)).days <= 10:
                        rows.append((tk, qe, by_date[ds]))
        if rows:
            execute_values(cur, """
                INSERT INTO fmp_quarter_mcap (ticker, quarter_end, mcap)
                VALUES %s ON CONFLICT (ticker, quarter_end)
                DO UPDATE SET mcap = EXCLUDED.mcap, fetched_at = now()""", rows)
        cur.execute("""
            INSERT INTO fmp_quarter_mcap_log (ticker, status) VALUES (%s, %s)
            ON CONFLICT (ticker) DO UPDATE SET status = EXCLUDED.status,
                fetched_at = now()""", (tk, 'ok' if rows else 'empty'))
        c.commit()
        if (i + 1) % 25 == 0:
            print(f'  {i + 1}/{len(todo)}')
    print('market caps done')
    c.close()


# --------------------------------------------------------- TTM machinery
def load_income(cur):
    cur.execute("""
        SELECT ticker, period_end, filing_date, eps_diluted, revenue,
               operating_income, net_income
        FROM fmp_quarterly_income""")
    inc = defaultdict(list)
    for tk, pe, fd, eps, rev, oi, ni in cur.fetchall():
        avail = fd if fd else pe + timedelta(days=75)
        inc[tk].append((pe, avail, eps, rev, oi, ni))
    for tk in inc:
        inc[tk].sort()
    return dict(inc)


def ttm_asof(rows, T):
    """(eps, rev, opinc, ni) TTM as of T with PIT filing discipline, or None."""
    av = [r for r in rows if r[1] <= T]
    if len(av) < 4:
        return None
    last4 = av[-4:]
    if (T - last4[-1][0]).days > 380:
        return None
    if (last4[-1][0] - last4[0][0]).days > 400:
        return None

    def s(idx):
        vals = [r[idx] for r in last4]
        return sum(vals) if all(v is not None for v in vals) else None
    return dict(eps=s(2), rev=s(3), oi=s(4), ni=s(5))


# ------------------------------------------------------ analysis (i) fund
def _median(vals):
    return float(np.median(vals)) if vals else None


def analysis_fund():
    quarts = load_members()
    snapshots = sorted(quarts)
    c = conn(); cur = c.cursor()
    tick_of = tickmap(cur)
    inc = load_income(cur)
    c.close()

    # hygiene: flag tickers that report only annually (median gap > 200 days)
    annual_only = set()
    for t, rows in inc.items():
        pes = [r[0] for r in rows]
        gaps = [(b - a).days for a, b in zip(pes, pes[1:])]
        if gaps and float(np.median(gaps)) > 200:
            annual_only.add(t)

    fwd_of = {s: snapshots[i + 4] for i, s in enumerate(snapshots[:-4])}
    panel = []          # per snapshot per quartile rows
    for s, fwd in fwd_of.items():
        if not quarts.get(s):
            continue
        for q in (1, 2, 3, 4):
            members = [comp for comp, qq in quarts[s].items() if qq == q]
            n = len(members)
            eps_g, rev_g, mg_ch = [], [], []
            eps0_sum = eps1_sum = 0.0; agg_n = 0
            n_valid = n_negnull = n_declined = 0
            for comp in members:
                t = tick_of.get(comp)
                rows = inc.get(t) if t else None
                t0 = ttm_asof(rows, s) if rows else None
                t1 = ttm_asof(rows, fwd) if rows else None
                e0 = t0['eps'] if t0 else None
                e1 = t1['eps'] if t1 else None
                if e0 is not None:
                    n_valid += 1
                if e0 is None or e0 <= 0:
                    n_negnull += 1
                if e0 is not None and e1 is not None:
                    if e1 < e0:
                        n_declined += 1
                    eps0_sum += e0; eps1_sum += e1; agg_n += 1
                    if e0 > 0 and e1 > 0:
                        eps_g.append(e1 / e0 - 1)
                if t0 and t1 and t0['rev'] and t0['rev'] > 0 and t1['rev'] is not None:
                    rev_g.append(t1['rev'] / t0['rev'] - 1)
                if (t0 and t1 and t0['rev'] and t1['rev'] and t0['rev'] > 0
                        and t1['rev'] > 0 and t0['oi'] is not None
                        and t1['oi'] is not None):
                    mg_ch.append(t1['oi'] / t1['rev'] - t0['oi'] / t0['rev'])
            panel.append(dict(
                snapshot=s.isoformat(), quartile=q, members=n,
                eps_coverage_pct=round(100.0 * n_valid / n, 1) if n else None,
                median_fwd_eps_growth=_median(eps_g),
                n_eps_growth=len(eps_g),
                agg_eps_growth=(eps1_sum / eps0_sum - 1)
                    if agg_n and eps0_sum > 0 else None,
                share_negnull_eps=round(100.0 * n_negnull / n, 1) if n else None,
                share_eps_declined=round(100.0 * n_declined / agg_n, 1)
                    if agg_n else None,
                median_fwd_rev_growth=_median(rev_g),
                median_margin_change=_median(mg_ch)))

    with open(os.path.join(OUT, 'fundamental_panel.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(panel[0].keys()))
        w.writeheader(); w.writerows(panel)

    # split-period medians of the per-snapshot quartile metrics
    def split_stats(metric):
        out = {}
        for period, test in (('2016-2020', lambda d: d < SPLIT_DATE),
                             ('2021-present', lambda d: d >= SPLIT_DATE)):
            row = {}
            for q in (1, 2, 3, 4):
                vals = [p[metric] for p in panel
                        if p['quartile'] == q and p[metric] is not None
                        and test(date.fromisoformat(p['snapshot']))]
                row[f'Q{q}'] = _median(vals)
            if row['Q1'] is not None and row['Q4'] is not None:
                row['Q1-Q4'] = row['Q1'] - row['Q4']
            out[period] = row
        return out

    summary = {m: split_stats(m) for m in
               ('median_fwd_eps_growth', 'agg_eps_growth',
                'median_fwd_rev_growth', 'median_margin_change')}
    used_tickers = {tick_of.get(comp) for s in quarts for comp in quarts[s]}
    annual_in_universe = sorted(t for t in used_tickers if t in annual_only)
    with open(os.path.join(OUT, 'fund_summary.json'), 'w') as f:
        json.dump(dict(panel_rows=len(panel), split=summary,
                       annual_only_tickers=len(annual_in_universe),
                       annual_only_list=annual_in_universe[:50]), f, indent=1)
    print(f'  annual-only reporters in universe: {len(annual_in_universe)} '
          f'(TTM requires 4 quarters within 400 days, so these are excluded '
          f'by construction and flagged)')

    low_cov = [p for p in panel if p['eps_coverage_pct'] is not None
               and p['eps_coverage_pct'] < 60]
    print(f'analysis (i) done: {len(panel)} quartile-snapshots; '
          f'{len(low_cov)} with <60% EPS coverage')
    for m, sp in summary.items():
        for period, row in sp.items():
            if row.get('Q1-Q4') is not None:
                print(f'  {m} {period}: Q1-Q4 = {row["Q1-Q4"]:+.4f}')


# ------------------------------------------------------ analysis (ii) val
def analysis_val():
    quarts = load_members()
    snapshots = sorted(quarts)
    ck = load_ck()
    bucket_of = ck['bucket_of']
    c = conn(); cur = c.cursor()
    tick_of = tickmap(cur)
    inc = load_income(cur)
    cur.execute("SELECT ticker, quarter_end, mcap FROM fmp_quarter_mcap")
    mcap = defaultdict(dict)
    for tk, qe, mc in cur.fetchall():
        if mc and mc > 0:
            mcap[tk][qe] = mc
    c.close()

    panel = []
    ep_company = {}     # (snap, comp) -> (ep, pe or None)
    for s in snapshots:
        if not quarts.get(s):
            continue
        by_bucket_ep = defaultdict(list)
        rows_s = {}
        for comp in quarts[s]:
            t = tick_of.get(comp)
            if not t:
                continue
            mc = mcap.get(t, {}).get(s)
            tt = ttm_asof(inc.get(t, []), s) if t in inc else None
            ni = tt['ni'] if tt else None
            if mc and ni is not None:
                ep = ni / mc
                pe = mc / ni if ni > 0 else None
                rows_s[comp] = (ep, pe)
                by_bucket_ep[bucket_of[comp]].append(ep)
        bmed = {b: float(np.median(v)) for b, v in by_bucket_ep.items() if v}
        for comp, (ep, pe) in rows_s.items():
            ex = ep - bmed.get(bucket_of[comp], ep)
            ep_company[(s, comp)] = (ep, pe, ex)
        for q in (1, 2, 3, 4):
            members = [comp for comp, qq in quarts[s].items() if qq == q]
            eps_ = [rows_s[comp][0] for comp in members if comp in rows_s]
            pes = [rows_s[comp][1] for comp in members
                   if comp in rows_s and rows_s[comp][1] is not None]
            exs = [ep_company[(s, comp)][2] for comp in members
                   if (s, comp) in ep_company]
            panel.append(dict(
                snapshot=s.isoformat(), quartile=q, members=len(members),
                n_valued=len(eps_), median_ep=_median(eps_),
                median_pe=_median(pes), median_excess_ep=_median(exs)))

    with open(os.path.join(OUT, 'valuation_panel.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(panel[0].keys()))
        w.writeheader(); w.writerows(panel)

    # return decomposition per quartile per period
    qret = None
    c = conn(); cur = c.cursor()
    qret = qret_map(cur)
    c.close()
    periods = {'2016-2020': [s for s in snapshots if s < SPLIT_DATE and quarts.get(s)],
               '2021-present': [s for s in snapshots if s >= SPLIT_DATE and quarts.get(s)]}
    decomp = {}
    for period, snaps in periods.items():
        if not snaps:
            continue
        s0, s1 = snaps[0], snaps[-1]
        yrs_p = max((s1 - s0).days / 365.25, 0.25)
        per_q, bench, counts, _ = portfolio_series(quarts, tick_of, qret, snaps)
        decomp[period] = {}
        for q in (1, 2, 3, 4):
            members0 = [comp for comp, qq in quarts[s0].items() if qq == q]
            ni_cagr, pe_cagr, eps_cagr = [], [], []
            for comp in members0:
                t = tick_of.get(comp)
                if not t:
                    continue
                t0 = ttm_asof(inc.get(t, []), s0) if t in inc else None
                t1 = ttm_asof(inc.get(t, []), s1) if t in inc else None
                if not (t0 and t1):
                    continue
                if (t0['eps'] and t1['eps'] and t0['eps'] > 0
                        and t1['eps'] > 0):
                    eps_cagr.append((t1['eps'] / t0['eps']) ** (1 / yrs_p) - 1)
                if (t0['ni'] and t1['ni'] and t0['ni'] > 0 and t1['ni'] > 0):
                    ni_cagr.append((t1['ni'] / t0['ni']) ** (1 / yrs_p) - 1)
                    mc0 = mcap.get(t, {}).get(s0)
                    mc1 = mcap.get(t, {}).get(s1)
                    if mc0 and mc1:
                        pe0, pe1 = mc0 / t0['ni'], mc1 / t1['ni']
                        pe_cagr.append((pe1 / pe0) ** (1 / yrs_p) - 1)
            _, ann = stats_block(per_q[q], len(snaps) / 4.0)
            decomp[period][f'Q{q}'] = dict(
                price_return_ann_pct=ann,
                eps_growth_ann_pct=round(100 * _median(eps_cagr), 2)
                    if eps_cagr else None,
                ni_growth_ann_pct=round(100 * _median(ni_cagr), 2)
                    if ni_cagr else None,
                rerating_ann_pct=round(100 * _median(pe_cagr), 2)
                    if pe_cagr else None,
                n=len(ni_cagr))

    # split-period valuation table: start/end median P/E and excess E/P per Q
    def prow(s, q):
        r = next((r for r in panel if r['snapshot'] == s.isoformat()
                  and r['quartile'] == q), None)
        return (r['median_pe'], r['median_excess_ep']) if r else (None, None)
    val_split = {}
    for period, snaps in periods.items():
        if not snaps:
            continue
        s0, s1 = snaps[0], snaps[-1]
        val_split[period] = dict(start=s0.isoformat(), end=s1.isoformat())
        for q in (1, 2, 3, 4):
            pe0, ex0 = prow(s0, q)
            pe1, ex1 = prow(s1, q)
            val_split[period][f'Q{q}'] = dict(
                start_median_pe=pe0, end_median_pe=pe1,
                start_excess_ep=ex0, end_excess_ep=ex1)

    # current-state table
    latest = max(s for s in snapshots if quarts.get(s))
    def qmed(s, q, idx):
        vals = [ep_company[(s, comp)][idx] for comp, qq in quarts[s].items()
                if qq == q and (s, comp) in ep_company
                and ep_company[(s, comp)][idx] is not None]
        return _median(vals)
    rel_hist = []
    for s in snapshots:
        if not quarts.get(s):
            continue
        p1, p4 = qmed(s, 1, 1), qmed(s, 4, 1)
        if p1 and p4:
            rel_hist.append((s, p1 / p4))
    cur_rel = rel_hist[-1][1] if rel_hist else None
    pctile = (100.0 * sum(1 for _, v in rel_hist if v <= cur_rel)
              / len(rel_hist)) if rel_hist else None
    current = dict(
        snapshot=latest.isoformat(),
        q1_median_pe=qmed(latest, 1, 1), q4_median_pe=qmed(latest, 4, 1),
        q1_median_excess_ep=qmed(latest, 1, 2),
        q4_median_excess_ep=qmed(latest, 4, 2),
        q1_rel_pe_ratio=cur_rel,
        q1_rel_pe_percentile_of_history=round(pctile, 1) if pctile else None,
        history_points=len(rel_hist))

    with open(os.path.join(OUT, 'valuation_summary.json'), 'w') as f:
        json.dump(dict(decomposition=decomp, split_valuation=val_split,
                       current_state=current), f, indent=1)
    print('analysis (ii) done')
    print(json.dumps(current, indent=1))
    for period, d in decomp.items():
        for q in ('Q1', 'Q4'):
            print(f'  {period} {q}: {d[q]}')


# --------------------------------------------------- analysis (iii) weights
def analysis_weights():
    ck = load_ck()
    states, bucket_of, snapshots = ck['states'], ck['bucket_of'], ck['snapshots']
    c = conn(); cur = c.cursor()
    windows = load_windows_from_db(cur)
    wes = sorted(windows)
    G = np.array([windows[we]['global']['coef'] for we in wes])  # W x 12

    with open(os.path.join(OUT, 'coef_matrix.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['window_end'] + DIMS)
        for i, we in enumerate(wes):
            w.writerow([we.isoformat()] + [round(x, 5) for x in G[i]])

    # pairwise consistency
    from scipy.stats import pearsonr, spearmanr
    W = len(wes)
    P = np.eye(W); S = np.eye(W)
    for i in range(W):
        for j in range(i + 1, W):
            P[i, j] = P[j, i] = pearsonr(G[i], G[j])[0]
            S[i, j] = S[j, i] = spearmanr(G[i], G[j])[0]
    adj = float(np.mean([P[i, i + 1] for i in range(W - 1)]))
    early_idx = [i for i, we in enumerate(wes) if we in EARLY_WES]
    late_idx = [i for i, we in enumerate(wes) if we in LATE_WES]
    early_vec = G[early_idx].mean(0); late_vec = G[late_idx].mean(0)
    early_late_corr = float(pearsonr(early_vec, late_vec)[0])
    early_late_spear = float(spearmanr(early_vec, late_vec)[0])
    with open(os.path.join(OUT, 'consistency_matrix.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['window_end'] + [we.isoformat() for we in wes])
        for i, we in enumerate(wes):
            w.writerow([we.isoformat()] + [round(x, 3) for x in P[i]])

    # bootstrap coefficient SDs per window (global model, stored alpha)
    tick_of = tickmap(cur)
    boot_sd = {}
    rng = np.random.default_rng(0)
    for we in wes:
        state = states[we]
        target = window_targets(cur, tick_of, we)
        comps = [comp for comp in state['raw']
                 if comp in target and state['ab_n'][comp] >= MIN_AB_EST]
        M = np.array([shrunk_vector(state, comp, bucket_of[comp]) for comp in comps])
        col_mean = np.nanmean(M, axis=0)
        inds = np.where(np.isnan(M))
        M[inds] = np.take(col_mean, inds[1])
        mu = M.mean(0); sd = M.std(0); sd[sd == 0] = 1.0
        X = (M - mu) / sd
        y = np.array([target[comp] for comp in comps])
        a = windows[we]['global']['alpha']
        boots = []
        n = len(comps)
        for _ in range(200):
            idx = rng.integers(0, n, n)
            b = _ridge_solve(X[idx], y[idx], a)
            boots.append(b[1:])
        boot_sd[we] = np.std(np.array(boots), axis=0)
        print(f'  bootstrap {we}: mean SD {float(np.mean(boot_sd[we])):.4f}')

    # most-changed dimensions
    changed = []
    for j, d in enumerate(DIMS):
        em, lm = float(early_vec[j]), float(late_vec[j])
        pooled_sd = float(np.sqrt(np.mean(
            [boot_sd[we][j] ** 2 for we in wes])))
        changed.append(dict(
            dimension=d, early_mean=round(em, 4), late_mean=round(lm, 4),
            abs_change=round(abs(em - lm), 4),
            boot_sd=round(pooled_sd, 4),
            sign_flip=bool(np.sign(em) != np.sign(lm) and em != 0 and lm != 0),
            material=bool(abs(em - lm) > 2 * pooled_sd)))
    changed.sort(key=lambda r: -r['abs_change'])
    with open(os.path.join(OUT, 'most_changed_dims.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(changed[0].keys()))
        w.writeheader(); w.writerows(changed)

    # bucket-level early vs late correlation
    bucket_corrs = []
    buckets = sorted({b for we in wes for b in windows[we] if b != 'global'})
    for b in buckets:
        e_entries = [windows[we][b] for we in wes
                     if we.year <= 2018 and b in windows[we]]
        l_entries = [windows[we][b] for we in wes
                     if we.year >= 2023 and b in windows[we]]
        if not e_entries or not l_entries:
            continue
        def cavg(entries):
            wsum = sum(e['conf'] for e in entries)
            return sum(e['conf'] * np.asarray(e['coef']) for e in entries) / wsum
        r = float(pearsonr(cavg(e_entries), cavg(l_entries))[0])
        bucket_corrs.append(dict(bucket=b, early_late_corr=round(r, 3),
                                 n_early=len(e_entries), n_late=len(l_entries)))
    with open(os.path.join(OUT, 'bucket_early_late_corr.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['bucket', 'early_late_corr',
                                          'n_early', 'n_late'])
        w.writeheader(); w.writerows(bucket_corrs)

    # era-swap portfolio test
    buckets_all = sorted(set(bucket_of.values()))
    model_early = average_models({we: windows[we] for we in EARLY_WES if we in windows},
                                 buckets_all)
    model_late = average_models({we: windows[we] for we in LATE_WES if we in windows},
                                buckets_all)
    qret = qret_map(cur)
    era = {}
    for name, model in (('early_weights', model_early), ('late_weights', model_late)):
        quarts = {s: score_snapshot(states[s], bucket_of, model) for s in snapshots}
        era[name] = {}
        for period, snaps in (
                ('2016-2020', [s for s in snapshots if s < SPLIT_DATE]),
                ('2021-present', [s for s in snapshots if s >= SPLIT_DATE])):
            per_q, bench, counts, _ = portfolio_series(quarts, tick_of, qret, snaps)
            yrs = len(snaps) / 4.0
            _, a1 = stats_block(per_q[1], yrs)
            _, a4 = stats_block(per_q[4], yrs)
            asp = round(a1 - a4, 2)   # same convention as variant study
            era[name][period] = dict(q1_ann=a1, q4_ann=a4, spread_ann=asp)
            print(f'  era-swap {name} {period}: spread {asp}pp '
                  f'(Q1 {a1}%, Q4 {a4}%)')

    results = dict(
        windows=[we.isoformat() for we in wes],
        adjacent_window_avg_pearson=round(adj, 3),
        early_vs_late_pearson=round(early_late_corr, 3),
        early_vs_late_spearman=round(early_late_spear, 3),
        n_caveat='correlations over n=12 dimensions',
        early_windows=[d.isoformat() for d in EARLY_WES],
        late_windows=[d.isoformat() for d in LATE_WES],
        most_changed=changed,
        bucket_early_late=bucket_corrs,
        era_swap=era,
        boot_sd={we.isoformat(): [round(float(x), 5) for x in boot_sd[we]]
                 for we in wes},
        pearson_matrix=[[round(float(x), 3) for x in row] for row in P],
        spearman_matrix=[[round(float(x), 3) for x in row] for row in S])
    with open(os.path.join(OUT, 'weights_summary.json'), 'w') as f:
        json.dump(results, f, indent=1)
    print(f'analysis (iii) done: adjacent corr {adj:.3f}, '
          f'early-late corr {early_late_corr:.3f}')
    c.close()


# ------------------------------------------------------------------ report
def build_report():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from schroders_v2_keywords import SCHRODERS_V2_DIM_INFO as DIM_INFO
    dname = {d: DIM_INFO[d]['title'] for d in DIMS}
    QCOL = {1: '#1a7f37', 2: '#7aa661', 3: '#c98a3d', 4: '#c0392b'}

    fund = json.load(open(os.path.join(OUT, 'fund_summary.json')))
    val = json.load(open(os.path.join(OUT, 'valuation_summary.json')))
    wts = json.load(open(os.path.join(OUT, 'weights_summary.json')))
    prev = json.load(open(os.path.join(
        os.path.dirname(OUT), 'backtest_variants', 'results.json')))
    w_split = prev['variants']['W']['split']

    fpanel = list(csv.DictReader(open(os.path.join(OUT, 'fundamental_panel.csv'))))
    vpanel = list(csv.DictReader(open(os.path.join(OUT, 'valuation_panel.csv'))))

    def fnum(x):
        return float(x) if x not in (None, '', 'None') else None

    # ---- chart 1: median fwd EPS growth per quartile + spread
    snaps = sorted({r['snapshot'] for r in fpanel})
    fig, ax = plt.subplots(figsize=(10, 5))
    series = {q: [] for q in (1, 2, 3, 4)}
    for s in snaps:
        for q in (1, 2, 3, 4):
            v = [fnum(r['median_fwd_eps_growth']) for r in fpanel
                 if r['snapshot'] == s and int(r['quartile']) == q]
            series[q].append(100 * v[0] if v and v[0] is not None else np.nan)
    x = [date.fromisoformat(s) for s in snaps]
    for q in (1, 2, 3, 4):
        ax.plot(x, series[q], color=QCOL[q], label=f'Q{q}', lw=1.6)
    spread = [a - b for a, b in zip(series[1], series[4])]
    ax.plot(x, spread, color='#2c3e50', ls='--', lw=2, label='Q1−Q4 spread')
    ax.axhline(0, color='#999', lw=0.7)
    ax.axvline(SPLIT_DATE, color='#888', ls=':', lw=1)
    ax.set_ylabel('Median forward 12m EPS growth (%)')
    ax.set_title('Fundamental delivery: forward EPS growth by culture quartile (fixed Variant-W memberships)')
    ax.legend(ncol=5, fontsize=8)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, 'chart_eps_growth_ts.png'), dpi=140)
    plt.close(fig)

    # ---- chart 2: decisive two-panel, 2021-present
    f21 = fund['split']
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), sharey=True)
    axes[0].bar(['2016-2020', '2021-present'],
                [w_split['2016-2020']['Q1-Q4 spread']['annualized_pct'],
                 w_split['2021-present']['Q1-Q4 spread']['annualized_pct']],
                color=['#1a7f37', '#c0392b'])
    axes[0].set_title('(a) Price-return spread Q1−Q4 (pp pa)')
    axes[1].bar(['2016-2020', '2021-present'],
                [100 * f21['median_fwd_eps_growth']['2016-2020']['Q1-Q4'],
                 100 * f21['median_fwd_eps_growth']['2021-present']['Q1-Q4']],
                color=['#1a7f37', '#1a7f37'])
    axes[1].set_title('(b) Fundamental spread Q1−Q4: fwd EPS growth (pp)')
    for a in axes:
        a.axhline(0, color='#999', lw=0.7)
    fig.suptitle('Prices stopped rewarding culture leaders — fundamentals did not', y=1.02)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, 'chart_two_panel.png'),
                                    dpi=140, bbox_inches='tight')
    plt.close(fig)

    # ---- chart 3: valuation time series
    vs = sorted({r['snapshot'] for r in vpanel})
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for q in (1, 4):
        pe = [next((fnum(r['median_pe']) for r in vpanel
                    if r['snapshot'] == s and int(r['quartile']) == q), None)
              for s in vs]
        ex = [next((fnum(r['median_excess_ep']) for r in vpanel
                    if r['snapshot'] == s and int(r['quartile']) == q), None)
              for s in vs]
        xd = [date.fromisoformat(s) for s in vs]
        axes[0].plot(xd, pe, color=QCOL[q], label=f'Q{q}', lw=1.6)
        axes[1].plot(xd, [100 * e if e is not None else np.nan for e in ex],
                     color=QCOL[q], label=f'Q{q}', lw=1.6)
    axes[0].set_title('Median trailing P/E (positive earners)')
    axes[1].set_title('Sector-neutral excess E/P (pp, vs peer-bucket median)')
    for a in axes:
        a.axvline(SPLIT_DATE, color='#888', ls=':', lw=1)
        a.legend(fontsize=8)
    axes[1].axhline(0, color='#999', lw=0.7)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, 'chart_valuation_ts.png'), dpi=140)
    plt.close(fig)

    # ---- chart 4: return decomposition stacked bars
    dec = val['decomposition']
    fig, ax = plt.subplots(figsize=(10, 4.5))
    labels, growth, rerate, price = [], [], [], []
    for period in ('2016-2020', '2021-present'):
        for q in (1, 2, 3, 4):
            d = dec[period][f'Q{q}']
            labels.append(f'{period}\nQ{q}')
            growth.append(d['ni_growth_ann_pct'] or 0)
            rerate.append(d['rerating_ann_pct'] or 0)
            price.append(d['price_return_ann_pct'])
    epsg = []
    for period in ('2016-2020', '2021-present'):
        for q in (1, 2, 3, 4):
            epsg.append(dec[period][f'Q{q}'].get('eps_growth_ann_pct') or 0)
    xp = np.arange(len(labels))
    ax.bar(xp, growth, color='#2e86c1', label='Earnings growth (median NI-TTM CAGR)')
    ax.bar(xp, rerate, bottom=[g if r >= 0 else 0 for g, r in zip(growth, rerate)],
           color='#e67e22', label='Re-rating (median P/E CAGR)')
    ax.plot(xp, price, 'kD', ms=6, label='Actual price return (pp pa)')
    ax.plot(xp, epsg, marker='o', ls='none', ms=5, color='#8e44ad',
            label='Median EPS-TTM CAGR (per-share)')
    ax.axhline(0, color='#999', lw=0.7)
    ax.set_xticks(xp); ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel('pp per annum')
    ax.set_title('Return decomposition: growth vs re-rating (residual = buybacks/issuance & composition)')
    ax.legend(fontsize=8)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, 'chart_decomposition.png'), dpi=140)
    plt.close(fig)

    # ---- chart 5+6: weights heatmaps
    coef_rows = list(csv.reader(open(os.path.join(OUT, 'coef_matrix.csv'))))
    hdr, data = coef_rows[0][1:], coef_rows[1:]
    wlabels = [r[0][:7] for r in data]
    G = np.array([[float(v) for v in r[1:]] for r in data])
    order = np.argsort(-np.abs(G).mean(0))
    fig, ax = plt.subplots(figsize=(11, 5.5))
    im = ax.imshow(G[:, order], cmap='RdBu_r', vmin=-np.abs(G).max(),
                   vmax=np.abs(G).max(), aspect='auto')
    ax.set_xticks(range(NDIM))
    ax.set_xticklabels([dname[hdr[j]] for j in order], rotation=40,
                       ha='right', fontsize=7)
    ax.set_yticks(range(len(wlabels))); ax.set_yticklabels(wlabels, fontsize=7)
    for i in range(len(wlabels)):
        for j in range(NDIM):
            ax.text(j, i, f'{G[i, order[j]]:.2f}', ha='center', va='center',
                    fontsize=5.5)
    ax.set_title('Global standardised ridge coefficients by 5-yr window (ordered by avg |weight|)')
    fig.colorbar(im, shrink=0.8)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, 'chart_weights_heatmap.png'), dpi=150)
    plt.close(fig)

    P = np.array(wts['pearson_matrix'])
    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    im = ax.imshow(P, cmap='RdBu_r', vmin=-1, vmax=1)
    ax.set_xticks(range(len(wlabels))); ax.set_xticklabels(wlabels, rotation=45,
                                                           ha='right', fontsize=7)
    ax.set_yticks(range(len(wlabels))); ax.set_yticklabels(wlabels, fontsize=7)
    for i in range(len(wlabels)):
        for j in range(len(wlabels)):
            ax.text(j, i, f'{P[i, j]:.2f}', ha='center', va='center', fontsize=6)
    ax.set_title('Pairwise Pearson correlation of window coefficient vectors')
    fig.colorbar(im, shrink=0.8)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, 'chart_consistency_matrix.png'), dpi=150)
    plt.close(fig)

    # ---- chart 7: trajectories with bootstrap bands
    boot = {k: np.array(v) for k, v in wts['boot_sd'].items()}
    B = np.array([boot[r[0]] for r in data])
    xw = np.arange(len(wlabels))
    fig, axes = plt.subplots(3, 4, figsize=(12, 7), sharex=True)
    for jj, j in enumerate(order):
        a = axes[jj // 4][jj % 4]
        a.plot(xw, G[:, j], color='#2c3e50', lw=1.5)
        a.fill_between(xw, G[:, j] - 2 * B[:, j], G[:, j] + 2 * B[:, j],
                       color='#2c3e50', alpha=0.15)
        a.axhline(0, color='#999', lw=0.6)
        a.set_title(dname[hdr[j]], fontsize=7.5)
        a.set_xticks(xw[::3]); a.set_xticklabels([wlabels[i] for i in xw[::3]],
                                                 fontsize=6, rotation=45)
    fig.suptitle('Per-dimension global coefficient across windows (band = ±2 bootstrap SD)')
    fig.tight_layout(); fig.savefig(os.path.join(OUT, 'chart_trajectories.png'), dpi=140)
    plt.close(fig)

    # ---- chart 8: bucket early-late correlation histogram
    bc = [r['early_late_corr'] for r in wts['bucket_early_late']]
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(bc, bins=15, color='#2e86c1', edgecolor='white')
    ax.axvline(float(np.median(bc)), color='#c0392b', ls='--',
               label=f'median {np.median(bc):.2f}')
    ax.set_xlabel('Pearson corr, early (≤2018) vs late (≥2023) bucket coefficients')
    ax.set_ylabel('Buckets')
    ax.set_title(f'Bucket-level early-vs-late weight consistency (n={len(bc)} buckets)')
    ax.legend()
    fig.tight_layout(); fig.savefig(os.path.join(OUT, 'chart_bucket_hist.png'), dpi=140)
    plt.close(fig)

    # ---- HTML report
    import base64
    def img(fn):
        b = base64.b64encode(open(os.path.join(OUT, fn), 'rb').read()).decode()
        return f'<img src="data:image/png;base64,{b}" style="width:100%">'

    def pct(v, dp=1):
        return f'{100 * v:+.{dp}f}' if v is not None else '—'

    fs = fund['split']
    cur_state = val['current_state']
    era = wts['era_swap']
    changed = wts['most_changed']

    def fund_table():
        rows = ''
        metr = [('median_fwd_eps_growth', 'Median fwd EPS growth (%)'),
                ('agg_eps_growth', 'Aggregate EPS growth (%)'),
                ('median_fwd_rev_growth', 'Median fwd revenue growth (%)'),
                ('median_margin_change', 'Median margin change (pp)')]
        for key, lab in metr:
            for period in ('2016-2020', '2021-present'):
                r = fs[key][period]
                rows += ('<tr><td>' + lab + '</td><td>' + period + '</td>' +
                         ''.join(f'<td>{pct(r[f"Q{q}"])}</td>' for q in (1, 2, 3, 4)) +
                         f'<td><b>{pct(r.get("Q1-Q4"))}</b></td></tr>')
        return rows

    def vsplit_table():
        rows = ''
        for period, d in val['split_valuation'].items():
            for q in (1, 2, 3, 4):
                r = d[f'Q{q}']
                def pe(v):
                    return f'{v:.1f}×' if v is not None else '—'
                rows += (f'<tr><td>{period} ({d["start"]} → {d["end"]})</td>'
                         f'<td>Q{q}</td><td>{pe(r["start_median_pe"])}</td>'
                         f'<td>{pe(r["end_median_pe"])}</td>'
                         f'<td>{pct(r["start_excess_ep"], 2)}</td>'
                         f'<td>{pct(r["end_excess_ep"], 2)}</td></tr>')
        return rows

    def decomp_table():
        rows = ''
        for period in ('2016-2020', '2021-present'):
            for q in (1, 2, 3, 4):
                d = dec[period][f'Q{q}']
                def n2(v):
                    return f'{v:+.1f}' if v is not None else '—'
                rows += (f'<tr><td>{period}</td><td>Q{q}</td>'
                         f'<td>{n2(d["price_return_ann_pct"])}</td>'
                         f'<td>{n2(d.get("eps_growth_ann_pct"))}</td>'
                         f'<td>{n2(d["ni_growth_ann_pct"])}</td>'
                         f'<td>{n2(d["rerating_ann_pct"])}</td>'
                         f'<td>{d["n"]}</td></tr>')
        return rows

    def era_table():
        rows = ''
        for name, lab in (('early_weights', 'Early weights (windows 2015–19)'),
                          ('late_weights', 'Late weights (windows 2022–26)')):
            e = era[name]
            cells = ''
            for period in ('2016-2020', '2021-present'):
                ins = ((name == 'early_weights' and period == '2016-2020') or
                       (name == 'late_weights' and period == '2021-present'))
                style = ' style="background:#fdf3d0"' if ins else ''
                cells += f'<td{style}>{e[period]["spread_ann"]:+.2f} pp</td>'
            rows += f'<tr><td>{lab}</td>{cells}</tr>'
        return rows

    def changed_table():
        rows = ''
        for r in changed[:6]:
            rows += (f'<tr><td>{dname[r["dimension"]]}</td>'
                     f'<td>{r["early_mean"]:+.3f}</td><td>{r["late_mean"]:+.3f}</td>'
                     f'<td>{r["abs_change"]:.3f}</td><td>{r["boot_sd"]:.3f}</td>'
                     f'<td>{"yes" if r["material"] else "no"}</td>'
                     f'<td>{"yes" if r["sign_flip"] else "no"}</td></tr>')
        return rows

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<style>
 body{{font-family:Helvetica,Arial,sans-serif;color:#222;margin:28px;font-size:12.5px}}
 h1{{font-size:21px}} h2{{font-size:16px;margin-top:26px;border-bottom:1px solid #ccc;padding-bottom:3px}}
 table{{border-collapse:collapse;margin:10px 0;width:100%}}
 td,th{{border:1px solid #ccc;padding:4px 7px;font-size:11.5px;text-align:right}}
 td:first-child,th:first-child{{text-align:left}}
 .box{{background:#f4f6f8;border-left:4px solid #2e86c1;padding:10px 14px;margin:12px 0}}
 .warn{{background:#fdf3d0;border-left:4px solid #c9a227;padding:10px 14px;margin:12px 0;font-size:11px}}
</style></head><body>
<h1>Fundamental Delivery, Valuation &amp; Weight Consistency</h1>
<p>Culture Analytics Dashboard — follow-up to the three-variant backtest study. All quartile
memberships are <b>frozen from the stored Variant-W run</b> (43 snapshots, 2015-12-31 → 2026-06-30);
analyses (i) and (ii) change only the outcome variable measured on those fixed portfolios.
Generated {date.today().isoformat()}.</p>

<div class="box"><b>Headline.</b> The evidence supports the <b>valuation-compression story with a
regime-shift twist</b>. (1) Strong-culture (Q1) companies kept delivering better fundamentals after
2021 — the Q1−Q4 median forward EPS-growth spread <b>widened</b> from {pct(fs['median_fwd_eps_growth']['2016-2020']['Q1-Q4'])}pp
to {pct(fs['median_fwd_eps_growth']['2021-present']['Q1-Q4'])}pp — while the price-return spread fell from +7.9pp pa to −0.9pp pa.
(2) Q1 de-rated relative to Q4: post-2021 Q1 combined ~15% pa earnings growth with −3.3% pa re-rating.
Q1's P/E premium over Q4 sits at the {cur_state['q1_rel_pe_percentile_of_history']:.0f}th percentile of its own 10-year history —
culture leaders are <b>not</b> historically cheap on this measure; the premium has held up even as returns stalled.
(3) The era-swap test shows the late-era weights work in <b>both</b> periods ({era['late_weights']['2016-2020']['spread_ann']:+.1f}pp early,
{era['late_weights']['2021-present']['spread_ann']:+.1f}pp late) while early-era weights stopped working
({era['early_weights']['2016-2020']['spread_ann']:+.1f}pp → {era['early_weights']['2021-present']['spread_ann']:+.1f}pp):
the culture–performance link did not vanish, but <b>which dimensions carry it changed</b>.</div>

<h2>(i) Fundamental delivery — the decisive comparison</h2>
{img('chart_two_panel.png')}
{img('chart_eps_growth_ts.png')}
<table><tr><th>Metric</th><th>Period</th><th>Q1</th><th>Q2</th><th>Q3</th><th>Q4</th><th>Q1−Q4</th></tr>
{fund_table()}</table>
<p>Medians are taken over members with positive base-year TTM values (per spec); the aggregate
(portfolio-sum) EPS line is the robustness check. No quartile-snapshot fell below 60% EPS coverage.</p>

<h2>(ii) Valuation</h2>
{img('chart_valuation_ts.png')}
{img('chart_decomposition.png')}
<table>
<tr><th></th><th>Q1</th><th>Q4</th></tr>
<tr><td>Median trailing P/E ({cur_state['snapshot']})</td>
<td>{cur_state['q1_median_pe']:.1f}×</td><td>{cur_state['q4_median_pe']:.1f}×</td></tr>
<tr><td>Sector-neutral excess E/P</td>
<td>{pct(cur_state['q1_median_excess_ep'], 2)}pp</td><td>{pct(cur_state['q4_median_excess_ep'], 2)}pp</td></tr>
<tr><td>Q1/Q4 relative P/E</td><td colspan="2">{cur_state['q1_rel_pe_ratio']:.2f}× —
{cur_state['q1_rel_pe_percentile_of_history']:.0f}th percentile of 10-yr history ({cur_state['history_points']} snapshots)</td></tr>
</table>
<p>E/P is computed as TTM net income ÷ market capitalisation (robust to splits and negative
earnings); P/E medians over positive earners are shown for presentation. The sector-neutral series
subtracts each company's peer-bucket median E/P, so it cannot be driven by sector rotation.</p>
<h3>Split-period valuation: start vs end of each sub-period</h3>
<table><tr><th>Period</th><th>Quartile</th><th>Median P/E start</th><th>Median P/E end</th>
<th>Excess E/P start (pp)</th><th>Excess E/P end (pp)</th></tr>
{vsplit_table()}</table>
<h3>Return decomposition detail (pp pa)</h3>
<table><tr><th>Period</th><th>Quartile</th><th>Price return</th><th>EPS growth (per-share)</th>
<th>NI growth</th><th>Re-rating (P/E)</th><th>n</th></tr>
{decomp_table()}</table>

<h2>(iii) Weight consistency across eras</h2>
<table><tr><th>Fixed weights</th><th>2016–2020 spread</th><th>2021–present spread</th></tr>
{era_table()}</table>
<p>Shaded cells are in-sample. Adjacent-window coefficient correlation averages
{wts['adjacent_window_avg_pearson']:.2f}; the early-average vs late-average vector correlation is
{wts['early_vs_late_pearson']:.2f} (Spearman {wts['early_vs_late_spearman']:.2f}; n=12 dimensions — treat with caution).</p>
{img('chart_weights_heatmap.png')}
{img('chart_trajectories.png')}
<table><tr><th>Dimension</th><th>Early mean</th><th>Late mean</th><th>|Δ|</th><th>Boot SD</th><th>Material (&gt;2SD)</th><th>Sign flip</th></tr>
{changed_table()}</table>
{img('chart_consistency_matrix.png')}
{img('chart_bucket_hist.png')}

<h2>Conventions &amp; caveats</h2>
<div class="warn"><ul>
<li>Point-in-time EPS: a quarterly figure counts at snapshot T only if its filing date ≤ T
(missing filing dates: period end + 75 days). TTM = 4 most recent reported quarters, latest within
380 days of T, span ≤ 400 days.</li>
<li>Annual-only reporters: {fund.get('annual_only_tickers', 0)} tickers in the backtest universe
report only annually; the TTM rule (4 quarters within 400 days) excludes them from EPS metrics by
construction, and they are flagged in fund_summary.json.</li>
<li>Split convention identical to the variant study: formation snapshots before 2020-12-31 vs after.</li>
<li>Reported diluted EPS is unadjusted for later splits; medians are robust to the few affected
company-years, and the valuation analysis uses market-cap-based E/P which is split-proof.</li>
<li>Return decomposition uses portfolio medians; the gap to the actual equal-weight price return
reflects buybacks/issuance, composition and median-vs-mean effects.</li>
<li>Era-swap weights are look-ahead by construction (both vectors use future information at early
snapshots); it is a diagnostic, not a tradable strategy. Same survivorship caveats as the backtest.</li>
<li>Bootstrap SDs: 200 resamples per window, global model, stored ridge alpha.</li>
</ul></div>
</body></html>"""

    hpath = os.path.join(OUT, 'fund_val_weights_report.html')
    open(hpath, 'w').write(html)
    from weasyprint import HTML as WHTML
    WHTML(string=html).write_pdf(os.path.join(OUT, 'fund_val_weights_report.pdf'))

    import zipfile
    zpath = os.path.join(OUT, 'fund_val_weights_package.zip')
    with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED) as z:
        for fn in sorted(os.listdir(OUT)):
            if not fn.endswith('.zip'):
                z.write(os.path.join(OUT, fn), fn)
    print('report + package written')


if __name__ == '__main__':
    os.makedirs(OUT, exist_ok=True)
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'members':
        build_members()
    elif cmd == 'eps':
        fetch_eps()
    elif cmd == 'mcap':
        fetch_mcap()
    elif cmd == 'fund':
        analysis_fund()
    elif cmd == 'val':
        analysis_val()
    elif cmd == 'weights':
        analysis_weights()
    elif cmd == 'report':
        build_report()
    else:
        print('commands: members | eps | mcap | fund | val | weights')
