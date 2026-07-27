"""Precompute Backtest-tab dashboard payloads for every combination of
measure (tsr | earnings | pe) and weighting variant (C | A | W).

Reuses the PIT snapshot states + per-window models from
pipeline/backtest_variants.py (run `backtest_variants.py pit` first if
/tmp/bv_pit_states.pkl is missing) and the PIT TTM machinery from
pipeline/fund_val_weights.py (needs fmp_quarterly_income and
fmp_quarter_mcap already fetched).

Writes 9 payloads into backtest_payload_cache keyed
`bt-series:{measure}:{variant}` at the CURRENT backtest_data_version, so
the web dyno serves them read-only via /api/v2/backtest-series.

Usage:
    DATABASE_URL=... python3 pipeline/backtest_dashboard.py payloads
"""
import json
import os
import pickle
import sys
from collections import defaultdict

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
sys.path.insert(0, os.path.dirname(_HERE))

from backtest_variants import (                          # noqa: E402
    PKL, conn, load_windows_from_db, average_models, score_snapshot,
    portfolio_series, stats_block, DIMS,
)
from fund_val_weights import load_income, ttm_asof, tickmap, qret_map  # noqa: E402
from backtest_payloads import write_cached_payload      # noqa: E402

VARIANT_LABEL = {
    'C': 'Current weights',
    'A': 'Average weights',
    'W': 'Walkback weights',
}


def _next_quarter_label(last):
    from datetime import date
    m = last.month + 3
    y = last.year + (1 if m > 12 else 0)
    m = m - 12 if m > 12 else m
    d = 31 if m in (3, 12) else 30
    return date(y, m, d).isoformat()


def build_quarts(cur, states, bucket_of, snapshots):
    """Quartile memberships per snapshot for each weighting variant."""
    cur.execute("""
        SELECT peer_bucket, dimension, dimension_weight_final,
               predictor_mean, predictor_std
        FROM schroders_sector_model_weights""")
    prod_raw = defaultdict(dict)
    for b, d, w, mu, sd in cur.fetchall():
        prod_raw[b][d] = (w, mu, sd if sd else 1.0)
    prod_model = {}
    for b, dd in prod_raw.items():
        prod_model[b] = dict(
            coef=np.array([dd.get(d, (0.0, 0.0, 1.0))[0] for d in DIMS]),
            mu=np.array([dd.get(d, (0.0, 0.0, 1.0))[1] for d in DIMS]),
            sd=np.array([dd.get(d, (0.0, 0.0, 1.0))[2] for d in DIMS]))

    windows = load_windows_from_db(cur)
    buckets_all = sorted(set(bucket_of.values()))
    model_A = average_models(windows, buckets_all)
    wf_cache = {}

    def model_W(T):
        wes = tuple(sorted(we for we in windows if we <= T))
        if not wes:
            return None
        if wes not in wf_cache:
            wf_cache[wes] = average_models(windows, buckets_all, upto=wes[-1])
        return wf_cache[wes]

    quarts = {'C': {}, 'A': {}, 'W': {}}
    for s in snapshots:
        st = states[s]
        quarts['C'][s] = score_snapshot(st, bucket_of, prod_model)
        quarts['A'][s] = score_snapshot(st, bucket_of, model_A)
        mw = model_W(s)
        quarts['W'][s] = score_snapshot(st, bucket_of, mw) if mw else {}
    return quarts


def tsr_payload(quarts_v, tick_of, qret, used):
    per_q, bench, counts, _ = portfolio_series(quarts_v, tick_of, qret, used)
    years = len(used) / 4.0
    labels = [s.isoformat() for s in used] + [_next_quarter_label(used[-1])]
    series, stats = {}, []
    q_cum, q_ann = {}, {}

    def cumseries(rets):
        out, c0 = [100.0], 100.0
        for r in rets:
            c0 *= 1 + r
            out.append(c0)
        return out

    for q in (1, 2, 3, 4):
        series[f'Q{q}'] = cumseries(per_q[q])
        cum, ann = stats_block(per_q[q], years)
        q_cum[q], q_ann[q] = cum, ann
        stats.append(dict(quartile=f'Q{q}', cumulative_return_pct=cum,
                          annualized_return_pct=ann,
                          avg_companies=round(float(np.mean(counts[q])), 1)))
    stats.append(dict(quartile='Q1 − Q4 spread',
                      cumulative_return_pct=round(q_cum[1] - q_cum[4], 1),
                      annualized_return_pct=round(q_ann[1] - q_ann[4], 2),
                      avg_companies=None))
    cum_b, ann_b = stats_block(bench, years)
    series['Benchmark'] = cumseries(bench)
    stats.append(dict(quartile='All companies', cumulative_return_pct=cum_b,
                      annualized_return_pct=ann_b,
                      avg_companies=round(float(np.mean(
                          [sum(1 for comp, q in quarts_v[s].items()
                               if tick_of.get(comp) and
                               qret.get(tick_of[comp], {}).get(s) is not None)
                           for s in used])), 1)))
    return dict(labels=labels, series=series, stats=stats,
                y_title='Growth of 100 (share price, total of quarterly moves)',
                is_level=False)


def earnings_payload(quarts_v, tick_of, inc, snapshots, used):
    """Chained median growth of point-in-time trailing-12m net income."""
    nxt = {s: snapshots[i + 1] for i, s in enumerate(snapshots[:-1])}
    years = len(used) / 4.0
    labels = [s.isoformat() for s in used] + [_next_quarter_label(used[-1])]
    per_q = {q: [] for q in (1, 2, 3, 4)}
    bench, counts = [], {q: [] for q in (1, 2, 3, 4)}
    ttm_cache = {}

    def ttm_ni(t, s):
        key = (t, s)
        if key not in ttm_cache:
            tt = ttm_asof(inc.get(t, []), s) if t in inc else None
            ttm_cache[key] = tt['ni'] if tt and tt['ni'] else None
        return ttm_cache[key]

    for s in used:
        s2 = nxt.get(s)
        growth = {q: [] for q in (1, 2, 3, 4)}
        allg = []
        if s2 is not None:
            for comp, q in quarts_v[s].items():
                t = tick_of.get(comp)
                if not t:
                    continue
                n0, n1 = ttm_ni(t, s), ttm_ni(t, s2)
                if n0 and n1 and n0 > 0 and n1 > 0:
                    g = n1 / n0 - 1.0
                    growth[q].append(g)
                    allg.append(g)
        for q in (1, 2, 3, 4):
            gs = growth[q]
            per_q[q].append(float(np.median(gs)) if gs else 0.0)
            counts[q].append(len(gs))
        bench.append(float(np.median(allg)) if allg else 0.0)

    series, stats = {}, {}
    out_stats = []
    q_cum, q_ann = {}, {}

    def cumseries(rets):
        out, c0 = [100.0], 100.0
        for r in rets:
            c0 *= 1 + r
            out.append(c0)
        return out

    for q in (1, 2, 3, 4):
        series[f'Q{q}'] = cumseries(per_q[q])
        cum, ann = stats_block(per_q[q], years)
        q_cum[q], q_ann[q] = cum, ann
        out_stats.append(dict(quartile=f'Q{q}', cumulative_return_pct=cum,
                              annualized_return_pct=ann,
                              avg_companies=round(float(np.mean(counts[q])), 1)))
    out_stats.append(dict(quartile='Q1 − Q4 spread',
                          cumulative_return_pct=round(q_cum[1] - q_cum[4], 1),
                          annualized_return_pct=round(q_ann[1] - q_ann[4], 2),
                          avg_companies=None))
    cum_b, ann_b = stats_block(bench, years)
    series['Benchmark'] = cumseries(bench)
    out_stats.append(dict(quartile='All companies', cumulative_return_pct=cum_b,
                          annualized_return_pct=ann_b,
                          avg_companies=round(float(np.mean(
                              [sum(counts[q][i] for q in (1, 2, 3, 4))
                               for i in range(len(used))])), 1)))
    return dict(labels=labels, series=series, stats=out_stats,
                y_title='Growth of 100 (median trailing-12m earnings growth, chained)',
                is_level=False)


def pe_payload(quarts_v, tick_of, inc, mcap, used):
    """Median trailing P/E level per quartile (positive earners only)."""
    labels = [s.isoformat() for s in used]
    series = {f'Q{q}': [] for q in (1, 2, 3, 4)}
    series['Benchmark'] = []
    counts = {q: [] for q in (1, 2, 3, 4)}
    for s in used:
        pes = {q: [] for q in (1, 2, 3, 4)}
        allpe = []
        for comp, q in quarts_v[s].items():
            t = tick_of.get(comp)
            if not t:
                continue
            mc = mcap.get(t, {}).get(s)
            tt = ttm_asof(inc.get(t, []), s) if t in inc else None
            ni = tt['ni'] if tt else None
            if mc and ni and ni > 0:
                pe = mc / ni
                if 0 < pe < 200:
                    pes[q].append(pe)
                    allpe.append(pe)
        for q in (1, 2, 3, 4):
            v = pes[q]
            series[f'Q{q}'].append(round(float(np.median(v)), 2) if v else None)
            counts[q].append(len(v))
        series['Benchmark'].append(
            round(float(np.median(allpe)), 2) if allpe else None)

    stats = []
    bench_latest = series['Benchmark'][-1]
    for q in (1, 2, 3, 4):
        vals = [v for v in series[f'Q{q}'] if v is not None]
        latest = series[f'Q{q}'][-1]
        stats.append(dict(quartile=f'Q{q}', latest_pe=latest,
                          avg_pe=round(float(np.mean(vals)), 1) if vals else None,
                          avg_companies=round(float(np.mean(counts[q])), 1)))
    q1l = stats[0]['latest_pe']; q4l = stats[3]['latest_pe']
    stats.append(dict(quartile='Q1 − Q4 spread',
                      latest_pe=round(q1l - q4l, 1) if q1l and q4l else None,
                      avg_pe=None, avg_companies=None))
    allvals = [v for v in series['Benchmark'] if v is not None]
    stats.append(dict(quartile='All companies', latest_pe=bench_latest,
                      avg_pe=round(float(np.mean(allvals)), 1) if allvals else None,
                      avg_companies=round(float(np.mean(
                          [sum(counts[q][i] for q in (1, 2, 3, 4))
                           for i in range(len(used))])), 1)))
    return dict(labels=labels, series=series, stats=stats,
                y_title='Median trailing P/E (×, positive earners)',
                is_level=True)


def build_payloads():
    with open(PKL, 'rb') as f:
        ck = pickle.load(f)
    states, bucket_of, snapshots = ck['states'], ck['bucket_of'], ck['snapshots']
    c = conn(); cur = c.cursor()
    tick_of = tickmap(cur)
    qret = qret_map(cur)
    print('building quartile memberships for C/A/W...')
    quarts = build_quarts(cur, states, bucket_of, snapshots)

    # common start across all variants (>=3 priced members per quartile)
    def q_ok(v, s):
        cnt = defaultdict(int)
        for comp, q in quarts[v][s].items():
            t = tick_of.get(comp)
            if t and qret.get(t, {}).get(s) is not None:
                cnt[q] += 1
        return all(cnt[q] >= 3 for q in (1, 2, 3, 4))
    start_idx = next(i for i, s in enumerate(snapshots)
                     if all(q_ok(v, s) for v in ('C', 'A', 'W')))
    used = snapshots[start_idx:]
    print(f'common start {used[0]}, {len(used)} formation quarters')

    print('loading income + market caps...')
    inc = load_income(cur)
    cur.execute("SELECT ticker, quarter_end, mcap FROM fmp_quarter_mcap")
    mcap = defaultdict(dict)
    for tk, qe, mc in cur.fetchall():
        if mc and mc > 0:
            mcap[tk][qe] = mc

    cur.execute("SELECT value FROM app_config WHERE key = 'backtest_data_version'")
    row = cur.fetchone()
    version = row[0] if row else 'None'  # matches str(None) used by the reader
    print(f'writing payloads at backtest_data_version={version}')

    for v in ('C', 'A', 'W'):
        for measure, fn in (
                ('tsr', lambda: tsr_payload(quarts[v], tick_of, qret, used)),
                ('earnings', lambda: earnings_payload(
                    quarts[v], tick_of, inc, snapshots, used)),
                ('pe', lambda: pe_payload(quarts[v], tick_of, inc, mcap, used))):
            p = fn()
            p.update(success=True, available=True, measure=measure,
                     weights=v, weights_label=VARIANT_LABEL[v],
                     first_snapshot=used[0].isoformat(),
                     last_snapshot=used[-1].isoformat(),
                     n_snapshots=len(used))
            write_cached_payload(cur, f'bt-series:{measure}:{v}', version, p)
            c.commit()
            q1 = p['stats'][0]
            print(f'  {measure}:{v} written '
                  f'(Q1 {list(q1.values())[1]})')
    c.close()
    print('all 9 payloads written')


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'payloads'
    if cmd == 'payloads':
        build_payloads()
    else:
        print('usage: backtest_dashboard.py payloads')
