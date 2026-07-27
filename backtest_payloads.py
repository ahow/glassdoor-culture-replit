"""Shared computation of the heavy backtest dashboard payloads.

Used by both the Flask endpoints in app.py (on cache miss) and by
pipeline/backtest.py, which precomputes the payloads into the
backtest_payload_cache table so the web dyno only ever reads a
ready-made result after a pipeline run or deploy.
"""

import json
from datetime import datetime


def _quarterly_returns(px):
    """ticker -> {quarter_end: return over next quarter} from price series."""
    qret = {}
    for t, series in px.items():
        ds = sorted(series)
        for a, b in zip(ds, ds[1:]):
            # consecutive calendar quarters only (~90 days apart)
            if 80 <= (b - a).days <= 100:
                qret.setdefault(t, {})[a] = series[b] / series[a] - 1.0
    return qret


def _load_common(cur):
    cur.execute("""
        SELECT company_name, ticker FROM fmp_performance_metrics
        WHERE ticker IS NOT NULL AND ticker <> ''""")
    tick = {c: t for c, t in cur.fetchall()}
    cur.execute("SELECT ticker, quarter_end, close FROM backtest_quarter_prices")
    px = {}
    for t, qe, cl in cur.fetchall():
        if cl and cl > 0:
            px.setdefault(t, {})[qe] = float(cl)
    return tick, px


def compute_backtest_payload(cur):
    """Payload for /api/v2/backtest (always global)."""
    cur.execute("""
        SELECT snapshot_date, company_name, quartile
        FROM schroders_backtest_scores""")
    score_rows = cur.fetchall()
    if not score_rows:
        return {'success': True, 'available': False,
                'message': 'Backtest has not been run yet.'}

    tick, px = _load_common(cur)
    qret = _quarterly_returns(px)

    snaps = sorted({r[0] for r in score_rows})
    holdings = {s: {1: [], 2: [], 3: [], 4: []} for s in snaps}
    bench = {s: [] for s in snaps}
    for snap, comp, quart in score_rows:
        t = tick.get(comp)
        r = qret.get(t, {}).get(snap) if t else None
        if r is None:
            continue
        holdings[snap][quart].append(r)
        bench[snap].append(r)

    min_members = 3
    start_idx = None
    for i, s in enumerate(snaps):
        if all(len(holdings[s][q]) >= min_members for q in (1, 2, 3, 4)):
            start_idx = i
            break
    if start_idx is None:
        return {'success': True, 'available': False,
                'message': 'Not enough companies with price data '
                           'for this filter to build quartile portfolios.'}

    used = [s for s in snaps[start_idx:]
            if any(holdings[s][q] for q in (1, 2, 3, 4))]
    labels = [s.isoformat() for s in used]
    series = {}
    stats = []
    counts = {}
    q_cum = {}
    q_ann = {}
    for q in (1, 2, 3, 4):
        cum, vals, ns = 100.0, [100.0], []
        for s in used:
            rets = holdings[s][q]
            r = (sum(rets) / len(rets)) if rets else 0.0
            cum *= (1.0 + r)
            vals.append(cum)
            ns.append(len(rets))
        series[f'Q{q}'] = vals
        counts[f'Q{q}'] = ns
        years = len(used) / 4.0
        ann = ((cum / 100.0) ** (1.0 / years) - 1.0) * 100 if years > 0 else None
        q_cum[q] = cum - 100.0
        q_ann[q] = ann
        stats.append({'quartile': f'Q{q}',
                      'cumulative_return_pct': round(cum - 100.0, 1),
                      'annualized_return_pct': round(ann, 2) if ann is not None else None,
                      'avg_companies': round(sum(ns) / len(ns), 1) if ns else 0})
    spread_ann = (q_ann[1] - q_ann[4]) if (q_ann[1] is not None and q_ann[4] is not None) else None
    stats.append({'quartile': 'Q1 − Q4 spread',
                  'cumulative_return_pct': round(q_cum[1] - q_cum[4], 1),
                  'annualized_return_pct': round(spread_ann, 2) if spread_ann is not None else None,
                  'avg_companies': None})
    cum, vals = 100.0, [100.0]
    for s in used:
        rets = bench[s]
        r = (sum(rets) / len(rets)) if rets else 0.0
        cum *= (1.0 + r)
        vals.append(cum)
    series['Benchmark'] = vals
    years = len(used) / 4.0
    ann_b = ((cum / 100.0) ** (1.0 / years) - 1.0) * 100 if years > 0 else None
    stats.append({'quartile': 'All companies',
                  'cumulative_return_pct': round(cum - 100.0, 1),
                  'annualized_return_pct': round(ann_b, 2) if ann_b is not None else None,
                  'avg_companies': round(sum(len(bench[s]) for s in used) / len(used), 1)})

    # x-axis: formation dates plus one extra point (end of last quarter)
    last = used[-1]
    next_q_month = last.month + 3
    next_q_year = last.year + (1 if next_q_month > 12 else 0)
    next_q_month = next_q_month - 12 if next_q_month > 12 else next_q_month
    next_q_day = 31 if next_q_month in (3, 12) else 30
    labels = labels + [datetime(next_q_year, next_q_month, next_q_day).date().isoformat()]

    return {'success': True, 'available': True,
            'labels': labels, 'series': series, 'stats': stats,
            'counts': counts,
            'first_snapshot': used[0].isoformat(),
            'last_snapshot': last.isoformat(),
            'n_snapshots': len(used)}


def compute_peer_group_outperformance_payload(cur, min_members=3):
    """Payload for /api/v2/peer-group-outperformance."""
    cur.execute("""
        SELECT snapshot_date, company_name, quartile, peer_bucket
        FROM schroders_backtest_scores""")
    score_rows = cur.fetchall()
    if not score_rows:
        return {'success': True, 'available': False,
                'message': 'Backtest has not been run yet.'}

    tick, px = _load_common(cur)
    qret = _quarterly_returns(px)

    snaps = sorted({r[0] for r in score_rows})
    # last 5 years = 20 formation quarters (each earns the following quarter)
    used_snaps = set(snaps[-20:] if len(snaps) > 20 else snaps)

    data = {}
    for snap, comp, quart, bucket in score_rows:
        if snap not in used_snaps or not bucket:
            continue
        t = tick.get(comp)
        r = qret.get(t, {}).get(snap) if t else None
        if r is None:
            continue
        data.setdefault(bucket, {}).setdefault(snap, {1: [], 2: [], 3: [], 4: []})[quart].append(r)

    groups = []
    for bucket, by_snap in data.items():
        snaps_b = sorted(by_snap)
        valid = [s for s in snaps_b
                 if all(len(by_snap[s][q]) >= min_members for q in (1, 2, 3, 4))]
        if len(valid) < 8:   # need at least 2 years of quarters
            continue
        years = len(valid) / 4.0
        out = {'peer_bucket': bucket, 'n_quarters': len(valid),
               'avg_companies': round(sum(
                   sum(len(by_snap[s][q]) for q in (1, 2, 3, 4)) for s in valid
               ) / len(valid), 1)}
        bench_cum = 1.0
        for s in valid:
            all_r = [r for q in (1, 2, 3, 4) for r in by_snap[s][q]]
            bench_cum *= 1.0 + (sum(all_r) / len(all_r) if all_r else 0.0)
        bench_cagr = (bench_cum ** (1.0 / years) - 1.0) * 100
        out['benchmark_cagr'] = round(bench_cagr, 2)
        for q in (1, 2, 3, 4):
            cum = 1.0
            for s in valid:
                rets = by_snap[s][q]
                cum *= 1.0 + (sum(rets) / len(rets) if rets else 0.0)
            cagr = (cum ** (1.0 / years) - 1.0) * 100
            out[f'q{q}_cagr'] = round(cagr, 2)
            out[f'q{q}_outperformance'] = round(cagr - bench_cagr, 2)
        groups.append(out)
    groups.sort(key=lambda g: -(g.get('q1_outperformance') or 0))
    return {'success': True, 'available': bool(groups), 'groups': groups}


# ------------------------------------------------------------ DB payload cache
def ensure_payload_cache_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS backtest_payload_cache (
            cache_key VARCHAR(100) PRIMARY KEY,
            data_version VARCHAR(255),
            payload TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        )""")


def read_cached_payload(cur, cache_key, version):
    """Return the precomputed payload dict if one exists for this version."""
    try:
        cur.execute("""
            SELECT payload FROM backtest_payload_cache
            WHERE cache_key = %s AND data_version = %s""",
                    (cache_key, str(version)))
        row = cur.fetchone()
        return json.loads(row[0]) if row and row[0] else None
    except Exception:
        try:
            cur.connection.rollback()
        except Exception:
            pass
        return None


def write_cached_payload(cur, cache_key, version, payload):
    ensure_payload_cache_table(cur)
    cur.execute("""
        INSERT INTO backtest_payload_cache (cache_key, data_version, payload)
        VALUES (%s, %s, %s)
        ON CONFLICT (cache_key) DO UPDATE
        SET data_version = EXCLUDED.data_version,
            payload = EXCLUDED.payload,
            updated_at = NOW()""",
        (cache_key, str(version), json.dumps(payload)))
