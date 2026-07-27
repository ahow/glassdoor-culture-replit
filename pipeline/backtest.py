"""Point-in-time culture-factor backtest.

For each quarter-end snapshot, recompute per-company dimension evidence
using ONLY reviews posted on or before the snapshot date, apply the
CURRENT production model weights (known look-ahead, documented), rank
companies into quartiles within their current peer buckets, and store the
result. A separate command caches quarter-end share prices from FMP so
the dashboard can chart subsequent performance of the quartile portfolios.

Usage:
    python pipeline/backtest.py scores    # point-in-time scores + quartiles
    python pipeline/backtest.py prices    # quarter-end closes from FMP
    python pipeline/backtest.py all       # scores + prices

Methodology notes / limitations (also shown in the dashboard):
- Model weights and peer buckets are today's (weights need financial
  history that is not available point-in-time) -> look-ahead in weights.
- Universe is today's scored universe -> survivorship bias.
- Prices are FMP end-of-day closes (adjusted close when available, which
  includes dividends via adjustment); no transaction costs; local currency.
"""

import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import date

import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schroders_v2_keywords import SCHRODERS_V2_DIMENSIONS  # noqa: E402
from backtest_payloads import (  # noqa: E402
    compute_backtest_payload,
    compute_peer_group_outperformance_payload,
    write_cached_payload,
)

DIMS = SCHRODERS_V2_DIMENSIONS
FIRST_SNAPSHOT = date(2015, 12, 31)
MIN_AB_DIMS = 4            # eligibility bar per snapshot
SHRINK_K = 50.0            # same k as live pipeline


def conn():
    return psycopg2.connect(os.environ['DATABASE_URL'])


def bump_data_version():
    """Precompute the dashboard payloads for /api/v2/backtest and
    /api/v2/peer-group-outperformance into backtest_payload_cache, then
    bump the backtest data version in app_config so the web dyno's
    in-process caches are invalidated. Payloads are written BEFORE the
    version bump so the endpoints never see a version without a
    ready-made payload."""
    version = str(time.time())
    c = conn()
    cur = c.cursor()

    print('precomputing dashboard payloads...')
    payload = compute_backtest_payload(cur)
    write_cached_payload(cur, 'backtest', version, payload)
    payload = compute_peer_group_outperformance_payload(cur, min_members=3)
    write_cached_payload(cur, 'peer-group-outperformance:3', version, payload)
    c.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_config (
            key VARCHAR(100) PRIMARY KEY,
            value VARCHAR(255),
            updated_at TIMESTAMP DEFAULT NOW()
        )""")
    cur.execute("""
        INSERT INTO app_config (key, value)
        VALUES ('backtest_data_version', %s)
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = NOW()""",
        (version,))
    c.commit()
    c.close()
    print('backtest_data_version bumped (dashboard caches invalidated; '
          'precomputed payloads written)')


def quarter_ends(first=FIRST_SNAPSHOT, last=None):
    """All calendar quarter-end dates from `first` to the last COMPLETED
    quarter before today (or `last`)."""
    today = last or date.today()
    out = []
    y, m = first.year, first.month
    while True:
        d = quarter_end_of(y, m)
        if d >= today:          # only completed quarters
            break
        out.append(d)
        m += 3
        if m > 12:
            m -= 12
            y += 1
    return out


def quarter_end_of(year, month):
    """Quarter-end date for the quarter containing (year, month)."""
    qm = ((month - 1) // 3) * 3 + 3
    if qm in (3, 12):
        day = 31
    else:
        day = 30
    return date(year, qm, day)


# ------------------------------------------------------------------ scores
def build_scores():
    c = conn()
    cur = c.cursor()

    # current peer buckets + model weights (today's model — documented look-ahead)
    cur.execute("SELECT company_name, peer_bucket FROM schroders_company_factor_scores")
    bucket_of = dict(cur.fetchall())
    cur.execute("""
        SELECT peer_bucket, dimension, dimension_weight_final,
               predictor_mean, predictor_std
        FROM schroders_sector_model_weights""")
    model = defaultdict(dict)
    for b, d, w, mu, sd in cur.fetchall():
        model[b][d] = (w, mu, sd if sd else 1.0)

    snapshots = quarter_ends()
    print(f'{len(snapshots)} snapshots: {snapshots[0]} .. {snapshots[-1]}')

    # per company x quarter: total scored reviews
    print('loading per-quarter review totals...')
    cur.execute("""
        SELECT s.company_name, date_trunc('quarter', r.review_datetime)::date, count(*)
        FROM review_culture_scores s
        JOIN reviews r ON s.review_id = r.id
        WHERE s.company_name IS NOT NULL AND r.review_datetime IS NOT NULL
        GROUP BY 1, 2""")
    totals_q = defaultdict(dict)      # comp -> {qstart: n}
    for comp, q, n in cur.fetchall():
        totals_q[comp][q] = n

    # per company x dimension x quarter: n scored, sum of scores
    dim_q = {d: defaultdict(dict) for d in DIMS}   # d -> comp -> {qstart: (n,sum)}
    for d in DIMS:
        print(f'loading quarterly aggregates for {d}...')
        cur.execute(f"""
            SELECT s.company_name, date_trunc('quarter', r.review_datetime)::date,
                   count(s.schroders_v2_{d}_score),
                   sum(s.schroders_v2_{d}_score)
            FROM review_culture_scores s
            JOIN reviews r ON s.review_id = r.id
            WHERE s.company_name IS NOT NULL AND r.review_datetime IS NOT NULL
              AND s.schroders_v2_{d}_score IS NOT NULL
            GROUP BY 1, 2""")
        for comp, q, n, sm in cur.fetchall():
            dim_q[d][comp][q] = (n, float(sm))

    companies = sorted(set(totals_q) & set(bucket_of))
    print(f'{len(companies)} companies in backtest universe')

    cur.execute("""
        CREATE TABLE IF NOT EXISTS schroders_backtest_scores (
            snapshot_date DATE NOT NULL,
            company_name TEXT NOT NULL,
            peer_bucket TEXT,
            n_total_reviews INT,
            n_dims_ab INT,
            factor_raw DOUBLE PRECISION,
            pctile DOUBLE PRECISION,
            quartile INT,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (snapshot_date, company_name))""")
    cur.execute("DELETE FROM schroders_backtest_scores")

    # cumulative state per company
    cum_total = defaultdict(int)
    cum_dim = {d: defaultdict(lambda: [0, 0.0]) for d in DIMS}   # comp -> [n, sum]
    q_iter_done = set()   # (quarter-start dates already folded in)

    all_quarters = sorted({q for comp in totals_q for q in totals_q[comp]})

    rows_out = []
    for snap in snapshots:
        # fold in all quarters that END on or before the snapshot
        for q in all_quarters:
            if q in q_iter_done:
                continue
            if quarter_end_of(q.year, q.month) <= snap:
                for comp, n in ((comp, totals_q[comp].get(q)) for comp in totals_q):
                    if n:
                        cum_total[comp] += n
                for d in DIMS:
                    dq = dim_q[d]
                    for comp in dq:
                        v = dq[comp].get(q)
                        if v:
                            st = cum_dim[d][comp]
                            st[0] += v[0]
                            st[1] += v[1]
                q_iter_done.add(q)

        # point-in-time evidence -> tier + raw mean per dimension
        raw_mean, tier, ab_n = {}, {}, defaultdict(int)
        for comp in companies:
            total = cum_total.get(comp, 0)
            for d in DIMS:
                n, sm = cum_dim[d].get(comp, (0, 0.0))
                if n < 5:
                    t = 'D'
                elif n >= 50 and total >= 150:
                    t = 'A'
                elif (20 <= n < 50) or (50 <= total < 150):
                    t = 'B'
                else:
                    t = 'C'
                tier[(comp, d)] = t
                raw_mean[(comp, d)] = (sm / n) if n else None
                if t in ('A', 'B'):
                    ab_n[comp] += 1

        # shrinkage toward point-in-time bucket prior
        prior = defaultdict(list)
        for comp in companies:
            b = bucket_of[comp]
            for d in DIMS:
                v = raw_mean[(comp, d)]
                if v is not None:
                    prior[(b, d)].append(v)
        prior = {k: float(np.mean(v)) for k, v in prior.items()}

        # factor via current model weights
        eligible = [comp for comp in companies if ab_n[comp] >= MIN_AB_DIMS]
        raw_f = {}
        for comp in eligible:
            b = bucket_of[comp]
            mw = model.get(b)
            if not mw:
                continue
            f = 0.0
            for d in DIMS:
                w, mu, sd = mw.get(d, (0.0, 0.0, 1.0))
                v = raw_mean[(comp, d)]
                n = cum_dim[d].get(comp, (0, 0.0))[0]
                if v is None or tier[(comp, d)] == 'D':
                    shrunk = mu          # neutral
                else:
                    wt = n / (n + SHRINK_K)
                    pm = prior.get((b, d), mu)
                    shrunk = wt * v + (1 - wt) * pm
                f += w * (shrunk - mu) / sd
            raw_f[comp] = f

        # percentile within bucket, quartile from percentile
        by_bucket = defaultdict(list)
        for comp in raw_f:
            by_bucket[bucket_of[comp]].append(comp)
        for b, comps_b in by_bucket.items():
            vals = np.array([raw_f[comp] for comp in comps_b])
            order = vals.argsort().argsort()
            nb = len(comps_b)
            for i, comp in enumerate(comps_b):
                pct = 100.0 * (order[i] + 0.5) / nb
                quart = 1 if pct >= 75 else 2 if pct >= 50 else 3 if pct >= 25 else 4
                rows_out.append((snap, comp, b, cum_total.get(comp, 0),
                                 ab_n[comp], float(raw_f[comp]),
                                 float(pct), quart))
        print(f'  {snap}: {len(raw_f)} eligible companies')

    execute_values(cur, """
        INSERT INTO schroders_backtest_scores
            (snapshot_date, company_name, peer_bucket, n_total_reviews,
             n_dims_ab, factor_raw, pctile, quartile)
        VALUES %s""", rows_out, page_size=2000)
    c.commit()
    print(f'wrote {len(rows_out)} backtest score rows')
    c.close()


# ------------------------------------------------------------------ prices
def build_prices():
    api_key = os.environ.get('FMP_API_KEY')
    if not api_key:
        print('FMP_API_KEY not set'); sys.exit(1)
    c = conn()
    cur = c.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS backtest_quarter_prices (
            ticker TEXT NOT NULL,
            quarter_end DATE NOT NULL,
            close DOUBLE PRECISION,
            fetched_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (ticker, quarter_end))""")
    # universe: tickers of companies in the backtest scores table
    cur.execute("""
        SELECT DISTINCT f.ticker
        FROM fmp_performance_metrics f
        JOIN (SELECT DISTINCT company_name FROM schroders_backtest_scores) b
          ON lower(f.company_name) = lower(b.company_name)
        WHERE f.ticker IS NOT NULL AND f.ticker <> ''""")
    tickers = sorted(r[0] for r in cur.fetchall())
    # a ticker is up to date only if it has the latest completed quarter end;
    # anything else (new ticker OR stale ticker) gets refetched
    latest_qe = quarter_ends()[-1]
    cur.execute("SELECT DISTINCT ticker FROM backtest_quarter_prices WHERE quarter_end = %s",
                (latest_qe,))
    done = {r[0] for r in cur.fetchall()}
    todo = [t for t in tickers if t not in done]
    print(f'{len(tickers)} tickers, {len(todo)} to fetch')

    snaps = quarter_ends()
    ok = fail = 0
    for i, tk in enumerate(todo):
        try:
            resp = requests.get(
                'https://financialmodelingprep.com/stable/historical-price-eod/full',
                params={'symbol': tk, 'apikey': api_key,
                        'from': '2015-01-01'},
                timeout=15)
            data = resp.json() if resp.status_code == 200 else None
        except Exception as e:
            print(f'  {tk}: request error {e}')
            data = None
        if not data or not isinstance(data, list):
            fail += 1
            time.sleep(0.1)
            continue
        # last close on or before each quarter end (within 10 days lookback)
        by_date = {}
        for entry in data:
            ds = entry.get('date')
            px = entry.get('adjClose') or entry.get('close')
            if ds and px:
                by_date[ds] = float(px)
        dates_sorted = sorted(by_date)
        rows = []
        import bisect
        for qe in snaps + [quarter_end_of(date.today().year, date.today().month)]:
            qs = qe.isoformat()
            idx = bisect.bisect_right(dates_sorted, qs) - 1
            if idx >= 0:
                ds = dates_sorted[idx]
                # must be within the same quarter (avoid stale delisted prices)
                if (qe - date.fromisoformat(ds)).days <= 10:
                    rows.append((tk, qe, by_date[ds]))
        if rows:
            execute_values(cur, """
                INSERT INTO backtest_quarter_prices (ticker, quarter_end, close)
                VALUES %s
                ON CONFLICT (ticker, quarter_end)
                DO UPDATE SET close = EXCLUDED.close, fetched_at = now()""",
                           rows)
            c.commit()
            ok += 1
        else:
            fail += 1
        if (i + 1) % 50 == 0:
            print(f'  {i + 1}/{len(todo)} fetched (ok {ok}, fail {fail})')
        time.sleep(0.1)
    print(f'prices done: ok {ok}, fail {fail}')
    c.close()


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'all'
    if cmd == 'scores':
        build_scores()
        bump_data_version()
    elif cmd == 'prices':
        build_prices()
        bump_data_version()
    elif cmd == 'all':
        build_scores()
        build_prices()
        bump_data_version()
    else:
        print('unknown command', cmd)
