"""Backtest weight-estimation variants (spec: backtest_variants_spec).

Compares three ways of setting the ridge weights used in the quarterly
quartile backtest:

  C  Current    — today's production weights at all snapshots (baseline).
  A  Averaged   — one fixed weight vector per bucket: confidence-weighted
                  average over ALL rolling 5-yr windows (still look-ahead).
  W  Walk-fwd   — at each snapshot T, average only windows ending <= T.

Everything else (point-in-time evidence, tiers, shrinkage, today's buckets,
eligibility >=4 A/B dims, quartiles within bucket, equal weight, quarterly
rebalancing, backtest_quarter_prices, no costs) is identical across variants.

Commands (all resumable; run against the production DATABASE_URL):
    python pipeline/backtest_variants.py fundamentals  # FMP annual history
    python pipeline/backtest_variants.py earlyprices   # 2010-2014 year-ends
    python pipeline/backtest_variants.py run           # windows + variants + outputs

Conventions logged per spec:
- conf(k, b) = sum over estimation-set companies of TOTAL scored reviews
  (rows in review_culture_scores) as of E_k  (company total, not per-dim sum).
- Averaging is done on standardised coefficients, with confidence-weighted
  average mu/sd used to standardise at scoring time.
- Split period: formation snapshots < 2020-12-31 => "2016-2020"
  (returns through 2020-12-31); snapshots >= 2020-12-31 => "2021-present".
- Turnover: one-way = |names added to Q1| / |Q1| averaged over rebalances.
"""

import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date

import numpy as np
import psycopg2
import requests
from psycopg2.extras import execute_values

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

from schroders_v2_keywords import SCHRODERS_V2_DIMENSIONS  # noqa: E402
from factor_build import _ridge_fit, _ridge_solve  # noqa: E402
from backtest import quarter_ends, quarter_end_of  # noqa: E402

DIMS = SCHRODERS_V2_DIMENSIONS
NDIM = len(DIMS)
SHRINK_K = 50.0
MIN_AB_SCORE = 4       # eligibility to be scored at a snapshot
MIN_AB_EST = 6         # eligibility for the estimation set (as production §9)
MIN_MODEL = 8          # bucket needs >= 8 estimation companies for own model
COEF_M = 10.0          # lambda = n / (n + 10) shrink toward window global
ALPHAS = [0.1, 0.3, 1.0, 3.0, 10.0, 30.0]
TARGET_W = [0.30, 0.25, 0.25, 0.20]   # roe, revgrowth, tsr, opmargin
FMP_BASE = 'https://financialmodelingprep.com/stable'
OUT_DIR = os.path.join(os.path.dirname(_HERE), 'pipeline_output', 'backtest_variants')

WINDOW_ENDS = [date(y, 12, 31) for y in range(2015, 2026)] + [date(2026, 6, 30)]
SPLIT_DATE = date(2020, 12, 31)


def conn():
    return psycopg2.connect(os.environ['DATABASE_URL'])


def _fmp_key():
    k = os.environ.get('FMP_API_KEY')
    if not k:
        print('FMP_API_KEY not set'); sys.exit(1)
    return k


def _universe_tickers(cur):
    cur.execute("""
        SELECT DISTINCT f.ticker
        FROM fmp_performance_metrics f
        JOIN schroders_company_factor_scores s
          ON lower(f.company_name) = lower(s.company_name)
        WHERE f.ticker IS NOT NULL AND f.ticker <> ''""")
    return sorted(r[0] for r in cur.fetchall())


# ------------------------------------------------------- fundamentals fetch
def fetch_fundamentals():
    api_key = _fmp_key()
    c = conn(); cur = c.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fmp_annual_fundamentals (
            ticker TEXT NOT NULL, fiscal_year INT NOT NULL,
            roe DOUBLE PRECISION, op_margin DOUBLE PRECISION,
            revenue DOUBLE PRECISION,
            fetched_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (ticker, fiscal_year));
        CREATE TABLE IF NOT EXISTS fmp_annual_fetch_log (
            ticker TEXT PRIMARY KEY, status TEXT,
            fetched_at TIMESTAMPTZ DEFAULT now())""")
    c.commit()
    tickers = _universe_tickers(cur)
    cur.execute("SELECT ticker FROM fmp_annual_fetch_log")
    done = {r[0] for r in cur.fetchall()}
    todo = [t for t in tickers if t not in done]
    print(f'{len(tickers)} tickers, {len(todo)} to fetch')

    def get(endpoint, tk):
        try:
            r = requests.get(f'{FMP_BASE}/{endpoint}',
                             params={'symbol': tk, 'period': 'annual',
                                     'limit': 16, 'apikey': api_key},
                             timeout=15)
            return r.json() if r.status_code == 200 else None
        except Exception:
            return None

    from concurrent.futures import ThreadPoolExecutor

    def fetch_one(tk):
        return (tk, get('key-metrics', tk) or [], get('ratios', tk) or [],
                get('income-statement', tk) or [])

    pool = ThreadPoolExecutor(max_workers=6)
    for i, (tk, km, ra, inc) in enumerate(pool.map(fetch_one, todo)):
        rows = {}
        for e in km:
            fy = e.get('fiscalYear') or e.get('calendarYear')
            if fy:
                rows.setdefault(int(fy), {})['roe'] = e.get('returnOnEquity')
        for e in ra:
            fy = e.get('fiscalYear') or e.get('calendarYear')
            if fy:
                rows.setdefault(int(fy), {})['opm'] = e.get('operatingProfitMargin')
        for e in inc:
            fy = e.get('fiscalYear') or e.get('calendarYear')
            if fy:
                rows.setdefault(int(fy), {})['rev'] = e.get('revenue')
        vals = [(tk, fy, d.get('roe'), d.get('opm'), d.get('rev'))
                for fy, d in sorted(rows.items())]
        if vals:
            execute_values(cur, """
                INSERT INTO fmp_annual_fundamentals
                    (ticker, fiscal_year, roe, op_margin, revenue)
                VALUES %s
                ON CONFLICT (ticker, fiscal_year) DO UPDATE
                SET roe = EXCLUDED.roe, op_margin = EXCLUDED.op_margin,
                    revenue = EXCLUDED.revenue, fetched_at = now()""", vals)
        cur.execute("""
            INSERT INTO fmp_annual_fetch_log (ticker, status) VALUES (%s, %s)
            ON CONFLICT (ticker) DO UPDATE SET status = EXCLUDED.status,
                fetched_at = now()""",
            (tk, 'ok' if vals else 'empty'))
        c.commit()
        if (i + 1) % 50 == 0:
            print(f'  {i + 1}/{len(todo)}')

    print('fundamentals done')
    c.close()


# ------------------------------------------------------ early price fetch
def fetch_early_prices():
    api_key = _fmp_key()
    c = conn(); cur = c.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS backtest_variants_early_prices (
            ticker TEXT NOT NULL, year_end DATE NOT NULL,
            close DOUBLE PRECISION,
            fetched_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (ticker, year_end));
        CREATE TABLE IF NOT EXISTS backtest_variants_early_price_log (
            ticker TEXT PRIMARY KEY, status TEXT,
            fetched_at TIMESTAMPTZ DEFAULT now())""")
    c.commit()
    tickers = _universe_tickers(cur)
    cur.execute("SELECT ticker FROM backtest_variants_early_price_log")
    done = {r[0] for r in cur.fetchall()}
    todo = [t for t in tickers if t not in done]
    print(f'{len(tickers)} tickers, {len(todo)} to fetch')
    year_ends = [date(y, 12, 31) for y in range(2010, 2015)]
    import bisect
    from concurrent.futures import ThreadPoolExecutor

    def fetch_one(tk):
        try:
            r = requests.get(f'{FMP_BASE}/historical-price-eod/full',
                             params={'symbol': tk, 'apikey': api_key,
                                     'from': '2010-11-01', 'to': '2015-01-15'},
                             timeout=15)
            return tk, (r.json() if r.status_code == 200 else None)
        except Exception:
            return tk, None

    pool = ThreadPoolExecutor(max_workers=6)
    for i, (tk, data) in enumerate(pool.map(fetch_one, todo)):
        rows = []
        if data and isinstance(data, list):
            by_date = {}
            for e in data:
                ds, px = e.get('date'), e.get('adjClose') or e.get('close')
                if ds and px:
                    by_date[ds] = float(px)
            ds_sorted = sorted(by_date)
            for ye in year_ends:
                idx = bisect.bisect_right(ds_sorted, ye.isoformat()) - 1
                if idx >= 0:
                    ds = ds_sorted[idx]
                    if (ye - date.fromisoformat(ds)).days <= 10:
                        rows.append((tk, ye, by_date[ds]))
        if rows:
            execute_values(cur, """
                INSERT INTO backtest_variants_early_prices (ticker, year_end, close)
                VALUES %s ON CONFLICT (ticker, year_end)
                DO UPDATE SET close = EXCLUDED.close, fetched_at = now()""", rows)
        cur.execute("""
            INSERT INTO backtest_variants_early_price_log (ticker, status)
            VALUES (%s, %s) ON CONFLICT (ticker) DO UPDATE
            SET status = EXCLUDED.status, fetched_at = now()""",
            (tk, 'ok' if rows else 'empty'))
        c.commit()
        if (i + 1) % 50 == 0:
            print(f'  {i + 1}/{len(todo)}')

    print('early prices done')
    c.close()


# ---------------------------------------------------------- PIT machinery
def load_pit_inputs(cur, bucket_of):
    """Quarterly cumulative review evidence, exactly as backtest.py.
    Each expensive query result is checkpointed to /tmp so timeouts resume."""
    import pickle
    ck_dir = '/tmp/bv_pit_inputs'
    os.makedirs(ck_dir, exist_ok=True)

    tot_pkl = os.path.join(ck_dir, 'totals.pkl')
    if os.path.exists(tot_pkl):
        with open(tot_pkl, 'rb') as f:
            totals_q = pickle.load(f)
        print('per-quarter review totals: cached')
    else:
        print('loading per-quarter review totals...')
        cur.execute("""
            SELECT s.company_name, date_trunc('quarter', r.review_datetime)::date, count(*)
            FROM review_culture_scores s
            JOIN reviews r ON s.review_id = r.id
            WHERE s.company_name IS NOT NULL AND r.review_datetime IS NOT NULL
            GROUP BY 1, 2""")
        totals_q = defaultdict(dict)
        for comp, q, n in cur.fetchall():
            totals_q[comp][q] = n
        with open(tot_pkl, 'wb') as f:
            pickle.dump(dict(totals_q), f, protocol=4)

    dim_q = {d: defaultdict(dict) for d in DIMS}
    for d in DIMS:
        d_pkl = os.path.join(ck_dir, f'{d}.pkl')
        if os.path.exists(d_pkl):
            with open(d_pkl, 'rb') as f:
                dim_q[d] = pickle.load(f)
            print(f'quarterly aggregates for {d}: cached')
            continue
        print(f'loading quarterly aggregates for {d}...')
        cur.execute(f"""
            SELECT s.company_name, date_trunc('quarter', r.review_datetime)::date,
                   count(s.schroders_v2_{d}_score), sum(s.schroders_v2_{d}_score)
            FROM review_culture_scores s
            JOIN reviews r ON s.review_id = r.id
            WHERE s.company_name IS NOT NULL AND r.review_datetime IS NOT NULL
              AND s.schroders_v2_{d}_score IS NOT NULL
            GROUP BY 1, 2""")
        for comp, q, n, sm in cur.fetchall():
            dim_q[d][comp][q] = (n, float(sm))
        with open(d_pkl, 'wb') as f:
            pickle.dump(dict(dim_q[d]), f, protocol=4)
    companies = sorted(set(totals_q) & set(bucket_of))
    print(f'{len(companies)} companies in backtest universe')
    return totals_q, dim_q, companies


def snapshot_states(totals_q, dim_q, companies, bucket_of, snapshots):
    """Yield (snap, state) where state has raw means, n, tiers, ab_n,
    cum_total, and PIT bucket priors — everything weight-independent."""
    cum_total = defaultdict(int)
    cum_dim = {d: defaultdict(lambda: [0, 0.0]) for d in DIMS}
    q_done = set()
    all_quarters = sorted({q for comp in totals_q for q in totals_q[comp]})
    for snap in snapshots:
        for q in all_quarters:
            if q in q_done or quarter_end_of(q.year, q.month) > snap:
                continue
            for comp in totals_q:
                n = totals_q[comp].get(q)
                if n:
                    cum_total[comp] += n
            for d in DIMS:
                dq = dim_q[d]
                for comp in dq:
                    v = dq[comp].get(q)
                    if v:
                        st = cum_dim[d][comp]
                        st[0] += v[0]; st[1] += v[1]
            q_done.add(q)

        raw = {}        # comp -> np arrays (v, n) ; v nan when no evidence
        tier_ok = {}    # comp -> bool array (tier not D)
        ab_n = {}
        for comp in companies:
            total = cum_total.get(comp, 0)
            v = np.full(NDIM, np.nan)
            nn = np.zeros(NDIM)
            ok = np.zeros(NDIM, bool)
            ab = 0
            for j, d in enumerate(DIMS):
                n, sm = cum_dim[d].get(comp, (0, 0.0))
                if n:
                    v[j] = sm / n
                    nn[j] = n
                if n < 5:
                    t = 'D'
                elif n >= 50 and total >= 150:
                    t = 'A'
                elif (20 <= n < 50) or (50 <= total < 150):
                    t = 'B'
                else:
                    t = 'C'
                ok[j] = t != 'D'
                if t in ('A', 'B'):
                    ab += 1
            raw[comp] = (v, nn)
            tier_ok[comp] = ok
            ab_n[comp] = ab
        # PIT bucket priors over raw means (not None), as backtest.py
        prior_acc = defaultdict(list)
        for comp in companies:
            b = bucket_of[comp]
            v = raw[comp][0]
            for j in range(NDIM):
                if not np.isnan(v[j]):
                    prior_acc[(b, j)].append(v[j])
        prior = {k: float(np.mean(vv)) for k, vv in prior_acc.items()}
        yield snap, dict(raw=raw, tier_ok=tier_ok, ab_n=ab_n,
                         cum_total=dict(cum_total), prior=prior)


def shrunk_vector(state, comp, bucket):
    """PIT shrunk-internal values; nan where no evidence or tier D."""
    v, nn = state['raw'][comp]
    ok = state['tier_ok'][comp]
    out = np.full(NDIM, np.nan)
    for j in range(NDIM):
        if not np.isnan(v[j]) and ok[j]:
            wt = nn[j] / (nn[j] + SHRINK_K)
            pm = state['prior'].get((bucket, j), v[j])
            out[j] = wt * v[j] + (1 - wt) * pm
    return out


# ---------------------------------------------------------- window targets
def window_targets(cur, tick_of, window_end):
    """Composite target per company measured inside the 5-yr window."""
    y1 = window_end.year - (4 if window_end.month == 12 else 5)
    y2 = window_end.year if window_end.month == 12 else window_end.year - 1
    cur.execute("""
        SELECT ticker, fiscal_year, roe, op_margin, revenue
        FROM fmp_annual_fundamentals
        WHERE fiscal_year BETWEEN %s AND %s""", (y1, y2))
    fund = defaultdict(dict)
    for tk, fy, roe, opm, rev in cur.fetchall():
        fund[tk][fy] = (roe, opm, rev)

    # prices for TSR: end price at window_end, start 5 years earlier
    start_d = date(window_end.year - 5, window_end.month, window_end.day)
    px_end, px_start = {}, {}
    cur.execute("SELECT ticker, close FROM backtest_quarter_prices WHERE quarter_end = %s",
                (window_end,))
    px_end.update({t: p for t, p in cur.fetchall() if p and p > 0})
    if start_d.year >= 2015:
        cur.execute("SELECT ticker, close FROM backtest_quarter_prices WHERE quarter_end = %s",
                    (start_d,))
        px_start.update({t: p for t, p in cur.fetchall() if p and p > 0})
    else:
        cur.execute("SELECT ticker, close FROM backtest_variants_early_prices WHERE year_end = %s",
                    (start_d,))
        px_start.update({t: p for t, p in cur.fetchall() if p and p > 0})

    metrics = {}   # comp -> [roe, revg, tsr, opm]
    for comp, tk in tick_of.items():
        fy_rows = fund.get(tk, {})
        roes = [r for (r, o, v) in fy_rows.values() if r is not None]
        opms = [o for (r, o, v) in fy_rows.values() if o is not None]
        revs = sorted((fy, v) for fy, (r, o, v) in fy_rows.items()
                      if v is not None and v > 0)
        roe = float(np.mean(roes)) if len(roes) >= 3 else None
        opm = float(np.mean(opms)) if len(opms) >= 3 else None
        revg = None
        if len(revs) >= 2 and revs[-1][0] - revs[0][0] >= 2:
            yrs = revs[-1][0] - revs[0][0]
            revg = (revs[-1][1] / revs[0][1]) ** (1.0 / yrs) - 1.0
        tsr = None
        pe, ps = px_end.get(tk), px_start.get(tk)
        if pe and ps:
            tsr = (pe / ps) ** (1.0 / 5.0) - 1.0
        if any(x is not None for x in (roe, revg, tsr, opm)):
            metrics[comp] = [roe, revg, tsr, opm]

    # z-score within window, clamp, weight-renormalise (as perf_targets)
    stats = []
    for j in range(4):
        vals = [m[j] for m in metrics.values() if m[j] is not None]
        stats.append((float(np.mean(vals)), float(np.std(vals)))
                     if len(vals) >= 3 and np.std(vals) > 0 else None)
    target = {}
    for comp, m in metrics.items():
        num = den = 0.0
        for j in range(4):
            if m[j] is not None and stats[j]:
                z = max(-2, min(2, (m[j] - stats[j][0]) / stats[j][1]))
                num += TARGET_W[j] * z
                den += TARGET_W[j]
        if den > 0:
            target[comp] = num / den
    return target


# ------------------------------------------------------------- estimation
def estimate_windows(cur, bucket_of, tick_of, window_states):
    """Per-window ridge estimation -> rows for schroders_rolling_window_weights.
    Returns {window_end: {bucket_or_'global': modeldict}}."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schroders_rolling_window_weights (
            window_end DATE NOT NULL, bucket TEXT NOT NULL,
            dimension TEXT NOT NULL,
            coef DOUBLE PRECISION, mu DOUBLE PRECISION, sd DOUBLE PRECISION,
            alpha DOUBLE PRECISION, n_est INT,
            r2 DOUBLE PRECISION, cv_r2 DOUBLE PRECISION,
            conf DOUBLE PRECISION,
            PRIMARY KEY (window_end, bucket, dimension))""")

    cur.execute("SELECT DISTINCT window_end FROM schroders_rolling_window_weights")
    done = {r[0] for r in cur.fetchall()}

    windows = {}
    for we, state in sorted(window_states.items()):
        if we in done:
            print(f'  window {we}: already estimated — skipped')
            continue
        target = window_targets(cur, tick_of, we)
        comps = [comp for comp in state['raw']
                 if comp in target and state['ab_n'][comp] >= MIN_AB_EST]
        if len(comps) < MIN_MODEL:
            print(f'  window {we}: only {len(comps)} estimable companies — skipped')
            continue

        def design(cs, mu=None, sd=None):
            M = np.array([shrunk_vector(state, comp, bucket_of[comp]) for comp in cs])
            col_mean = np.nanmean(M, axis=0)
            inds = np.where(np.isnan(M))
            M[inds] = np.take(col_mean, inds[1])
            if mu is None:
                mu = M.mean(0); sd = M.std(0); sd[sd == 0] = 1.0
            return (M - mu) / sd, mu, sd

        Xg, mu_g, sd_g = design(comps)
        yg = np.array([target[comp] for comp in comps])
        coef_g, _, a_g, r2_g, cv_g = _ridge_fit(Xg, yg, ALPHAS)
        conf_g = float(sum(state['cum_total'].get(comp, 0) for comp in comps))
        models = {'global': dict(coef=coef_g, mu=mu_g, sd=sd_g, alpha=a_g,
                                 n=len(comps), r2=r2_g, cv=cv_g, conf=conf_g)}
        by_bucket = defaultdict(list)
        for comp in comps:
            by_bucket[bucket_of[comp]].append(comp)
        for b, cs in by_bucket.items():
            if b == 'global' or len(cs) < MIN_MODEL:
                continue
            Xb, mu_b, sd_b = design(cs)
            yb = np.array([target[comp] for comp in cs])
            coef_s, _, a_b, r2_b, cv_b = _ridge_fit(Xb, yb, ALPHAS)
            lam = len(cs) / (len(cs) + COEF_M)
            coef_b = lam * coef_s + (1 - lam) * coef_g
            conf_b = float(sum(state['cum_total'].get(comp, 0) for comp in cs))
            models[b] = dict(coef=coef_b, mu=mu_b, sd=sd_b, alpha=a_b,
                             n=len(cs), r2=r2_b, cv=cv_b, conf=conf_b)
        windows[we] = models
        rows = []
        for b, m in models.items():
            for j, d in enumerate(DIMS):
                rows.append((we, b, d, float(m['coef'][j]), float(m['mu'][j]),
                             float(m['sd'][j]), m['alpha'], m['n'],
                             m['r2'], m['cv'], m['conf']))
        execute_values(cur, """
            INSERT INTO schroders_rolling_window_weights
                (window_end, bucket, dimension, coef, mu, sd, alpha,
                 n_est, r2, cv_r2, conf) VALUES %s""", rows)
        cur.connection.commit()
        print(f'  window {we}: n_est={len(comps)} global R2={r2_g:.3f} '
              f'CV={cv_g:.3f} bucket_models={len(models) - 1}')
    return windows


def average_models(windows, buckets_all, upto=None):
    """Confidence-weighted average of standardised coefs + mu/sd per bucket.
    Buckets with no bucket-specific window models use the global average."""
    wes = sorted(we for we in windows if upto is None or we <= upto)
    if not wes:
        return None

    def avg(entries):
        wsum = sum(e['conf'] for e in entries)
        coef = sum(e['conf'] * np.asarray(e['coef']) for e in entries) / wsum
        mu = sum(e['conf'] * np.asarray(e['mu']) for e in entries) / wsum
        sd = sum(e['conf'] * np.asarray(e['sd']) for e in entries) / wsum
        sd = np.where(sd == 0, 1.0, sd)
        return dict(coef=coef, mu=mu, sd=sd)

    g_entries = [windows[we]['global'] for we in wes]
    g_avg = avg(g_entries)
    out = {}
    for b in buckets_all:
        entries = [windows[we][b] for we in wes if b in windows[we]]
        out[b] = avg(entries) if entries else g_avg
    out['global'] = g_avg
    return out


# ------------------------------------------------------------ backtesting
def score_snapshot(state, bucket_of, model_by_bucket):
    """factor + quartiles for one snapshot given per-bucket weights."""
    raw_f = {}
    for comp in state['raw']:
        if state['ab_n'][comp] < MIN_AB_SCORE:
            continue
        b = bucket_of[comp]
        m = model_by_bucket.get(b) or model_by_bucket.get('global')
        if m is None:
            continue
        sv = shrunk_vector(state, comp, b)
        x = np.where(np.isnan(sv), 0.0, (sv - m['mu']) / m['sd'])
        x[np.isnan(sv)] = 0.0
        raw_f[comp] = float(np.dot(m['coef'], x))
    quart = {}
    by_bucket = defaultdict(list)
    for comp in raw_f:
        by_bucket[bucket_of[comp]].append(comp)
    for b, cs in by_bucket.items():
        vals = np.array([raw_f[comp] for comp in cs])
        order = vals.argsort().argsort()
        nb = len(cs)
        for i, comp in enumerate(cs):
            pct = 100.0 * (order[i] + 0.5) / nb
            quart[comp] = 1 if pct >= 75 else 2 if pct >= 50 else 3 if pct >= 25 else 4
    return quart


def portfolio_series(quarts_by_snap, tick_of, qret, snaps_used):
    """Equal-weight quarterly returns per quartile + benchmark."""
    per_q = {q: [] for q in (1, 2, 3, 4)}
    bench = []
    counts = {q: [] for q in (1, 2, 3, 4)}
    q1_names = []
    for s in snaps_used:
        holdings = {1: [], 2: [], 3: [], 4: []}
        names1 = set()
        all_r = []
        for comp, q in quarts_by_snap[s].items():
            t = tick_of.get(comp)
            r = qret.get(t, {}).get(s) if t else None
            if r is None:
                continue
            holdings[q].append(r)
            all_r.append(r)
            if q == 1:
                names1.add(comp)
        for q in (1, 2, 3, 4):
            rs = holdings[q]
            per_q[q].append(sum(rs) / len(rs) if rs else 0.0)
            counts[q].append(len(rs))
        bench.append(sum(all_r) / len(all_r) if all_r else 0.0)
        q1_names.append(names1)
    return per_q, bench, counts, q1_names


def stats_block(rets, years):
    cum = float(np.prod([1 + r for r in rets]))
    ann = (cum ** (1 / years) - 1) * 100 if years > 0 else None
    return round((cum - 1) * 100, 1), round(ann, 2) if ann is not None else None


PKL = os.path.join('/tmp', 'bv_pit_states.pkl')


def build_pit():
    """Stage 1: compute all PIT snapshot states, checkpoint to pickle."""
    import pickle
    os.makedirs(OUT_DIR, exist_ok=True)
    c = conn(); cur = c.cursor()
    cur.execute("SELECT company_name, peer_bucket FROM schroders_company_factor_scores")
    bucket_of = dict(cur.fetchall())
    totals_q, dim_q, companies = load_pit_inputs(cur, bucket_of)
    snapshots = quarter_ends()
    states = {}
    for snap, state in snapshot_states(totals_q, dim_q, companies,
                                       bucket_of, snapshots):
        states[snap] = state
        print(f'  state {snap}: eligible='
              f'{sum(1 for v in state["ab_n"].values() if v >= MIN_AB_SCORE)}')
    with open(PKL, 'wb') as f:
        pickle.dump(dict(states=states, bucket_of=bucket_of,
                         snapshots=snapshots), f, protocol=4)
    print(f'PIT states checkpointed to {PKL}')
    c.close()


def build_windows():
    """Stage 2: per-window estimation (resumable per window)."""
    import pickle
    with open(PKL, 'rb') as f:
        ck = pickle.load(f)
    c = conn(); cur = c.cursor()
    cur.execute("""
        SELECT company_name, ticker FROM fmp_performance_metrics
        WHERE ticker IS NOT NULL AND ticker <> ''""")
    tick_of = {cn: t for cn, t in cur.fetchall()}
    window_set = set(w for w in WINDOW_ENDS if w <= ck['snapshots'][-1])
    window_states = {s: ck['states'][s] for s in ck['snapshots'] if s in window_set}
    estimate_windows(cur, ck['bucket_of'], tick_of, window_states)
    c.close()


def load_windows_from_db(cur):
    """Rebuild the {window_end: {bucket: model}} dict from the DB table."""
    cur.execute("""
        SELECT window_end, bucket, dimension, coef, mu, sd, alpha,
               n_est, r2, cv_r2, conf
        FROM schroders_rolling_window_weights""")
    acc = defaultdict(lambda: defaultdict(dict))
    meta = {}
    for we, b, d, coef, mu, sd, a, n, r2, cv, cf in cur.fetchall():
        acc[we][b][d] = (coef, mu, sd)
        meta[(we, b)] = dict(alpha=a, n=n, r2=r2, cv=cv, conf=cf)
    windows = {}
    for we, bd in acc.items():
        windows[we] = {}
        for b, dd in bd.items():
            m = meta[(we, b)]
            windows[we][b] = dict(
                coef=np.array([dd[d][0] for d in DIMS]),
                mu=np.array([dd[d][1] for d in DIMS]),
                sd=np.array([dd[d][2] for d in DIMS]),
                alpha=m['alpha'], n=m['n'], r2=m['r2'], cv=m['cv'],
                conf=m['conf'])
    return windows


def run_variants():
    import pickle
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(PKL, 'rb') as f:
        ck = pickle.load(f)
    states, bucket_of, snapshots = ck['states'], ck['bucket_of'], ck['snapshots']
    c = conn(); cur = c.cursor()

    cur.execute("""
        SELECT peer_bucket, dimension, dimension_weight_final,
               predictor_mean, predictor_std
        FROM schroders_sector_model_weights""")
    prod_raw = defaultdict(dict)
    for b, d, w, mu, sd in cur.fetchall():
        prod_raw[b][d] = (w, mu, sd if sd else 1.0)
    prod_model = {}
    for b, dd in prod_raw.items():
        coef = np.array([dd.get(d, (0.0, 0.0, 1.0))[0] for d in DIMS])
        mu = np.array([dd.get(d, (0.0, 0.0, 1.0))[1] for d in DIMS])
        sd = np.array([dd.get(d, (0.0, 0.0, 1.0))[2] for d in DIMS])
        prod_model[b] = dict(coef=coef, mu=mu, sd=sd)
    cur.execute("""
        SELECT peer_bucket, max(n_companies_model)
        FROM schroders_sector_model_weights GROUP BY 1""")
    prod_n = dict(cur.fetchall())

    cur.execute("""
        SELECT company_name, ticker FROM fmp_performance_metrics
        WHERE ticker IS NOT NULL AND ticker <> ''""")
    tick_of = {cn: t for cn, t in cur.fetchall()}

    print('loading per-window models from DB...')
    windows = load_windows_from_db(cur)
    print(f'{len(windows)} windows loaded')

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

    # prices / quarterly returns (same as dashboard payload)
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

    print('scoring variants...')
    quarts = {'C': {}, 'A': {}, 'W': {}}
    for s in snapshots:
        st = states[s]
        quarts['C'][s] = score_snapshot(st, bucket_of, prod_model)
        quarts['A'][s] = score_snapshot(st, bucket_of, model_A)
        mw = model_W(s)
        quarts['W'][s] = score_snapshot(st, bucket_of, mw) if mw else {}

    # common start: first snapshot with >=3 priced members per quartile in ALL variants
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
    years = len(used) / 4.0
    print(f'common start {used[0]}, {len(used)} formation quarters')

    results = {'meta': {
        'run_date': date.today().isoformat(),
        'common_start': used[0].isoformat(),
        'n_formation_quarters': len(used),
        'conf_convention': 'sum over estimation set of company total scored reviews as of window end',
        'split_rule': 'formation snapshots < 2020-12-31 vs >= 2020-12-31',
        'turnover_definition': 'one-way: |names added to Q1| / |Q1|, averaged over rebalances',
        'windows_used': [we.isoformat() for we in sorted(windows)],
        'variant_a_caveat': ('Variant A is NOT free of look-ahead: every '
                             'evaluation quarter lies inside at least one '
                             'estimation window that fed the average. It is a '
                             'robustness comparison, not the honest benchmark; '
                             'Variant W is the walk-forward (honest) variant.'),
    }, 'variants': {}}

    series_for_chart = {}
    rolling_for_chart = {}
    for v, label in (('C', 'Variant C — current weights'),
                     ('A', 'Variant A — averaged windows'),
                     ('W', 'Variant W — walk-forward')):
        per_q, bench, counts, q1_names = portfolio_series(quarts[v], tick_of, qret, used)
        vd = {'label': label, 'headline': [], 'split': {}, 'rolling_3y_spread': [],
              'turnover_q1_oneway_avg': None}
        for q in (1, 2, 3, 4):
            cum, ann = stats_block(per_q[q], years)
            vd['headline'].append({'portfolio': f'Q{q}', 'cumulative_pct': cum,
                                   'annualized_pct': ann,
                                   'avg_companies': round(float(np.mean(counts[q])), 1)})
        cum_b, ann_b = stats_block(bench, years)
        sp_rets = [a - b for a, b in zip(per_q[1], per_q[4])]
        cum1, ann1 = stats_block(per_q[1], years)
        cum4, ann4 = stats_block(per_q[4], years)
        vd['headline'].append({'portfolio': 'Benchmark', 'cumulative_pct': cum_b,
                               'annualized_pct': ann_b, 'avg_companies': None})
        vd['headline'].append({'portfolio': 'Q1-Q4 spread',
                               'cumulative_pct': round(cum1 - cum4, 1),
                               'annualized_pct': round(ann1 - ann4, 2),
                               'avg_companies': None})
        # split periods
        for name, sel in (('2016-2020', [i for i, s in enumerate(used) if s < SPLIT_DATE]),
                          ('2021-present', [i for i, s in enumerate(used) if s >= SPLIT_DATE])):
            if not sel:
                continue
            yrs_p = len(sel) / 4.0
            block = {}
            for q in (1, 2, 3, 4):
                cum, ann = stats_block([per_q[q][i] for i in sel], yrs_p)
                block[f'Q{q}'] = {'cumulative_pct': cum, 'annualized_pct': ann}
            cumb, annb = stats_block([bench[i] for i in sel], yrs_p)
            block['Benchmark'] = {'cumulative_pct': cumb, 'annualized_pct': annb}
            block['Q1-Q4 spread'] = {
                'cumulative_pct': round(block['Q1']['cumulative_pct'] - block['Q4']['cumulative_pct'], 1),
                'annualized_pct': round(block['Q1']['annualized_pct'] - block['Q4']['annualized_pct'], 2)}
            vd['split'][name] = block
        # rolling 3y spread (12 formation quarters)
        for i in range(11, len(used)):
            win = sp_rets[i - 11:i + 1]
            cum = float(np.prod([1 + r for r in win]))
            vd['rolling_3y_spread'].append(
                {'through': used[i].isoformat(),
                 'spread_ann_pct': round((cum ** (1 / 3.0) - 1) * 100, 2)})
        # turnover
        tos = []
        for prev, curr in zip(q1_names, q1_names[1:]):
            if curr:
                tos.append(len(curr - prev) / len(curr))
        vd['turnover_q1_oneway_avg'] = round(float(np.mean(tos)) * 100, 1) if tos else None
        # cumulative series for charts
        def cumseries(rets):
            out, c0 = [100.0], 100.0
            for r in rets:
                c0 *= 1 + r
                out.append(c0)
            return out
        series_for_chart[v] = {'Q1': cumseries(per_q[1]), 'Q4': cumseries(per_q[4]),
                               'Benchmark': cumseries(bench), 'label': label}
        rolling_for_chart[v] = vd['rolling_3y_spread']
        results['variants'][v] = vd
        print(f'  {v}: spread ann {vd["headline"][-1]["annualized_pct"]}pp, '
              f'turnover {vd["turnover_q1_oneway_avg"]}%')

    # ---- Variant W weight-stability diagnostics (global bucket)
    wstab = []
    prev = None
    for we in sorted(windows):
        m = average_models(windows, buckets_all, upto=we)['global']
        row = {'window_end': we.isoformat(),
               'coefs': {d: round(float(m['coef'][j]), 4) for j, d in enumerate(DIMS)}}
        if prev is not None:
            row['mean_abs_coef_change'] = round(float(
                np.mean(np.abs(m['coef'] - prev))), 4)
        wstab.append(row)
        prev = m['coef']
    results['walk_forward_global_weight_stability'] = wstab

    # ---- per-window estimation log + thinness flags
    cur.execute("""
        SELECT window_end, bucket, max(n_est), max(alpha), max(r2), max(cv_r2), max(conf)
        FROM schroders_rolling_window_weights
        GROUP BY 1, 2 ORDER BY 1, 2""")
    wlog = []
    for we, b, n, a, r2, cv, cf in cur.fetchall():
        ref = prod_n.get(b if b != 'global' else 'global') or prod_n.get('global')
        wlog.append({'window_end': we.isoformat(), 'bucket': b, 'n_est': n,
                     'alpha': a, 'r2': round(r2, 3), 'cv_r2': round(cv, 3),
                     'conf': cf,
                     'thin_vs_current': bool(ref and n < 0.6 * ref)})
    results['window_log'] = wlog

    with open(os.path.join(OUT_DIR, 'results.json'), 'w') as f:
        json.dump(results, f, indent=1)

    # ---- CSVs
    with open(os.path.join(OUT_DIR, 'headline_stats.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['variant', 'portfolio', 'cumulative_pct', 'annualized_pct', 'avg_companies'])
        for v, vd in results['variants'].items():
            for row in vd['headline']:
                w.writerow([v, row['portfolio'], row['cumulative_pct'],
                            row['annualized_pct'], row['avg_companies']])
    with open(os.path.join(OUT_DIR, 'split_period_stats.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['variant', 'period', 'portfolio', 'cumulative_pct', 'annualized_pct'])
        for v, vd in results['variants'].items():
            for p, block in vd['split'].items():
                for port, st in block.items():
                    w.writerow([v, p, port, st['cumulative_pct'], st['annualized_pct']])
    with open(os.path.join(OUT_DIR, 'window_log.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['window_end', 'bucket', 'n_est', 'alpha', 'r2', 'cv_r2',
                    'conf', 'thin_vs_current'])
        for r in wlog:
            w.writerow([r['window_end'], r['bucket'], r['n_est'], r['alpha'],
                        r['r2'], r['cv_r2'], r['conf'], r['thin_vs_current']])
    with open(os.path.join(OUT_DIR, 'rolling_3y_spread.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['variant', 'through', 'spread_ann_pct'])
        for v, rows in rolling_for_chart.items():
            for r in rows:
                w.writerow([v, r['through'], r['spread_ann_pct']])

    # ---- charts
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    labels = [s.isoformat() for s in used] + ['end']
    xs = list(range(len(used) + 1))
    fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharey=True)
    for ax, v in zip(axes, ('C', 'A', 'W')):
        sc = series_for_chart[v]
        ax.plot(xs, sc['Q1'], label='Q1 (strong culture)', color='#2563eb')
        ax.plot(xs, sc['Q4'], label='Q4 (weak culture)', color='#dc2626')
        ax.plot(xs, sc['Benchmark'], label='Benchmark', color='#6b7280', ls='--')
        ax.set_title(sc['label'], fontsize=10)
        ax.set_yscale('log')
        step = max(1, len(xs) // 6)
        ax.set_xticks(xs[::step])
        ax.set_xticklabels([labels[i][:7] for i in xs[::step]], rotation=45, fontsize=7)
        ax.grid(alpha=0.3)
    axes[0].set_ylabel('Cumulative (start=100, log scale)')
    axes[0].legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, 'cumulative_returns.png'), dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(11, 5))
    for v, col in (('C', '#2563eb'), ('A', '#059669'), ('W', '#dc2626')):
        rows = rolling_for_chart[v]
        ax.plot([r['through'][:7] for r in rows],
                [r['spread_ann_pct'] for r in rows],
                label=series_for_chart[v]['label'], color=col)
    ax.axhline(0, color='black', lw=0.8)
    ax.set_ylabel('Rolling 3-yr Q1−Q4 spread (annualised, pp)')
    ticks = ax.get_xticks()
    ax.set_xticks(ticks[::max(1, len(ticks) // 8)])
    plt.setp(ax.get_xticklabels(), rotation=45, fontsize=8)
    ax.legend(fontsize=9); ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, 'rolling_3y_spread.png'), dpi=130)
    plt.close(fig)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8), sharex=True)
    wes = [r['window_end'][:4] for r in wstab]
    for j, d in enumerate(DIMS):
        ax1.plot(wes, [r['coefs'][d] for r in wstab], label=d, lw=1.2)
    ax1.set_ylabel('Walk-forward global coefficient')
    ax1.legend(fontsize=6, ncol=4)
    ax1.grid(alpha=0.3)
    ax2.bar(wes[1:], [r.get('mean_abs_coef_change', 0) for r in wstab[1:]],
            color='#2563eb')
    ax2.set_ylabel('Mean |Δcoef| vs prior year')
    ax2.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, 'weight_stability_W.png'), dpi=130)
    plt.close(fig)

    c.close()
    print(f'outputs written to {OUT_DIR}')


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'run'
    if cmd == 'fundamentals':
        fetch_fundamentals()
    elif cmd == 'earlyprices':
        fetch_early_prices()
    elif cmd == 'run':
        build_pit()
        build_windows()
        run_variants()
    elif cmd == 'pit':
        build_pit()
    elif cmd == 'windows':
        build_windows()
    elif cmd == 'variants':
        run_variants()
    else:
        print('unknown command', cmd)
