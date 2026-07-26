"""Schroders sector-relative factor rebuild (reviewer brief 2026-07-21).

Implements:
  §5  company x dimension evidence metrics
  §6  evidence tiers A-D
  §7  sector-relative framework with configurable fallback hierarchy
  §8  sector-relative shrinkage (k_j configurable, default 50)
  §9  sector ridge model on shrunk Tier A/B predictors vs composite target
  §10 single sector-relative factor score (raw / z / percentile)
  §11 reliability tiers (High/Medium/Low/Insufficient)
  §12 concentration risk checks + configurable downgrade rule
  §13 overlap / multicollinearity gates (|r|>0.75, VIF>4/5/10)
  §14 bootstrap rank-stability diagnostics
  §15 output tables
  §17 run logging

Usage:
    python pipeline/factor_build.py settings    # create/refresh settings table
    python pipeline/factor_build.py termscan    # resumable per-company term scan
    python pipeline/factor_build.py evidence    # §5-§6 -> schroders_company_dimension_evidence
    python pipeline/factor_build.py shrink      # §7-§8 shrunk scores
    python pipeline/factor_build.py model       # §9-§14 model, factor, reliability, overlap, bootstrap
    python pipeline/factor_build.py all         # evidence -> shrink -> model
"""

import json
import math
import os
import pickle
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

import numpy as np
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schroders_v2_keywords import (  # noqa: E402
    SCHRODERS_V2_KEYWORDS, SCHRODERS_V2_DIMENSIONS, DICTIONARY_VERSION,
)
from scoring_engine_v2 import (  # noqa: E402
    compile_phrase, find_matches_with_negation, _TokenIndex,
    SCORING_ENGINE_VERSION,
)

DIMS = SCHRODERS_V2_DIMENSIONS
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   'pipeline_output')
TERM_STATE = os.path.join(OUT, 'termscan_state.json')
TERM_ACC = os.path.join(OUT, 'termscan_acc.pkl')
RUN_LOG = os.path.join(OUT, 'factor_run_log.json')

SNAPSHOT_TAG = 'pre_schroders_sector_relative_rebuild_2026_07_21'

DEFAULT_SETTINGS = {
    'shrinkage_k': {d: 50 for d in DIMS},            # §8
    'peer_hierarchy': ['gics_sub_industry', 'gics_industry', 'gics_sector'],  # §7
    'min_companies_per_bucket': 8,                    # fallback trigger
    'min_companies_model': 8,                         # §9 sector model minimum
    'coef_shrink_m': 10,                              # sector->global coef shrinkage
    'ridge_alphas': [0.1, 0.3, 1.0, 3.0, 10.0, 30.0],
    # §12 — reviews use the brief's 30%; terms use a higher default because
    # with 125-158 terms per dimension the top-5 terms naturally carry ~60%
    # of weighted matches corpus-wide (documented deviation, configurable).
    'concentration_share_threshold': 0.30,            # top-5 reviews share
    'concentration_term_share_threshold': 0.80,       # top-5 terms share
    'concentration_max_flagged_dims': 3,              # downgrade if more than this
    'corr_flag_threshold': 0.75,                      # §13
    'vif_flag': 4.0, 'vif_warn': 5.0, 'vif_fail': 10.0,
    'bootstrap_reps': 200,                            # §14
    'rank_pctile_sd_downgrade': 15.0,                 # instability downgrade
    'min_dims_high': 8, 'min_dims_medium': 6,         # §11
    'reliability_min_peer_count': 8,
}


def conn():
    return psycopg2.connect(os.environ['DATABASE_URL'])


# ---------------------------------------------------------------- settings
def ensure_settings(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schroders_factor_settings (
            key TEXT PRIMARY KEY,
            value JSONB NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now())""")
    for k, v in DEFAULT_SETTINGS.items():
        cur.execute("""
            INSERT INTO schroders_factor_settings (key, value)
            VALUES (%s, %s) ON CONFLICT (key) DO NOTHING""", (k, json.dumps(v)))


def load_settings(cur):
    ensure_settings(cur)
    cur.execute("SELECT key, value FROM schroders_factor_settings")
    s = dict(DEFAULT_SETTINGS)
    s.update({k: v for k, v in cur.fetchall()})
    return s


# ---------------------------------------------------------------- term scan
def termscan():
    """Resumable scan: per company x dimension, weighted contribution per term."""
    terms = []
    for dim in DIMS:
        for pole in ('positive', 'negative'):
            for t, w in SCHRODERS_V2_KEYWORDS[dim][pole].items():
                terms.append((dim, pole, t, float(w), compile_phrase(t)))
    state = {'last_id': 0}
    acc = defaultdict(lambda: defaultdict(float))  # (company,dim) -> term -> contrib
    if os.path.exists(TERM_STATE):
        with open(TERM_STATE) as f:
            state = json.load(f)
        with open(TERM_ACC, 'rb') as f:
            raw = pickle.load(f)
        for k, v in raw.items():
            acc[k] = defaultdict(float, v)
    c = conn(); cur = c.cursor()
    print(f'{len(terms)} terms; resuming from id {state["last_id"]}')
    batch = 4000
    while True:
        cur.execute("""
            SELECT id, company_name,
                   COALESCE(summary,'') || '. ' || COALESCE(pros,'') || '. ' ||
                   COALESCE(cons,'') || '. ' || COALESCE(advice_to_management,'')
            FROM reviews WHERE id > %s ORDER BY id LIMIT %s""",
                    (state['last_id'], batch))
        rows = cur.fetchall()
        if not rows:
            break
        for rid, comp, text in rows:
            tl = (text or '').lower()
            if not tl.strip('. '):
                continue
            ti = _TokenIndex(tl)
            for dim, pole, t, w, pat in terms:
                plain, negd = find_matches_with_negation(tl, pat, ti, 5)
                n = plain + negd
                if n:
                    acc[(comp, dim)][t] += w * n
        state['last_id'] = rows[-1][0]
        with open(TERM_ACC, 'wb') as f:
            pickle.dump({k: dict(v) for k, v in acc.items()}, f)
        with open(TERM_STATE, 'w') as f:
            json.dump(state, f)
        print(f'  termscan at id {state["last_id"]}')
    c.close()
    print('termscan complete')


# ---------------------------------------------------------------- peer map
def peer_map(cur, settings):
    """company -> {gics_sector, gics_industry, gics_sub_industry} with fallback."""
    cur.execute("""
        SELECT glassdoor_name, gics_sector, gics_industry, gics_sub_industry
        FROM extraction_queue WHERE glassdoor_name IS NOT NULL""")
    q = {r[0]: {'gics_sector': r[1], 'gics_industry': r[2],
                'gics_sub_industry': r[3]} for r in cur.fetchall()}
    cur.execute("SELECT company_name FROM company_culture_scores_v2")
    out = {}
    for (comp,) in cur.fetchall():
        info = q.get(comp)
        if info is None:
            info = {}
        out[comp] = {
            'gics_sector': info.get('gics_sector') or 'Asset Management/Other',
            'gics_industry': info.get('gics_industry') or 'Asset Management/Other',
            'gics_sub_industry': (info.get('gics_sub_industry')
                                  or 'Asset Management/Other'),
        }
    return out


def pick_bucket(companies_info, settings):
    """Per-company peer assignment (§7, per-group variant).

    Greedy, finest-first: at each level of the hierarchy, companies whose
    remaining group has >= min_companies_per_bucket members are assigned a
    bucket at that level; the rest fall through to the next (coarser) level.
    Companies whose sector-level remainder is still too small go to 'global'.
    Buckets are disjoint and every non-global bucket has >= min_n members.

    DELIBERATE DESIGN CHOICE — residual counts, not full-group counts:
    counts at each level are taken over the companies still unassigned at
    that level. Full-group counts would let a company claim an industry
    bucket whose other members were already assigned at sub-industry level,
    leaving the actual ranking peer set below min_n. Residual counting
    trades a coarser assignment for a guaranteed adequate peer set, which
    the ranking (z / percentile) step requires.

    Returns (buckets: {company: bucket_name},
             bucket_level: {bucket_name: level_name}).
    """
    hierarchy = settings['peer_hierarchy']
    min_n = settings['min_companies_per_bucket']
    buckets, bucket_level = {}, {}
    remaining = set(companies_info)
    for level in hierarchy:
        counts = defaultdict(int)
        for comp in remaining:
            counts[companies_info[comp].get(level) or 'Unknown'] += 1
        for comp in sorted(remaining):
            val = companies_info[comp].get(level) or 'Unknown'
            if counts[val] < min_n:
                continue
            name = val
            # disambiguate on the rare GICS name collision across levels
            if name in bucket_level and bucket_level[name] != level:
                name = f'{val} ({level.replace("gics_", "").replace("_", "-")})'
            buckets[comp] = name
            bucket_level[name] = level
        remaining -= set(buckets)
    for comp in remaining:
        buckets[comp] = 'global'
    if remaining:
        bucket_level['global'] = 'global'
    return buckets, bucket_level


# ---------------------------------------------------------------- evidence
def build_evidence():
    c = conn(); cur = c.cursor()
    settings = load_settings(cur)
    pm = peer_map(cur, settings)

    cur.execute("""
        DROP TABLE IF EXISTS schroders_company_dimension_evidence;
        CREATE TABLE schroders_company_dimension_evidence (
            company_name TEXT NOT NULL,
            gics_sector TEXT, gics_industry TEXT, gics_sub_industry TEXT,
            dimension TEXT NOT NULL,
            total_reviews_company INT,
            n_scored_reviews_dimension INT,
            share_reviews_scored_dimension DOUBLE PRECISION,
            mean_dimension_score_raw DOUBLE PRECISION,
            std_dimension_score_raw DOUBLE PRECISION,
            se_dimension_score_raw DOUBLE PRECISION,
            n_positive_reviews_dimension INT,
            n_negative_reviews_dimension INT,
            top_5_terms_contribution_share DOUBLE PRECISION,
            top_5_reviews_contribution_share DOUBLE PRECISION,
            evidence_tier_dimension TEXT,
            shrinkage_k_dimension DOUBLE PRECISION,
            shrinkage_weight_dimension DOUBLE PRECISION,
            sector_dim_mean DOUBLE PRECISION,
            mean_dimension_score_shrunk DOUBLE PRECISION,
            mean_dimension_score_shrunk_internal DOUBLE PRECISION,
            dictionary_version TEXT, scoring_engine_version TEXT,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (company_name, dimension))""")

    term_acc = {}
    if os.path.exists(TERM_ACC):
        with open(TERM_ACC, 'rb') as f:
            term_acc = pickle.load(f)

    rows_out = []
    cur.execute("""SELECT company_name, count(*) FROM review_culture_scores
                   WHERE company_name IS NOT NULL GROUP BY 1""")
    totals = dict(cur.fetchall())
    for d in DIMS:
        cur.execute(f"""
            SELECT company_name,
                   count(schroders_v2_{d}_score),
                   avg(schroders_v2_{d}_score),
                   stddev_samp(schroders_v2_{d}_score),
                   count(*) FILTER (WHERE schroders_v2_{d}_score > 0),
                   count(*) FILTER (WHERE schroders_v2_{d}_score < 0),
                   array_agg(schroders_v2_{d}_evidence
                             ORDER BY schroders_v2_{d}_evidence DESC)
                       FILTER (WHERE schroders_v2_{d}_score IS NOT NULL)
            FROM review_culture_scores
            WHERE company_name IS NOT NULL GROUP BY company_name""")
        for comp, n, mean, std, npos, nneg, evs in cur.fetchall():
            total = totals.get(comp, 0)
            info = pm.get(comp) or {
                'gics_sector': 'Asset Management/Other',
                'gics_industry': 'Asset Management/Other',
                'gics_sub_industry': 'Asset Management/Other'}
            # tiers (§6) — Tier D rule takes precedence
            if n < 5:
                tier = 'D'
            elif n >= 50 and total >= 150:
                tier = 'A'
            elif (20 <= n < 50) or (50 <= total < 150):
                tier = 'B'
            else:
                tier = 'C'
            se = (std / math.sqrt(n)) if (std is not None and n) else None
            # top-5 review concentration
            r5 = None
            if evs:
                tot_ev = sum(evs)
                if tot_ev > 0:
                    r5 = sum(evs[:5]) / tot_ev
            # top-5 term concentration
            t5 = None
            contribs = term_acc.get((comp, d))
            if contribs:
                vals = sorted(contribs.values(), reverse=True)
                tot = sum(vals)
                if tot > 0:
                    t5 = sum(vals[:5]) / tot
            rows_out.append((
                comp, info.get('gics_sector'), info.get('gics_industry'),
                info.get('gics_sub_industry'), d, total, n,
                (n / total) if total else None,
                float(mean) if mean is not None else None,
                float(std) if std is not None else None,
                float(se) if se is not None else None,
                npos, nneg, t5, r5, tier,
                DICTIONARY_VERSION, SCORING_ENGINE_VERSION))
    execute_values(cur, """
        INSERT INTO schroders_company_dimension_evidence
            (company_name, gics_sector, gics_industry, gics_sub_industry,
             dimension, total_reviews_company, n_scored_reviews_dimension,
             share_reviews_scored_dimension, mean_dimension_score_raw,
             std_dimension_score_raw, se_dimension_score_raw,
             n_positive_reviews_dimension, n_negative_reviews_dimension,
             top_5_terms_contribution_share, top_5_reviews_contribution_share,
             evidence_tier_dimension, dictionary_version, scoring_engine_version)
        VALUES %s""", rows_out)
    c.commit()
    print(f'evidence rows: {len(rows_out)}')
    c.close()


# ---------------------------------------------------------------- shrinkage
def build_shrinkage():
    c = conn(); cur = c.cursor()
    settings = load_settings(cur)
    cur.execute("""
        SELECT company_name, gics_sector, gics_industry, gics_sub_industry,
               dimension, n_scored_reviews_dimension, mean_dimension_score_raw,
               evidence_tier_dimension
        FROM schroders_company_dimension_evidence""")
    rows = cur.fetchall()
    info = {r[0]: {'gics_sector': r[1], 'gics_industry': r[2],
                   'gics_sub_industry': r[3]} for r in rows}
    buckets, bucket_level = pick_bucket(info, settings)
    lvl_counts = defaultdict(int)
    for comp in buckets:
        lvl_counts[bucket_level[buckets[comp]]] += 1
    print(f'peer classification (per-group): {dict(lvl_counts)}')

    # sector prior means per bucket x dim (companies with any mentions)
    by_bd = defaultdict(list)
    for comp, *_rest in rows:
        pass
    for comp, sec, ind, sub, d, n, raw, tier in rows:
        if raw is not None and n and n > 0:
            by_bd[(buckets[comp], d)].append(raw)
    prior = {k: float(np.mean(v)) for k, v in by_bd.items()}

    k_map = settings['shrinkage_k']
    upd = []
    for comp, sec, ind, sub, d, n, raw, tier in rows:
        k = float(k_map.get(d, 50))
        pmean = prior.get((buckets[comp], d))
        if raw is None or not n:
            w = 0.0
            shrunk = pmean
        else:
            w = n / (n + k)
            shrunk = w * raw + (1 - w) * (pmean if pmean is not None else 0.0)
        published = None if tier == 'D' else shrunk   # §8 rules
        upd.append((k, w, pmean, published, shrunk, comp, d))
    cur.executemany("""
        UPDATE schroders_company_dimension_evidence
        SET shrinkage_k_dimension=%s, shrinkage_weight_dimension=%s,
            sector_dim_mean=%s, mean_dimension_score_shrunk=%s,
            mean_dimension_score_shrunk_internal=%s, updated_at=now()
        WHERE company_name=%s AND dimension=%s""", upd)
    c.commit()
    c.close()
    return buckets, bucket_level


# ---------------------------------------------------------------- model
def perf_targets(cur):
    """Composite performance target — unchanged definition (existing weights)."""
    cur.execute("""
        SELECT e.company_name, f.roe_5y_avg, f.revenue_growth_5y, f.tsr_5y,
               f.op_margin_5y_avg
        FROM (SELECT DISTINCT company_name FROM schroders_company_dimension_evidence) e
        JOIN fmp_performance_metrics f
          ON lower(e.company_name) = lower(f.company_name)""")
    raw = {r[0]: r[1:] for r in cur.fetchall()}
    # z-score each metric across available companies, weight 0.30/0.25/0.25/0.20
    cols = list(zip(*raw.values())) if raw else []
    stats = []
    for j in range(4):
        vals = [v for v in cols[j] if v is not None] if cols else []
        stats.append((float(np.mean(vals)), float(np.std(vals)))
                     if len(vals) >= 3 and np.std(vals) > 0 else None)
    weights = [0.30, 0.25, 0.25, 0.20]
    target = {}
    for comp, vals in raw.items():
        num = den = 0.0
        for j, v in enumerate(vals):
            if v is not None and stats[j]:
                z = max(-2, min(2, (v - stats[j][0]) / stats[j][1]))
                num += weights[j] * z
                den += weights[j]
        if den > 0:
            target[comp] = num / den
    return target


def _ridge_fit(X, y, alphas, seed=0):
    """Ridge with leave-one-out CV over alphas. X standardized. Returns
    (coefs, intercept, alpha, r2_in, cv_r2)."""
    n = len(y)
    best = None
    for a in alphas:
        errs = []
        for i in range(n):
            m = np.ones(n, bool); m[i] = False
            b = _ridge_solve(X[m], y[m], a)
            pred = X[i] @ b[1:] + b[0]
            errs.append((y[i] - pred) ** 2)
        mse = float(np.mean(errs))
        if best is None or mse < best[0]:
            best = (mse, a)
    alpha = best[1]
    b = _ridge_solve(X, y, alpha)
    pred = X @ b[1:] + b[0]
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1 - ss_res / ss_tot if ss_tot else 0.0
    cv_r2 = 1 - best[0] * n / ss_tot if ss_tot else 0.0
    return b[1:], b[0], alpha, r2, cv_r2


def _ridge_solve(X, y, alpha):
    n, p = X.shape
    X1 = np.column_stack([np.ones(n), X])
    P = alpha * np.eye(p + 1); P[0, 0] = 0.0
    return np.linalg.solve(X1.T @ X1 + P, X1.T @ y)


def build_model():
    c = conn(); cur = c.cursor()
    settings = load_settings(cur)
    buckets, bucket_level = build_shrinkage()   # ensures shrunk scores fresh
    target = perf_targets(cur)

    cur.execute("""
        SELECT company_name, dimension, evidence_tier_dimension,
               mean_dimension_score_shrunk_internal,
               top_5_terms_contribution_share, top_5_reviews_contribution_share
        FROM schroders_company_dimension_evidence""")
    ev = defaultdict(dict)
    conc = defaultdict(dict)
    tiers = defaultdict(dict)
    for comp, d, tier, shr, t5, r5 in cur.fetchall():
        ev[comp][d] = shr
        tiers[comp][d] = tier
        conc[comp][d] = (t5, r5)

    companies = sorted(ev)
    ab_count = {comp: sum(1 for d in DIMS if tiers[comp].get(d) in ('A', 'B'))
                for comp in companies}

    # ---- model estimation per bucket (§9)
    min_model = settings['min_companies_model']
    min_dims = settings['min_dims_medium']
    alphas = settings['ridge_alphas']
    m_shrink = settings['coef_shrink_m']

    def model_rows(comps):
        """Companies usable for estimation: target + >=min_dims A/B dims."""
        out = []
        for comp in comps:
            if comp not in target or ab_count[comp] < min_dims:
                continue
            out.append(comp)
        return out

    def design(comps, mu=None, sd=None):
        M = np.array([[ev[comp][d] if ev[comp][d] is not None else np.nan
                       for d in DIMS] for comp in comps], dtype=float)
        col_mean = np.nanmean(M, axis=0)
        inds = np.where(np.isnan(M))
        M[inds] = np.take(col_mean, inds[1])
        if mu is None:
            mu = M.mean(0)
            sd = M.std(0)
            sd[sd == 0] = 1.0
        return (M - mu) / sd, mu, sd

    # global model first (fallback anchor)
    g_comps = model_rows(companies)
    exclusions = {comp: ('no performance target' if comp not in target
                         else f'only {ab_count[comp]} Tier A/B dimensions')
                  for comp in companies if comp not in g_comps}
    Xg, mu_g, sd_g = design(g_comps)
    yg = np.array([target[comp] for comp in g_comps])
    coef_g, int_g, alpha_g, r2_g, cv_g = _ridge_fit(Xg, yg, alphas)

    bucket_names = sorted(set(buckets.values()))
    cur.execute("""
        DROP TABLE IF EXISTS schroders_sector_model_weights;
        CREATE TABLE schroders_sector_model_weights (
            peer_bucket TEXT, classification_level TEXT,
            sector_model_level_used TEXT,
            n_companies_model INT,
            ridge_alpha_selected DOUBLE PRECISION,
            model_r2_in_sample DOUBLE PRECISION,
            cross_validated_r2 DOUBLE PRECISION,
            coef_shrink_lambda DOUBLE PRECISION,
            dimension TEXT,
            dimension_weight_final DOUBLE PRECISION,
            predictor_mean DOUBLE PRECISION, predictor_std DOUBLE PRECISION,
            coefficient_stability_sd DOUBLE PRECISION,
            updated_at TIMESTAMPTZ DEFAULT now())""")

    bucket_model = {}
    rng = np.random.default_rng(7)
    reps = settings['bootstrap_reps']
    for b in bucket_names:
        comps_b = [comp for comp in companies if buckets[comp] == b]
        est = model_rows(comps_b)
        if b == 'global' or len(est) < min_model:
            used = 'global_fallback' if b != 'global' else 'global'
            coef, mu, sd = coef_g, mu_g, sd_g
            alpha, r2, cv, nm, lam = alpha_g, r2_g, cv_g, len(g_comps), 0.0
            Xb, yb = Xg, yg
        else:
            Xb, mu, sd = design(est)
            yb = np.array([target[comp] for comp in est])
            coef_s, _, alpha, r2, cv = _ridge_fit(Xb, yb, alphas)
            lam = len(est) / (len(est) + m_shrink)
            coef = lam * coef_s + (1 - lam) * coef_g   # hierarchical shrink §9
            used, nm = bucket_level[b], len(est)
        # coefficient stability via bootstrap over estimation set
        coefs_bs = []
        for _ in range(min(reps, 100)):
            idx = rng.integers(0, len(Xb), len(Xb))
            if len(set(idx.tolist())) < 3:
                continue
            try:
                bcoef = _ridge_solve(Xb[idx], yb[idx], alpha)[1:]
                coefs_bs.append(bcoef)
            except np.linalg.LinAlgError:
                continue
        coef_sd = np.std(coefs_bs, axis=0) if coefs_bs else [None] * len(DIMS)
        bucket_model[b] = dict(coef=coef, mu=mu, sd=sd, used=used, n=nm,
                               alpha=alpha, r2=r2, cv=cv)
        rows = [(b, bucket_level[b], used, nm, alpha, r2, cv, lam, d, float(coef[j]),
                 float(mu[j]), float(sd[j]),
                 float(coef_sd[j]) if coef_sd[j] is not None else None)
                for j, d in enumerate(DIMS)]
        execute_values(cur, """
            INSERT INTO schroders_sector_model_weights
                (peer_bucket, classification_level, sector_model_level_used,
                 n_companies_model, ridge_alpha_selected, model_r2_in_sample,
                 cross_validated_r2, coef_shrink_lambda, dimension,
                 dimension_weight_final, predictor_mean, predictor_std,
                 coefficient_stability_sd)
            VALUES %s""", rows)

    # ---- factor scores (§10)
    def factor_raw(comp, model):
        x = []
        for j, d in enumerate(DIMS):
            v = ev[comp][d]
            if v is None or tiers[comp][d] == 'D':
                v = model['mu'][j]          # neutral: sector mean
            x.append((v - model['mu'][j]) / model['sd'][j])
        return float(np.dot(model['coef'], x))

    raw_f = {comp: factor_raw(comp, bucket_model[buckets[comp]])
             for comp in companies}
    z_f, pct_f = {}, {}
    for b in bucket_names:
        comps_b = [comp for comp in companies if buckets[comp] == b]
        vals = np.array([raw_f[comp] for comp in comps_b])
        mu, sd = vals.mean(), vals.std() if vals.std() > 0 else 1.0
        order = vals.argsort().argsort()
        for i, comp in enumerate(comps_b):
            z_f[comp] = (raw_f[comp] - mu) / sd
            pct_f[comp] = 100.0 * (order[i] + 0.5) / len(comps_b)

    # ---- concentration flags (§12)
    thr = settings['concentration_share_threshold']
    thr_t = settings.get('concentration_term_share_threshold', 0.80)
    max_fl = settings['concentration_max_flagged_dims']
    conc_flags = {}
    for comp in companies:
        flagged = 0
        for d in DIMS:
            if tiers[comp][d] in ('A', 'B'):
                t5, r5 = conc[comp][d]
                if (t5 is not None and t5 > thr_t) or (r5 is not None and r5 > thr):
                    flagged += 1
        conc_flags[comp] = flagged
    severe_conc = {comp: conc_flags[comp] > max_fl for comp in companies}

    # ---- bootstrap rank stability (§14)
    stab = {comp: dict(ranks=[], pcts=[], top=0, bot=0, reps=0) for comp in companies}
    for b in bucket_names:
        comps_b = [comp for comp in companies if buckets[comp] == b]
        est = model_rows(comps_b)
        m = bucket_model[b]
        for _ in range(reps):
            if m['used'] not in ('global', 'global_fallback') and len(est) >= min_model:
                idx = rng.integers(0, len(est), len(est))
                sub = [est[i] for i in idx]
                try:
                    Xb, mu_b, sd_b = design(sub)
                    yb = np.array([target[comp] for comp in sub])
                    coef_b = _ridge_solve(Xb, yb, m['alpha'])[1:]
                    lam = len(est) / (len(est) + m_shrink)
                    coef_b = lam * coef_b + (1 - lam) * coef_g
                    mm = dict(coef=coef_b, mu=mu_b, sd=sd_b)
                except np.linalg.LinAlgError:
                    continue
            else:
                idx = rng.integers(0, len(g_comps), len(g_comps))
                sub = [g_comps[i] for i in idx]
                try:
                    Xb, mu_b, sd_b = design(sub)
                    yb = np.array([target[comp] for comp in sub])
                    coef_b = _ridge_solve(Xb, yb, m['alpha'])[1:]
                    mm = dict(coef=coef_b, mu=mu_b, sd=sd_b)
                except np.linalg.LinAlgError:
                    continue
            vals = np.array([factor_raw(comp, mm) for comp in comps_b])
            order = vals.argsort().argsort()
            nb = len(comps_b)
            for i, comp in enumerate(comps_b):
                rank = nb - order[i]                    # 1 = best
                pct = 100.0 * (order[i] + 0.5) / nb
                st = stab[comp]
                st['ranks'].append(rank); st['pcts'].append(pct); st['reps'] += 1
                if pct >= 80: st['top'] += 1
                if pct <= 20: st['bot'] += 1

    # ---- reliability tiers (§11 + §12 + §14 downgrades)
    order_t = ['High', 'Medium', 'Low', 'Insufficient']
    def downgrade(t):
        return order_t[min(order_t.index(t) + 1, 3)]
    min_peer = settings['reliability_min_peer_count']
    sd_thr = settings['rank_pctile_sd_downgrade']
    rel = {}
    for comp in companies:
        b = buckets[comp]
        m = bucket_model[b]
        nb = sum(1 for c2 in companies if buckets[c2] == b)
        score_ok = raw_f.get(comp) is not None
        if not score_ok or ab_count[comp] < 4 or nb < 3:
            rel[comp] = 'Insufficient'
            continue
        if (ab_count[comp] >= settings['min_dims_high'] and not severe_conc[comp]
                and nb >= min_peer):
            t = 'High'
        elif ab_count[comp] >= settings['min_dims_medium'] and not severe_conc[comp]:
            t = 'Medium'
        else:
            t = 'Low'
        if severe_conc[comp] and t in ('High', 'Medium'):
            t = downgrade(t)
        pct_sd = float(np.std(stab[comp]['pcts'])) if stab[comp]['pcts'] else None
        if pct_sd is not None and pct_sd > sd_thr and t in ('High', 'Medium'):
            t = downgrade(t)
        rel[comp] = t

    cur.execute("""
        DROP TABLE IF EXISTS schroders_company_factor_scores;
        CREATE TABLE schroders_company_factor_scores (
            company_name TEXT PRIMARY KEY,
            peer_bucket TEXT, classification_level TEXT,
            sector_model_level_used TEXT,
            n_dims_tier_ab INT, n_dims_concentration_flagged INT,
            schroders_factor_raw DOUBLE PRECISION,
            schroders_factor_sector_z DOUBLE PRECISION,
            schroders_factor_sector_pctile DOUBLE PRECISION,
            schroders_factor_reliability_tier TEXT,
            bootstrap_mean_rank DOUBLE PRECISION,
            bootstrap_rank_sd DOUBLE PRECISION,
            bootstrap_pctile_sd DOUBLE PRECISION,
            top_quintile_frequency DOUBLE PRECISION,
            bottom_quintile_frequency DOUBLE PRECISION,
            updated_at TIMESTAMPTZ DEFAULT now())""")
    rows = []
    for comp in companies:
        st = stab[comp]
        rows.append((
            comp, buckets[comp], bucket_level[buckets[comp]],
            bucket_model[buckets[comp]]['used'],
            ab_count[comp], conc_flags[comp],
            raw_f[comp], float(z_f[comp]), float(pct_f[comp]), rel[comp],
            float(np.mean(st['ranks'])) if st['ranks'] else None,
            float(np.std(st['ranks'])) if st['ranks'] else None,
            float(np.std(st['pcts'])) if st['pcts'] else None,
            st['top'] / st['reps'] if st['reps'] else None,
            st['bot'] / st['reps'] if st['reps'] else None))
    execute_values(cur, """
        INSERT INTO schroders_company_factor_scores
            (company_name, peer_bucket, classification_level,
             sector_model_level_used, n_dims_tier_ab,
             n_dims_concentration_flagged, schroders_factor_raw,
             schroders_factor_sector_z, schroders_factor_sector_pctile,
             schroders_factor_reliability_tier, bootstrap_mean_rank,
             bootstrap_rank_sd, bootstrap_pctile_sd, top_quintile_frequency,
             bottom_quintile_frequency)
        VALUES %s""", rows)

    # ---- overlap diagnostics (§13)
    cur.execute("""
        DROP TABLE IF EXISTS schroders_overlap_diagnostics;
        CREATE TABLE schroders_overlap_diagnostics (
            peer_bucket TEXT, dimension TEXT,
            correlated_counterparts TEXT,
            max_abs_correlation DOUBLE PRECISION,
            vif DOUBLE PRECISION,
            flag TEXT, suggested_action TEXT,
            updated_at TIMESTAMPTZ DEFAULT now())""")
    diag_rows = []
    for b in bucket_names + ['ALL']:
        comps_b = (companies if b == 'ALL'
                   else [comp for comp in companies if buckets[comp] == b])
        if len(comps_b) < 6:
            continue
        M, _, _ = design(comps_b)
        C = np.corrcoef(M.T)
        for j, d in enumerate(DIMS):
            partners = [(DIMS[k], C[j, k]) for k in range(len(DIMS))
                        if k != j and abs(C[j, k]) > settings['corr_flag_threshold']]
            X = np.delete(M, j, axis=1)
            y = M[:, j]
            X1 = np.column_stack([np.ones(len(X)), X])
            beta, *_ = np.linalg.lstsq(X1, y, rcond=None)
            resid = y - X1 @ beta
            ss_tot = ((y - y.mean()) ** 2).sum()
            r2 = 1 - (resid ** 2).sum() / ss_tot if ss_tot else 0
            vif = 1 / (1 - r2) if r2 < 1 else float('inf')
            if vif > settings['vif_fail']:
                flag, action = 'FAIL', 'exclude-from-model or merge-review (remediation required)'
            elif vif > settings['vif_warn']:
                flag, action = 'STRONG_WARNING', 'downweight or merge-review'
            elif vif > settings['vif_flag'] or partners:
                flag, action = 'REVIEW', 'merge-review'
            else:
                flag, action = 'OK', 'keep'
            diag_rows.append((
                b, d,
                '; '.join(f'{p}:{v:.2f}' for p, v in partners),
                float(max(abs(C[j, k]) for k in range(len(DIMS)) if k != j)),
                float(vif) if math.isfinite(vif) else None, flag, action))
    execute_values(cur, """
        INSERT INTO schroders_overlap_diagnostics
            (peer_bucket, dimension, correlated_counterparts,
             max_abs_correlation, vif, flag, suggested_action)
        VALUES %s""", diag_rows)

    c.commit()

    # ---- run log (§17)
    log = {
        'run_at': datetime.now(timezone.utc).isoformat(),
        'dictionary_version': DICTIONARY_VERSION,
        'scoring_engine_version': SCORING_ENGINE_VERSION,
        'snapshot_tag': SNAPSHOT_TAG,
        'classification_level_used': 'per_group_mixed',
        'companies_per_classification_level': dict(Counter(
            bucket_level[buckets[comp]] for comp in companies)),
        'bucket_classification_levels': {b: bucket_level[b] for b in bucket_names},
        'bucket_model_levels': {b: bucket_model[b]['used'] for b in bucket_names},
        'n_companies_total': len(companies),
        'n_companies_in_global_model': len(g_comps),
        'excluded_from_estimation': exclusions,
        'shrinkage_k': settings['shrinkage_k'],
        'ridge_alpha_by_bucket': {b: bucket_model[b]['alpha'] for b in bucket_names},
        'concentration_rule': {
            'review_share_threshold': thr, 'term_share_threshold': thr_t,
            'max_flagged_dims': max_fl},
        'overlap_flags': {f'{r[0]}/{r[1]}': r[5] for r in diag_rows if r[5] != 'OK'},
        'bootstrap_reps': reps,
        'reliability_distribution': {t: sum(1 for v in rel.values() if v == t)
                                     for t in order_t},
    }
    with open(RUN_LOG, 'w') as f:
        json.dump(log, f, indent=2, default=str)
    print(json.dumps({k: log[k] for k in
                      ('classification_level_used', 'n_companies_in_global_model',
                       'reliability_distribution', 'ridge_alpha_by_bucket')},
                     indent=2))
    c.close()


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'all'
    if cmd == 'settings':
        c = conn(); cur = c.cursor(); ensure_settings(cur); c.commit(); c.close()
    elif cmd == 'termscan':
        termscan()
    elif cmd == 'evidence':
        build_evidence()
    elif cmd == 'shrink':
        c = conn(); cur = c.cursor()
        build_shrinkage()
    elif cmd == 'model':
        build_model()
    elif cmd == 'all':
        build_evidence()
        build_model()
    else:
        print('unknown command', cmd)
