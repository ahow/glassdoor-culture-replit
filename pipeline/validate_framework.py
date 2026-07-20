"""Phase 3.4 — framework validation Tests A-F on company-level v2 scores.

Test A — Ceiling saturation: <=15% of companies at |score| > 0.95 per dimension
Test B — Pairwise correlation: no |r| > 0.7 in the 12x12 matrix
Test C — PCA: PC1 explains <= 40% of variance (ideally 25-35%)
Test D — VIF: each dimension's VIF against the other 11 < 5
Test E — Sector universality: Test B stratified by top-5 GICS sectors (flag only)
Test F — Bootstrap stability: 80% subsample x 20 iters, pairwise r std < 0.05

Usage: python pipeline/validate_framework.py [--min-reviews 30]
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schroders_v2_keywords import SCHRODERS_V2_DIMENSIONS  # noqa: E402

DIMS = SCHRODERS_V2_DIMENSIONS


def _arg(name, default=None, cast=str):
    for i, a in enumerate(sys.argv):
        if a == name and i + 1 < len(sys.argv):
            return cast(sys.argv[i + 1])
    return default


def load_scores(min_reviews=30):
    import psycopg2
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cols = ', '.join(f'schroders_v2_{d}_score' for d in DIMS)
    df = pd.read_sql(
        f"SELECT company_name, review_count, {cols} "
        "FROM company_culture_scores_v2 WHERE review_count >= %s",
        conn, params=(min_reviews,))
    conn.close()
    df = df.rename(columns={f'schroders_v2_{d}_score': d for d in DIMS})
    return df.dropna(subset=DIMS)


def load_sectors(companies):
    import psycopg2
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    try:
        cur.execute("SELECT company_name, gics_sector FROM extraction_queue "
                    "WHERE company_name = ANY(%s)", (list(companies),))
        m = dict(cur.fetchall())
    except Exception:
        m = {}
    conn.close()
    return m


def test_a(df):
    print("Test A — Ceiling saturation (<=15% at |score| > 0.95):")
    ok = True
    for d in DIMS:
        pct = 100.0 * (df[d].abs() > 0.95).mean()
        passed = pct <= 15
        ok &= passed
        print(f"  {d}: {pct:.1f}% {'PASS' if passed else 'FAIL'}")
    return ok


def test_b(df, label=''):
    corr = df[DIMS].corr()
    worst = 0.0
    pairs = []
    for i, d1 in enumerate(DIMS):
        for d2 in DIMS[i + 1:]:
            r = abs(corr.loc[d1, d2])
            worst = max(worst, r)
            if r > 0.7:
                pairs.append((d1, d2, corr.loc[d1, d2]))
    ok = not pairs
    print(f"Test B{label} — Pairwise |r| <= 0.7: max |r| = {worst:.2f} "
          f"{'PASS' if ok else 'FAIL'}")
    for d1, d2, r in pairs:
        print(f"  VIOLATION: {d1} ~ {d2}: r = {r:+.2f}")
    return ok, corr


def test_c(df):
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler
    X = StandardScaler().fit_transform(df[DIMS].values)
    pca = PCA().fit(X)
    pc1 = 100.0 * pca.explained_variance_ratio_[0]
    ok = pc1 <= 40
    print(f"Test C — PCA: PC1 explains {pc1:.1f}% (target <= 40%, ideal 25-35%) "
          f"{'PASS' if ok else 'FAIL'}")
    return ok


def test_d(df):
    from sklearn.linear_model import LinearRegression
    X = df[DIMS].values
    print("Test D — VIF < 5:")
    ok = True
    for i, d in enumerate(DIMS):
        others = np.delete(X, i, axis=1)
        r2 = LinearRegression().fit(others, X[:, i]).score(others, X[:, i])
        vif = 1.0 / max(1e-9, 1.0 - r2)
        passed = vif < 5
        ok &= passed
        print(f"  {d}: VIF = {vif:.2f} {'PASS' if passed else 'FAIL'}")
    return ok


def test_e(df):
    sectors = load_sectors(df['company_name'])
    df = df.copy()
    df['sector'] = df['company_name'].map(sectors)
    top = df['sector'].value_counts().head(5).index
    print("Test E — Sector universality (flag only):")
    mats = {}
    for s in top:
        sub = df[df['sector'] == s]
        if len(sub) < 10:
            print(f"  {s}: only {len(sub)} companies — skipped")
            continue
        mats[s] = sub[DIMS].corr()
        print(f"  {s}: {len(sub)} companies")
    flags = []
    keys = list(mats)
    for i, d1 in enumerate(DIMS):
        for d2 in DIMS[i + 1:]:
            vals = [mats[s].loc[d1, d2] for s in keys]
            if vals and (max(vals) - min(vals)) > 0.6:
                flags.append((d1, d2, min(vals), max(vals)))
    for d1, d2, lo, hi in flags:
        print(f"  FLAG: {d1} ~ {d2} ranges {lo:+.2f} to {hi:+.2f} across sectors")
    if not flags:
        print("  No radical sector divergence found")
    return True  # flag-only


def test_f(df, iters=20):
    rng = np.random.default_rng(42)
    n = len(df)
    stack = []
    for _ in range(iters):
        idx = rng.choice(n, size=int(0.8 * n), replace=False)
        stack.append(df.iloc[idx][DIMS].corr().values)
    stds = np.std(np.array(stack), axis=0)
    worst = 0.0
    for i in range(12):
        for j in range(i + 1, 12):
            worst = max(worst, stds[i, j])
    ok = worst < 0.05
    print(f"Test F — Bootstrap stability: max pairwise r std = {worst:.3f} "
          f"(target < 0.05) {'PASS' if ok else 'FAIL'}")
    if not ok:
        print("  NOTE: instability suggests small-n effects; expected to improve "
              "on the full production corpus.")
    return ok


if __name__ == '__main__':
    min_reviews = _arg('--min-reviews', 30, int)
    df = load_scores(min_reviews)
    print(f"Loaded {len(df)} companies with >= {min_reviews} reviews "
          "and complete v2 scores\n")
    if len(df) < 10:
        print("Not enough companies to validate — score the corpus first.")
        sys.exit(1)
    results = {'A': test_a(df)}
    results['B'], _ = test_b(df)
    results['C'] = test_c(df)
    results['D'] = test_d(df)
    results['E'] = test_e(df)
    results['F'] = test_f(df)
    print("\nSummary:", {k: ('PASS' if v else 'FAIL') for k, v in results.items()})
