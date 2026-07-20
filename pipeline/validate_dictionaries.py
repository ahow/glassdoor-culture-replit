"""Phase 2.5 — four automated dictionary validation checks (no hand-labelling).

Check 1 — Balance: pos/neg term-count ratio in [0.8, 1.25] per dimension
Check 2 — Corpus firing rate: each pole fires on 5-40% of 10,000 random reviews
Check 3 — Semantic separation: pole centroids (own-dim < 0.5, cross-dim < 0.6)
Check 4 — Known-company sanity test: dominant poles match public reputations

Usage: python pipeline/validate_dictionaries.py [check1|check2|check3|check4|all]
"""

import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schroders_v2_keywords import SCHRODERS_V2_KEYWORDS, SCHRODERS_V2_DIMENSIONS  # noqa: E402
from scoring_engine_v2 import compile_keywords, score_review_v2  # noqa: E402

COMPILED = compile_keywords(SCHRODERS_V2_KEYWORDS)

KNOWN_COMPANIES = {
    # company -> {dim: expected sign (+1 positive pole, -1 negative pole)}
    'Costco Wholesale': {'b09': +1, 'b02': -1, 'b06': +1, 'b10': +1},
    'Goldman Sachs': {'b03': -1, 'b05': -1, 'b07': +1, 'b08': +1},
    'Salesforce': {'b01': +1, 'b02': +1, 'b12': +1, 'b06': +1},
    'Wells Fargo': {'b11': -1, 'b03': -1, 'b10': -1},
    "McDonald's": {'b04': -1, 'b03': -1, 'b02': -1},
    'Microsoft': {'b01': +1, 'b09': +1, 'b12': +1},
}


def fetch_reviews(where='', params=(), limit=10000):
    import psycopg2
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(summary,'') || '. ' || COALESCE(pros,'') || '. ' || "
        "COALESCE(cons,'') || '. ' || COALESCE(advice_to_management,'') "
        f"FROM reviews {where} ORDER BY random() LIMIT %s", params + (limit,))
    rows = [r[0] for r in cur.fetchall() if r[0] and r[0].strip()]
    conn.close()
    return rows


def check1_balance():
    print("Check 1 — Balance (pos/neg ratio in [0.8, 1.25]):")
    ok = True
    for dim in SCHRODERS_V2_DIMENSIONS:
        p = len(SCHRODERS_V2_KEYWORDS[dim]['positive'])
        n = len(SCHRODERS_V2_KEYWORDS[dim]['negative'])
        ratio = p / max(1, n)
        passed = 0.8 <= ratio <= 1.25
        ok &= passed
        print(f"  {dim}: {p} pos / {n} neg = {ratio:.2f} {'PASS' if passed else 'FAIL'}")
    return ok


def check2_firing_rate(n_reviews=10000):
    print(f"Check 2 — Firing rate on {n_reviews} random reviews (target 5-40%):")
    texts = fetch_reviews(limit=n_reviews)
    fires = {(d, p): 0 for d in SCHRODERS_V2_DIMENSIONS for p in ('positive', 'negative')}
    for t in texts:
        tl = t.lower()
        for dim, poles in COMPILED.items():
            for pole, pats in poles.items():
                if any(pat.search(tl) for pat, _ in pats):
                    fires[(dim, pole)] += 1
    ok = True
    for (dim, pole), ct in sorted(fires.items()):
        rate = 100.0 * ct / max(1, len(texts))
        passed = 5 <= rate <= 40
        ok &= passed
        print(f"  {dim} {pole}: {rate:.1f}% {'PASS' if passed else 'FAIL'}")
    return ok


def check3_semantic_separation():
    print("Check 3 — Semantic separation of pole centroids:")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    centroids = {}
    for dim in SCHRODERS_V2_DIMENSIONS:
        for pole in ('positive', 'negative'):
            terms = list(SCHRODERS_V2_KEYWORDS[dim][pole].keys())
            embs = model.encode(terms, normalize_embeddings=True)
            c = embs.mean(axis=0)
            centroids[(dim, pole)] = c / np.linalg.norm(c)
    issues = []
    for (d1, p1), c1 in centroids.items():
        opp = (d1, 'negative' if p1 == 'positive' else 'positive')
        sim = float(np.dot(c1, centroids[opp]))
        if p1 == 'positive' and sim > 0.5:
            issues.append(f"{d1} pos<->neg similarity {sim:.2f} > 0.5")
        for (d2, p2), c2 in centroids.items():
            if d1 >= d2:
                continue
            sim = float(np.dot(c1, c2))
            if sim > 0.6:
                issues.append(f"{d1} {p1} ~ {d2} {p2}: {sim:.2f} > 0.6")
    if issues:
        for i in issues:
            print("  ISSUE:", i)
    else:
        print("  PASS: all centroids well separated")
    return not issues


def check4_known_companies():
    print("Check 4 — Known-company sanity test:")
    results = {}
    ok = True
    for company, expected in KNOWN_COMPANIES.items():
        texts = fetch_reviews("WHERE company_name = %s", (company,), limit=2000)
        if len(texts) < 50:
            print(f"  {company}: only {len(texts)} reviews in this corpus — SKIP")
            continue
        sums = {d: [] for d in SCHRODERS_V2_DIMENSIONS}
        for t in texts:
            r = score_review_v2(t, COMPILED)
            if not r:
                continue
            for d in SCHRODERS_V2_DIMENSIONS:
                if r[d]['score'] is not None:
                    sums[d].append(r[d]['score'])
        means = {d: (np.mean(v) if v else None) for d, v in sums.items()}
        results[company] = means
        for dim, sign in expected.items():
            m = means.get(dim)
            if m is None:
                print(f"  {company} {dim}: no evidence — FAIL")
                ok = False
                continue
            passed = (m > 0) == (sign > 0)
            ok &= passed
            print(f"  {company} {dim}: mean {m:+.3f}, expected "
                  f"{'positive' if sign > 0 else 'negative'} "
                  f"{'PASS' if passed else 'FAIL'}")
    return ok


if __name__ == '__main__':
    which = sys.argv[1] if len(sys.argv) > 1 else 'all'
    results = {}
    if which in ('check1', 'all'):
        results['balance'] = check1_balance()
    if which in ('check2', 'all'):
        results['firing_rate'] = check2_firing_rate()
    if which in ('check3', 'all'):
        results['semantic_separation'] = check3_semantic_separation()
    if which in ('check4', 'all'):
        results['known_companies'] = check4_known_companies()
    print("\nSummary:", {k: ('PASS' if v else 'FAIL') for k, v in results.items()})
