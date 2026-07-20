"""Phase 1 sanity check: diff v1 (substring) vs v2 (regex+negation) Schroders
scores on a random sample of reviews. Expect >=5% of reviews to change."""

import os
import sys
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from culture_scoring import score_review_with_dictionary
from schroders_keywords import SCHRODERS_KEYWORDS, SCHRODERS_DIMENSIONS
from scoring_engine_v2 import compile_keywords, score_review_v2

SAMPLE_SIZE = int(sys.argv[1]) if len(sys.argv) > 1 else 1000


def fetch_sample(n):
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(summary,'') || ' ' || COALESCE(pros,'') || ' ' || COALESCE(cons,'') || ' ' || COALESCE(advice_to_management,'')
        FROM reviews TABLESAMPLE SYSTEM (1)
        LIMIT %s
    """, (n,))
    rows = [r[0] for r in cur.fetchall() if r[0] and r[0].strip()]
    conn.close()
    return rows


def main():
    compiled = compile_keywords(SCHRODERS_KEYWORDS)
    texts = fetch_sample(SAMPLE_SIZE)
    print(f"Sampled {len(texts)} reviews")

    changed_reviews = 0
    dim_changes = {d: 0 for d in SCHRODERS_DIMENSIONS}
    sign_flips = {d: 0 for d in SCHRODERS_DIMENSIONS}

    for text in texts:
        v1 = score_review_with_dictionary(text)
        v2 = score_review_v2(text, compiled)
        if not v1 or not v2:
            continue
        any_change = False
        for d in SCHRODERS_DIMENSIONS:
            s1 = v1['schroders'][d]['score']
            s2 = v2[d]['score']
            if s1 != s2:
                dim_changes[d] += 1
                any_change = True
                if s1 is not None and s2 is not None and (s1 > 0) != (s2 > 0):
                    sign_flips[d] += 1
        if any_change:
            changed_reviews += 1

    pct = 100.0 * changed_reviews / max(1, len(texts))
    print(f"\nReviews with >=1 Schroders score change: {changed_reviews} ({pct:.1f}%)")
    print(f"{'dim':>5} {'changed':>8} {'sign flips':>10}")
    for d in SCHRODERS_DIMENSIONS:
        print(f"{d:>5} {dim_changes[d]:>8} {sign_flips[d]:>10}")

    if pct < 5:
        print("\nWARNING: <5% of reviews changed — regex/negation may not be firing.")
        sys.exit(1)
    print("\nPASS: change rate >= 5% threshold.")


if __name__ == '__main__':
    main()
