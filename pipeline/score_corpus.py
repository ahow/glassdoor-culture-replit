"""Phase 3.3 — score the full review corpus under the v2 dictionaries/engine.

Writes per-review b01-b12 scores into review_culture_scores
(schroders_v2_bXX_score / _evidence, dictionary_version, scoring_engine_version)
and aggregates company-level scores into company_culture_scores_v2, including
the 2018-cutoff temporal series (Phase 5).

Resumable: tracks the last processed review id in pipeline_output/score_state.json.

Usage:
    python pipeline/score_corpus.py reviews   [--batch 5000] [--max-batches N]
    python pipeline/score_corpus.py aggregate
"""

import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_values

from schroders_v2_keywords import (  # noqa: E402
    SCHRODERS_V2_KEYWORDS, SCHRODERS_V2_DIMENSIONS, DICTIONARY_VERSION,
)
from scoring_engine_v2 import (  # noqa: E402
    compile_keywords, score_review_v2, SCORING_ENGINE_VERSION,
)

COMPILED = compile_keywords(SCHRODERS_V2_KEYWORDS)
STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                     'pipeline_output', 'score_state.json')


def _arg(name, default=None, cast=str):
    for i, a in enumerate(sys.argv):
        if a == name and i + 1 < len(sys.argv):
            return cast(sys.argv[i + 1])
    return default


def load_state():
    if os.path.exists(STATE):
        with open(STATE) as f:
            return json.load(f)
    return {'last_id': 0}


def save_state(s):
    with open(STATE, 'w') as f:
        json.dump(s, f)


def score_reviews():
    batch = _arg('--batch', 5000, int)
    max_batches = _arg('--max-batches', None, int)
    state = load_state()
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    wcur = conn.cursor()
    cols = []
    for i in range(1, 13):
        cols += [f'schroders_v2_b{i:02d}_score', f'schroders_v2_b{i:02d}_evidence']
    n_done = 0
    b = 0
    while True:
        cur.execute(
            "SELECT id, COALESCE(summary,'') || '. ' || COALESCE(pros,'') || '. ' || "
            "COALESCE(cons,'') || '. ' || COALESCE(advice_to_management,''), company_name "
            "FROM reviews WHERE id > %s ORDER BY id LIMIT %s",
            (state['last_id'], batch))
        rows = cur.fetchall()
        if not rows:
            break
        values = []
        for rid, text, company in rows:
            r = score_review_v2(text, COMPILED)
            row = [rid, company]
            for d in SCHRODERS_V2_DIMENSIONS:
                if r:
                    row += [r[d]['score'], r[d]['evidence_count']]
                else:
                    row += [None, 0]
            row += [DICTIONARY_VERSION, SCORING_ENGINE_VERSION]
            values.append(tuple(row))
        col_sql = ', '.join(cols)
        set_sql = ', '.join(f"{c} = EXCLUDED.{c}" for c in cols)
        execute_values(wcur, f"""
            INSERT INTO review_culture_scores (review_id, company_name, {col_sql},
                dictionary_version, scoring_engine_version)
            VALUES %s
            ON CONFLICT (review_id) DO UPDATE SET {set_sql},
                dictionary_version = EXCLUDED.dictionary_version,
                scoring_engine_version = EXCLUDED.scoring_engine_version,
                company_name = COALESCE(review_culture_scores.company_name, EXCLUDED.company_name)
        """, values)
        conn.commit()
        state['last_id'] = rows[-1][0]
        save_state(state)
        n_done += len(rows)
        b += 1
        print(f"  scored {n_done} reviews (last id {state['last_id']})", flush=True)
        if max_batches and b >= max_batches:
            break
    conn.close()
    print(f"Done: {n_done} reviews scored this run.")


def aggregate():
    """Company-level aggregation incl. 2018-cutoff series (Phase 5)."""
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    score_cols = ', '.join(
        f"rcs.schroders_v2_b{i:02d}_score, rcs.schroders_v2_b{i:02d}_evidence"
        for i in range(1, 13))
    cur.execute(f"""
        SELECT rcs.company_name, r.review_datetime, {score_cols}
        FROM review_culture_scores rcs
        JOIN reviews r ON r.id = rcs.review_id
        WHERE rcs.dictionary_version = %s
    """, (DICTIONARY_VERSION,))
    agg = defaultdict(lambda: {'all': defaultdict(list), 'pre2019': defaultdict(list),
                               'n': 0, 'n2018': 0})
    for row in cur:
        company, dt = row[0], row[1]
        rec = agg[company]
        rec['n'] += 1
        is_pre = dt is not None and dt.year <= 2018
        if is_pre:
            rec['n2018'] += 1
        for i in range(12):
            s = row[2 + i * 2]
            if s is not None:
                rec['all'][i].append(s)
                if is_pre:
                    rec['pre2019'][i].append(s)

    up = conn.cursor()
    dims = SCHRODERS_V2_DIMENSIONS
    for company, rec in agg.items():
        vals = {'company_name': company, 'review_count': rec['n'],
                'review_count_2018': rec['n2018']}
        means = []
        for i, d in enumerate(dims):
            xs = rec['all'][i]
            m = sum(xs) / len(xs) if xs else None
            vals[f'schroders_v2_{d}_score'] = m
            vals[f'schroders_v2_{d}_evidence'] = len(xs)
            xs18 = rec['pre2019'][i]
            vals[f'schroders_v2_{d}_score_2018'] = (
                sum(xs18) / len(xs18) if xs18 else None)
            if m is not None:
                means.append(m)
        # Composite 1 — equal-weighted mean of available dimension scores
        vals['schroders_v2_composite_equalwt'] = (
            sum(means) / len(means) if len(means) == 12 else None)
        vals['schroders_v2_composite_corrwt'] = None  # computed by the app per industry group
        vals['dictionary_version'] = DICTIONARY_VERSION
        vals['scoring_engine_version'] = SCORING_ENGINE_VERSION
        cols = list(vals.keys())
        up.execute(f"""
            INSERT INTO company_culture_scores_v2 ({', '.join(cols)})
            VALUES ({', '.join(['%s'] * len(cols))})
            ON CONFLICT (company_name) DO UPDATE SET
                {', '.join(f"{c} = EXCLUDED.{c}" for c in cols if c != 'company_name')},
                updated_at = NOW()
        """, [vals[c] for c in cols])
    conn.commit()
    conn.close()
    print(f"Aggregated {len(agg)} companies into company_culture_scores_v2")


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'reviews'
    if cmd == 'reviews':
        score_reviews()
    elif cmd == 'aggregate':
        aggregate()
    else:
        print(__doc__)
