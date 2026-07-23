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
    """Company-level aggregation incl. 2018-cutoff series (Phase 5).

    Done entirely in SQL so it scales to millions of rows (the previous
    Python-side version held every score in memory and OOMs on prod).
    """
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    dims = SCHRODERS_V2_DIMENSIONS
    select_parts = []
    insert_cols = ['company_name', 'review_count', 'review_count_2018']
    for i, d in enumerate(dims):
        col = f'schroders_v2_b{i + 1:02d}_score'
        select_parts.append(f"AVG(rcs.{col}) AS {d}_score")
        select_parts.append(f"COUNT(rcs.{col}) AS {d}_evidence")
        select_parts.append(
            f"AVG(rcs.{col}) FILTER (WHERE r.review_datetime IS NOT NULL "
            f"AND EXTRACT(YEAR FROM r.review_datetime) <= 2018) AS {d}_score_2018")
        insert_cols += [f'schroders_v2_{d}_score', f'schroders_v2_{d}_evidence',
                        f'schroders_v2_{d}_score_2018']
    insert_cols += ['schroders_v2_composite_equalwt', 'schroders_v2_composite_corrwt',
                    'dictionary_version', 'scoring_engine_version']
    cur.execute(f"""
        SELECT rcs.company_name,
               COUNT(*) AS review_count,
               COUNT(*) FILTER (WHERE r.review_datetime IS NOT NULL
                   AND EXTRACT(YEAR FROM r.review_datetime) <= 2018) AS review_count_2018,
               {', '.join(select_parts)}
        FROM review_culture_scores rcs
        JOIN reviews r ON r.id = rcs.review_id
        WHERE rcs.dictionary_version = %s
        GROUP BY rcs.company_name
    """, (DICTIONARY_VERSION,))
    rows = cur.fetchall()

    up = conn.cursor()
    n = 0
    for row in rows:
        company, review_count, review_count_2018 = row[0], row[1], row[2]
        vals = [company, review_count, review_count_2018]
        means = []
        for i in range(len(dims)):
            score, evidence, score_2018 = row[3 + i * 3], row[4 + i * 3], row[5 + i * 3]
            score = float(score) if score is not None else None
            score_2018 = float(score_2018) if score_2018 is not None else None
            vals += [score, evidence, score_2018]
            if score is not None:
                means.append(score)
        vals.append(sum(means) / len(means) if len(means) == 12 else None)
        vals.append(None)  # composite_corrwt computed by the app per industry group
        vals += [DICTIONARY_VERSION, SCORING_ENGINE_VERSION]
        up.execute(f"""
            INSERT INTO company_culture_scores_v2 ({', '.join(insert_cols)})
            VALUES ({', '.join(['%s'] * len(insert_cols))})
            ON CONFLICT (company_name) DO UPDATE SET
                {', '.join(f"{c} = EXCLUDED.{c}" for c in insert_cols if c != 'company_name')},
                updated_at = NOW()
        """, vals)
        n += 1
    conn.commit()
    conn.close()
    print(f"Aggregated {n} companies into company_culture_scores_v2")


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'reviews'
    if cmd == 'reviews':
        score_reviews()
    elif cmd == 'aggregate':
        aggregate()
    else:
        print(__doc__)
