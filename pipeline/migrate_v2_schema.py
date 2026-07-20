"""Phase 1.3 + Phase 6.1 schema migration (idempotent).

Adds:
- dictionary_version / scoring_engine_version to review_culture_scores
- v2 bipole columns (b01-b12 score + evidence) to review_culture_scores
- schroders_v2 composite columns to company_metrics_cache-adjacent storage
  (company-level v2 scores live in company_culture_scores_v2)
- framework toggle in extraction_control-style config table (app_config)

Run:  python pipeline/migrate_v2_schema.py [--database-url URL]
"""

import os
import sys

import psycopg2


def get_url():
    for i, a in enumerate(sys.argv):
        if a == '--database-url' and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return os.environ['DATABASE_URL']


DDL = []

# Phase 1.3 — version tagging
DDL += [
    "ALTER TABLE review_culture_scores ADD COLUMN IF NOT EXISTS dictionary_version VARCHAR(40)",
    "ALTER TABLE review_culture_scores ADD COLUMN IF NOT EXISTS scoring_engine_version VARCHAR(40)",
]

# Phase 6.1 — v2 bipole columns on review_culture_scores
for i in range(1, 13):
    DDL.append(f"ALTER TABLE review_culture_scores ADD COLUMN IF NOT EXISTS schroders_v2_b{i:02d}_score REAL")
    DDL.append(f"ALTER TABLE review_culture_scores ADD COLUMN IF NOT EXISTS schroders_v2_b{i:02d}_evidence REAL DEFAULT 0")

# Company-level v2 scores (aggregated), incl. both composites and the
# 2018-cutoff temporal series (Phase 5).
company_cols = ",\n".join(
    [f"schroders_v2_b{i:02d}_score REAL,\n            schroders_v2_b{i:02d}_evidence REAL DEFAULT 0,"
     f"\n            schroders_v2_b{i:02d}_score_2018 REAL" for i in range(1, 13)]
)
DDL.append(f"""
    CREATE TABLE IF NOT EXISTS company_culture_scores_v2 (
        company_name VARCHAR(255) PRIMARY KEY,
        review_count INTEGER DEFAULT 0,
        review_count_2018 INTEGER DEFAULT 0,
        {company_cols},
        schroders_v2_composite_equalwt REAL,
        schroders_v2_composite_corrwt REAL,
        dictionary_version VARCHAR(40),
        scoring_engine_version VARCHAR(40),
        updated_at TIMESTAMP DEFAULT NOW()
    )
""")

# Config table with framework toggle
DDL += [
    """
    CREATE TABLE IF NOT EXISTS app_config (
        key VARCHAR(100) PRIMARY KEY,
        value VARCHAR(255),
        updated_at TIMESTAMP DEFAULT NOW()
    )
    """,
    """
    INSERT INTO app_config (key, value) VALUES ('schroders_framework_active', 'v1')
    ON CONFLICT (key) DO NOTHING
    """,
]

# Tag existing v1 rows
DDL += [
    "UPDATE review_culture_scores SET dictionary_version = '2026-04-20', "
    "scoring_engine_version = 'v1-substring' WHERE dictionary_version IS NULL",
]


def main():
    conn = psycopg2.connect(get_url())
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    conn.commit()
    cur.execute("SELECT count(*) FROM information_schema.columns WHERE table_name='review_culture_scores' AND column_name LIKE 'schroders_v2%'")
    n = cur.fetchone()[0]
    print(f"review_culture_scores v2 columns: {n}")
    cur.execute("SELECT value FROM app_config WHERE key='schroders_framework_active'")
    print("framework toggle:", cur.fetchone()[0])
    conn.close()
    print("Migration complete.")


if __name__ == '__main__':
    main()
