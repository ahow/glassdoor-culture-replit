# Rollback Point: pre_schroders_sector_relative_rebuild_2026_07_21

Created: 2026-07-21
Git commit (code snapshot): `65f822630e49960fb26350e8e77b73863b3840c0`
(Replit checkpoint "Saved progress at the end of the loop", 2026-07-21)

## What is saved

1. **Code** — the git commit above contains the full codebase at snapshot
   time (Hofstede + MIT + Schroders v2 all active, expert dictionary
   `v2.1.0-expert-seeds` live in `schroders_v2_keywords.py`).
2. **Dictionaries** — `dictionaries/` in this folder:
   - `schroders_v2_keywords.py` (expert, v2.1.0-expert-seeds — active at snapshot)
   - `schroders_v2_keywords_mined_2026-08-01.py` (mined backup)
   - `schroders_keywords.py` (v1), `culture_scoring.py` (Hofstede/MIT),
     `scoring_engine_v2.py` (engine)
3. **Database** — `db/*.sql.gz` pg_dump of every table the rebuild touches:
   - `review_culture_scores` (321,066 rows: Hofstede/MIT + v2 expert scores)
   - `company_culture_scores_v2` (52 rows, expert aggregates)
   - `company_metrics_cache` (Hofstede/MIT company aggregates)
   - `quarterly_culture_trends`, `company_culture_profiles`,
     `fmp_performance_metrics`, `app_config`
   - `reviews` and `extraction_*` tables are NOT dumped — the rebuild never
     modifies raw reviews or extraction state.

## Exact rollback steps

```bash
# 1. Restore code (via Replit: roll back to checkpoint 65f8226,
#    or ask the agent to restore files from this commit).

# 2. Restore database tables (drops rebuilt versions, restores snapshot):
SNAP=snapshots/pre_schroders_sector_relative_rebuild_2026_07_21
for t in review_culture_scores company_culture_scores_v2 company_metrics_cache \
         quarterly_culture_trends company_culture_profiles \
         fmp_performance_metrics app_config; do
  psql "$DATABASE_URL" -c "DROP TABLE IF EXISTS $t CASCADE;"
  zcat $SNAP/db/$t.sql.gz | psql "$DATABASE_URL"
done

# 3. Drop any new tables created by the rebuild (safe if absent):
psql "$DATABASE_URL" -c "DROP TABLE IF EXISTS
  schroders_company_dimension_evidence,
  schroders_sector_model_weights,
  schroders_company_factor_scores,
  schroders_overlap_diagnostics,
  schroders_factor_settings CASCADE;"

# 4. Restart the app workflow.
```

## Estimated rollback time
- Code: instant (checkpoint rollback).
- Database: ~2–5 minutes (review_culture_scores is the only large table).
- Total: under 10 minutes.
