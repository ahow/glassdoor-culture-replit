---
name: Culture v2 pipeline
description: How the v2 bipolar framework pipeline is run/re-run and its dev-corpus caveats
---
- Re-scoring/rebuilding v2: resumable offline scripts in `pipeline/` (mine → embed → build dicts → score_corpus reviews → aggregate → validate_framework). Run in foreground `timeout 110` chunks; background nohup procs get reaped in the agent env.
- **Why:** long jobs on the web dyno/agent shell get killed; every stage checkpoints state under `pipeline_output/`.
- Pipeline-heavy deps (sentence-transformers, spacy, nltk) must stay ONLY in `pipeline/requirements.txt`, never the root `requirements.txt` — they break/bloat the Heroku web build.
- Dev-corpus caveats documented in METHODOLOGY.md Part II: finance-only dictionaries, Test F bootstrap stability fails at n=47 companies, 3 poles seed-sensitive (b03/b05/b07 positive), b11neg under-fires.
- Dashboard v1/v2 switch is `schroders_framework_active` in `app_config` (served by /api/v2/framework-toggle, fail-soft table creation).
