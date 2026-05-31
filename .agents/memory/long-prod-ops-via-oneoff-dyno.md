---
name: Long-running prod data ops via Heroku one-off dyno
description: How to run hours-long maintenance/scoring work against the Heroku prod DB reliably, and why the Replit agent env can't host it.
---

# Running long maintenance work against the Heroku production DB

## The constraint that bites
Background processes launched from the Replit agent shell (`nohup`, `setsid`, `disown`,
`&`) are **reaped when the tool-call that launched them returns** — they survive a few
minutes at most, then get SIGKILLed with no traceback in their log. Do NOT rely on them
for any job longer than a single ~120s tool call.

**Why:** the agent's bash runs in a process group the platform tears down between turns.

## The reliable pattern: Heroku one-off dyno
Run the work on Heroku's own infra instead. It is not subject to the 30s router timeout
(that only affects web requests), is co-located with the RDS DB, and is not reaped by the
agent env.

- Create via Platform API: `POST https://api.heroku.com/apps/<app>/dynos` with
  `{"command": "...", "attach": false, "type": "run", "time_to_live": 86400}`.
- **This app is on the Basic dyno tier**, so one-off `size` MUST be `"basic"`
  (`standard-2x` → HTTP 422 "You can only use Basic dynos").
- One-off dynos inherit all config vars (so `DATABASE_URL` is already set).
- To run a *local* script without redeploying, base64-encode it and run
  `python -u -c "import base64;exec(base64.b64decode('<B64>').decode())"`. It executes
  against the **already-deployed** modules (e.g. `culture_scoring`), so scoring matches
  prod as long as those modules haven't changed locally.
- Poll progress by connecting to the prod DB read-only and counting rows; check dyno
  liveness via `GET /apps/<app>/dynos/<name>` (404/absent = finished/exited).

**How to apply:** any backfill/re-score/migration over the full review set (millions of
rows). Make the driver idempotent (`ON CONFLICT DO NOTHING`, select only unscored via
`LEFT JOIN ... IS NULL`) and process per `company_name` (indexed) — not a global
`LEFT JOIN` over all 3.3M+ rows per batch, which is what makes `/api/score-reviews` slow.

## Scoring driver must write ALL frameworks
The old standalone `score_reviews_batch.py` inserts only Hofstede+MIT (19 cols) and
**omits the 18 Schroders columns** — using it leaves `schroders_d01..d18` NULL. The
canonical full insert (36 score cols) lives in `extraction_manager._score_company_reviews`.
Any from-scratch scorer must mirror that column set or it silently skips Schroders.

## Browser scoring loop fragility
The dashboard "Process Culture Scores" loop calls `/api/score-reviews` repeatedly. A single
transient response (Heroku H12 30s timeout returns an HTML error page, or a brief 5xx) used
to make `response.json()` throw `Unexpected token '<'` and kill the whole run. The loop must
guard `r.ok`, wrap `r.json()` in try/catch, treat `!d.success`/network errors as retryable
with backoff, and use a run-instance id so a stop+restart during backoff can't spawn
overlapping loops.
