---
name: Heroku incremental-update resilience
description: Why long-running incremental updates must not live in the web dyno, and the worker + auto-resume pattern that replaced it.
---

# Heroku incremental-update resilience

The "Update All Companies" incremental review-fetch is long-running and calls the
**paid** Glassdoor APIs (RapidAPI / OpenWeb Ninja). It used to run as a daemon
thread inside the Heroku **web** dyno, which restarts often (daily cycling,
deploys, idle-sleep). Every restart killed the thread mid-run → state stuck and
surfaced to the user as "Interrupted (server restart)".

**Two-layer fix (user chose "do both"):**
- **A — web auto-resume (no extra cost):** a startup daemon thread, ~20s after
  boot, resumes an `interrupted` run automatically — but only when NOT in worker
  mode. So a single-web-dyno user self-heals without manual re-click.
- **B — worker dyno:** when `USE_WORKER=true`, the web process only *queues* the
  request (state `queued`) and a dedicated worker dyno executes/resumes it, so a
  web restart never touches the run.

**Rules baked in (don't regress):**
- In worker mode, web must NOT run `_reset_stale_running_state` against a
  worker-owned run (guarded by a module flag set only in the worker process).
  Otherwise a web restart flips the worker's live run to `interrupted`.
- Only ONE executor may start a run. Use an **atomic** SQL claim
  (`UPDATE ... WHERE state IN (resumable...) RETURNING`) to transition to
  `running` — read-then-write races otherwise double-spend the paid API.
- Every early error-return AFTER claiming must release the claim (set `error`),
  or the run wedges in `running`.
- Stale-state recovery must handle BOTH `running` (→ `interrupted`, resumable)
  and `stopping` (→ `stopped`, honour the stop). Missing `stopping` wedges the
  worker loop forever because it waits on `running`/`stopping` as "active".
- `WORKER_AUTO_EXTRACT` (default OFF) gates the worker auto-starting the costly
  full extraction of the ~2,400-company queue. Explicit "Start extraction" from
  the dashboard still works regardless of the gate. Keep it UNSET in prod to
  avoid surprise paid extraction when scaling the worker on.

**Why:** Heroku web dynos are not a safe home for long, stateful, paid
background work; treat any such job as worker-owned with idempotent,
atomically-claimed, restart-tolerant state.

**Dev gotcha:** state lives in a single `incremental_update_status` row (id=1).
Running the state-machine tests in a side process pollutes the shared dev DB;
the web auto-resume can then pick up a fake `interrupted` and fire a real paid
run. Reset the row to `idle` after such tests.

States: idle, queued (worker hand-off), running, stopping, stopped, completed,
interrupted, error.
