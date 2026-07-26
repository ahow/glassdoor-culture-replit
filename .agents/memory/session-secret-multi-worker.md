---
name: Session secret behind multiple workers
description: Flask session cookies fail intermittently on Heroku/gunicorn if the secret is not shared across workers
---

Flask's session cookie is signed with `app.secret_key`. If that secret is random per process (e.g. `os.urandom` fallback when SESSION_SECRET is unset), every gunicorn worker/dyno gets a different secret and a cookie signed by one worker is rejected by the others.

**Symptom:** intermittent "logged out"/401s, users must log in several times, pages half-fail to load — works fine in single-process dev.

**Why:** Heroku prod did not have SESSION_SECRET set; the Replit dev env did, which masked the bug locally.

**How to apply:** the app now falls back to a shared secret stored once in the `app_config` table (`key='session_secret'`, inserted with ON CONFLICT DO NOTHING so all workers converge). Env var SESSION_SECRET still takes priority. Never use a per-process random secret or a static string default.
