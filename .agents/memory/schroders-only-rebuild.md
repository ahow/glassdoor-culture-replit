---
name: Schroders-only rebuild state
description: Which framework is active after the 2026-07-21 rebuild and how retirement was done (hidden, not deleted).
---
Since 2026-07-21, Schroders v2 (mined dictionary) is the ONLY active framework. Hofstede/MIT tabs and the v1/v2 toggle are hidden in templates/index.html (markup + JS retained, tab buttons removed); /api/v2/framework-toggle rejects anything but 'v2'.
**Why:** reviewer brief required a single active framework with a fast rollback path; deleting would break restore.
**How to apply:** never resurrect Hofstede/MIT outputs in new features; to restore, use snapshots/pre_schroders_sector_relative_rebuild_2026_07_21/ROLLBACK.md. Factor model outputs are internal-research-only (negative CV R², only 23/53 companies have perf data). Dashboard tables must use DOM construction (textContent), not innerHTML — company names from DB are untrusted (architect flagged stored XSS once already).
