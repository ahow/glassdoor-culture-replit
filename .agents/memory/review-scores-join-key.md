---
name: review_culture_scores join key
description: Correct way to join review_culture_scores to reviews — the FK is reviews.id, not reviews.review_id
---

**Rule:** `review_culture_scores.review_id` references `reviews.id` (the serial PK), NOT `reviews.review_id` (the Glassdoor review ID). Always join `ON s.review_id = r.id`.

**Why:** Joining on `r.review_id` silently matches only ~125K of 3.4M rows (coincidental integer overlap, all pre-2013 reviews). This froze the backtest universe at 475 companies at every quarter since 2015 and looked plausible until the constant membership was questioned. Fixed 2026-07-27 in pipeline/backtest.py and the current-employee filter in app.py.

**How to apply:** Any new query joining review-level culture scores to review metadata (dates, employee status, ratings) must use `reviews.id`. A quick sanity check: the join should return ~3.4M rows and cover review years through the present.
