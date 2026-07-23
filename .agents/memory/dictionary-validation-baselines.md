---
name: Dictionary validation needs baselines
description: How to judge dictionary validation gate "failures" for the Schroders v2 keyword pipeline
---
Validation gates (firing rates, semantic separation, check4 anchors) produce FAIL labels that the
*previous* dictionary also failed. Never judge a new dictionary on absolute gate labels alone.

**Why:** The v3 generalised dictionary initially looked like a regression (check2/check3/check4 FAIL),
but re-running the same gates on the old finance dictionary against the same samples showed the old
one was equal or worse (semantic issues 66 vs 56; per-pole under-firing pre-existing; check4 was
never runnable before). The FAILs are inherent limits of keyword bipolar scoring.

**How to apply:** Before deciding a dictionary rebuild "failed validation", re-run every gate with
the old dictionary isolated in /tmp (root file shadows imports — copy the whole script+engine+dict
into a temp dir and run with cwd there, or sys.path tricks silently pick up the new root file; a
tell-tale is byte-identical results between "old" and "new" runs). Compare deltas, not labels.
Also: score_review_v2 returns 'evidence_count', not 'evidence'.
