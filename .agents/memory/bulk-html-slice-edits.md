---
name: Bulk string-slice edits in templates/index.html
description: Risks when rewriting large blocks of the monolithic dashboard template with python slice scripts
---
Rule: when replacing a JS/HTML region of templates/index.html by slicing between two string anchors, the end anchor can silently swallow neighboring functions that sit between the region you meant and the anchor (e.g. a helper defined right before the next function used as end marker). After any bulk slice edit, grep for every function still referenced (calls without definitions) before restarting.
**Why:** A Sector Comparison rewrite deleted renderDimensionWeights because the end anchor was the following function's signature; only an architect review caught the resulting ReferenceError.
**How to apply:** After python slice-replace edits, run a quick check that all `functionName(` call sites have matching `function functionName` definitions, plus the Jinja-stripped `new Function()` syntax check.
