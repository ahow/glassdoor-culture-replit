# Exact Scoring Logic (as implemented)

Engine: `scoring_engine_v2.py`, version `v2.0-regex-negation`. Identical engine
used for both dictionaries in every file in this pack.

## 1. Text assembly
Each review's scored text = `summary + '. ' + pros + '. ' + cons + '. ' + advice_to_management`
(NULL fields treated as empty strings). No other fields are scored.

## 2. Preprocessing
- Lowercased once per review.
- **No stemming, no lemmatization, no stop-word removal, no spelling normalization.**
- Tokenization for negation windows only: whitespace-delimited tokens (`\S+`),
  with character offsets recorded.

## 3. Phrase matching
- Every dictionary term is compiled to a **word-boundary regular expression**.
- Term is split on whitespace **and hyphens**; tokens are rejoined with the
  pattern `[\s\-]+`. So `long-term thinking` matches "long-term thinking",
  "long term thinking", and "long   term-thinking". Case-insensitive.
- Exact token match only — no fuzzy matching, no partial-word matches
  (`ethical` never fires inside `unethical`).
- Note: the hyphen-splitting of the *term* side was added on 2026-07-21.
  Before that fix, a hyphenated dictionary term only matched hyphenated text.
  All outputs in this pack were produced **after** the fix.

## 4. Negation handling
- Window: 5 whitespace tokens immediately preceding the match start.
- Negation markers: no, not, never, nothing, none, nobody, nowhere, lacks,
  lacking, lack, without, absence, poor, weak, insufficient, inadequate,
  minimal, n't (as substring of a token), hardly, barely, rarely, seldom.
- A negated positive-pole match counts as evidence for the negative pole and
  vice versa (polarity flip, not discard).
- Intra-word prefixes (un-, dis-) are NOT treated as negation; prefixed
  antonyms are expected to be dictionary entries in the opposite pole.

## 5. Per-review dimension score
For each bipole: `pos = Σ weight×(plain positive matches + negated negative matches)`,
`neg = Σ weight×(plain negative matches + negated positive matches)`.

- `score = (pos − neg) / (pos + neg)` ∈ [−1, +1]
- `evidence = pos + neg` (weighted match mass)
- If `pos + neg = 0` the dimension is **None/blank** for that review (not 0).

## 6. Company aggregation
- Company dimension score = unweighted mean of the per-review scores of the
  reviews where that dimension fired (blanks excluded — this is
  "mean of mentions", not "mean over all reviews").
- No review-count weighting, no time decay, no normalization across companies
  at this stage. Composite (equal-weight) = mean of the 12 dimension means,
  only when all 12 are non-null.

## 7. Weights
- Mined dictionary: weights from corpus-mining pipeline (0.25–2.0 range,
  frequency/distinctiveness based).
- Expert dictionary: weights exactly as delivered (1.0 / 0.75 / 0.5 / 0.25
  per the semantic-margin table in the construction memo). 480 expert seeds
  at 1.0.

## 8. Dictionary versions used in this pack
- `mined` = corpus-mined dictionary, version `2026-08-01-v2`, 1,731 terms.
- `expert` = expert-seed dictionary as delivered, version
  `v2.1.0-expert-seeds`, 2,242 terms.
