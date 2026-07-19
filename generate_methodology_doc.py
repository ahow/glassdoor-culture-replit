"""Generate METHODOLOGY.md — full technical methodology + complete keyword appendices.
Run: python3 generate_methodology_doc.py
"""
from culture_scoring import HOFSTEDE_DIMENSIONS, MIT_BIG_9_KEYWORDS
from schroders_keywords import SCHRODERS_DIMENSIONS, SCHRODERS_DIM_INFO, SCHRODERS_KEYWORDS

OUT = "METHODOLOGY.md"

PROSE = r"""# Culture Analytics Dashboard — Methodology & Technical Specification

This document describes, in full technical detail, how employee-review text is converted into
organisational-culture scores under three frameworks (Hofstede, MIT Big 9, Schroders 18) and how
those scores are related to business performance. It includes the complete keyword dictionaries
with weights, so a developer can replicate the analysis exactly.

---

## 1. Pipeline Overview

```
Glassdoor reviews (API extraction)
        │
        ▼
PostgreSQL `reviews` table  (raw text + star ratings + metadata)
        │
        ▼
Per-review keyword scoring  (score_review_with_dictionary)
        │
        ▼
PostgreSQL `review_culture_scores` table  (one row per review, 33 dimension columns)
        │
        ▼
Company-level aggregation  (SQL AVG per dimension + evidence counts)
        │
        ▼
Normalisation & display transforms  (MIT sector-relative rescale, bipolar → 0-10)
        │
        ▼
Culture ↔ performance modelling  (Pearson correlations, correlation-weighted
composite culture scores, OLS regression per industry group)
```

## 2. Data Sources

- **Reviews**: extracted from Glassdoor via the OpenWeb Ninja Real-Time Glassdoor Data API
  (primary; endpoints `company-reviews`, `company-search`, `company-overview`) with the
  RapidAPI Real-Time Glassdoor Data host as fallback. Each review stores: review text
  (pros + cons + headline), overall star rating, sub-ratings (work/life balance, culture &
  values, career opportunities, compensation & benefits, senior management), current/former
  employee flag, date, job title, location, and raw JSON payload.
- **Company universe**: 2,442 MSCI-listed companies across 11 GICS sectors / 73 industries /
  158 sub-industries, plus ~15 unlisted asset managers treated as a separate
  "Asset Management" category.
- **Performance data**: Financial Modeling Prep (FMP) API (ROE, operating margin, TSR,
  revenue growth, market cap; ISIN→ticker resolution) supplemented by an Excel workbook for
  asset managers (AUM growth, financials, shareholder returns). Stored in
  `fmp_performance_metrics` with a `data_source` flag ('fmp' or 'excel').

## 3. Text Preparation

Scoring input is the concatenated review text (headline + pros + cons). The only
pre-processing is lower-casing:

```python
text_lower = review_text.lower()
```

Keyword matching is **substring matching** on the lower-cased text (`phrase in text_lower`)
— no tokenisation, stemming, or lemmatisation. Multi-word phrases (e.g. "manual processes")
match as contiguous substrings. Each dictionary phrase counts **at most once per review**
(presence, not frequency).

## 4. Dictionary Construction

- Base dictionaries were expanded with embedding-similarity candidates and expert review
  (dictionary version 2026-04-20). Source: `keyword_expansion_output/final_keyword_dictionary.csv`.
- Each term carries an **effective weight**:

  `effective_weight = (expert_override or proposed_weight) / (1 + 0.5 × cross_load_count)`

  where `cross_load_count` is the number of *other* dimensions the same term also loads on.
  This down-weights ambiguous terms that appear in multiple dimension dictionaries.
- Schroders dictionary terms were derived from a curated Excel keyword workbook with
  strength ratings mapped to weights: High = 1.0, Medium = 0.75, Low = 0.25, and a
  direction flag (Positive adds to the dimension pole, Negative subtracts).

## 5. Per-Review Scoring Algorithms

All three frameworks are scored in a single pass per review (`score_review_with_dictionary`).

### 5.1 Hofstede (6 bipolar practice dimensions, score ∈ [−1, +1])

Dimensions (Pole A ↔ Pole B): process↔results, job↔employee, professional↔parochial,
open↔closed, tight↔loose, pragmatic↔normative.

For each dimension:

```
pole_A = Σ weight(term)  for every Pole-A term found in the text
pole_B = Σ weight(term)  for every Pole-B term found in the text

score  = (pole_B − pole_A) / (pole_A + pole_B)     if pole_A + pole_B > 0
score  = None (dimension not mentioned)            otherwise
evidence = pole_A + pole_B
```

The sign convention is therefore: **+1 = fully Pole B, −1 = fully Pole A**, where Pole A is
the first dictionary key and Pole B the second (see Appendix A for the exact pole order).

### 5.2 MIT Big 9 (9 unipolar dimensions, score ∈ [0, 10])

Dimensions: agility, collaboration, customer_orientation, diversity, execution, innovation,
integrity, performance, respect.

```
weighted_sum = Σ weight(term)  for every dimension term found in the text
score        = min(10, weighted_sum × 2)    (0 if no terms matched)
evidence     = weighted_sum
```

Note: raw per-review MIT scores are small in practice; company-level averages are later
rescaled relative to the sector maximum (Section 7.2).

### 5.3 Schroders (18 dimensions, score ∈ [−1, +1])

15 "attribute" dimensions (Weak↔Strong) and 3 truly bipolar dimensions. Every dimension has
a positive-direction and a negative-direction keyword set:

```
pos = Σ weight(term)  for positive-direction terms found
neg = Σ weight(term)  for negative-direction terms found

score = (pos − neg) / (pos + neg)   if pos + neg > 0, else None
evidence = pos + neg
```

+1 means the review text is entirely evidence *for* the dimension (or its "high" pole);
−1 entirely against.

### 5.4 Storage

Each scored review is written to `review_culture_scores` with one column per dimension:
6 Hofstede columns (`process_results_score`, …), 9 MIT columns (`agility_score`, …), and 18
Schroders columns (`schroders_d01_score` … `schroders_d18_score`), plus `review_id` and
`company_name`. `None` scores are stored as NULL, which is essential for correct averaging.
Scoring runs in batches (≤500 reviews per HTTP call) via a self-scheduling endpoint until
no unscored reviews remain.

## 6. Company-Level Aggregation

Company profiles are computed with SQL aggregation (not in-memory), per dimension:

- **Value** = `AVG(dimension_score)` over non-NULL rows (bipolar) — NULLs (unmentioned
  dimensions) are excluded automatically by SQL AVG. For MIT the count uses `score > 0`.
- **Evidence count** = number of reviews with a non-NULL (MIT: positive) score.
- **Confidence level** per dimension based on that count:
  - High: ≥ 50 scoring reviews
  - Medium: ≥ 20
  - Low: < 20
- **Relative confidence score (0–100)**: the dimension with the most evidence within the
  company gets 100; every other dimension is scaled proportionally
  (`evidence / max_evidence × 100`). Fallbacks estimate evidence from review counts when
  legacy rows lack evidence data.

Company-level results are cached in a PostgreSQL cache table and invalidated on rescoring.

## 7. Normalisation & Display Transforms

### 7.1 Bipolar → 0–10 display scale (Schroders)

`display = clamp((value + 1) × 5, 0, 10)` — so −1 → 0, 0 → 5, +1 → 10. Chart labels show the
"high" pole adjective (e.g. Strong Social Norms, Internally Driven, Profession-Focused).

### 7.2 MIT sector-relative normalisation

Company-average MIT values are rescaled so the best company **within the active GICS
filter group** scores 10 on each dimension:

```
max_d      = MAX over companies in group of AVG(dimension_score)   (floored at 0.01)
display_d  = company_avg_d / max_d × 10
```

### 7.3 Hofstede display

Values are displayed on the raw −1…+1 axis between the two pole labels, positioned
against empirically observed min/max ranges per dimension.

## 8. Culture ↔ Performance Modelling

### 8.1 Performance composite

Per company: metrics (ROE 5-yr avg, operating margin 5-yr avg, TSR CAGR 5-yr, AUM CAGR
where applicable) are z-normalised within business-model / sector peer groups and combined
into a composite performance score.

### 8.2 Dimension-performance correlations

For each culture dimension d and performance metric m, the Pearson correlation
(`scipy.stats.pearsonr`) is computed across companies that have both a culture value and
the performance metric, along with p-value and sample size. Correlations vs the composite
score (`corr_d`) drive the weighting below. Undefined correlations (insufficient data or
zero variance) default to 0.

### 8.3 Correlation-weighted culture score (per framework)

For a company c in an industry group G:

```
framework_score(c) = Σ_d  corr_d × ( value_d(c) − mean_d(G) )
```

i.e. each dimension's deviation from the group average, weighted by how strongly that
dimension correlates with performance. Missing Schroders values are skipped
(company-analysis endpoint) or treated as 0 (correlation-analysis endpoint).

### 8.4 Combined score

```
combined = hofstede_score × 5 + mit_score + schroders_score × 5
```

Hofstede and Schroders deviations live on a −1…+1 scale, MIT on 0–10; the ×5 factor puts
all three on comparable magnitudes.

### 8.5 Framework confidence

Per framework: confidence = Σ_d (confidence_d/100 × |corr_d|) / Σ_d |corr_d| × 100.
Combined confidence is the weight-averaged blend of the three framework confidences.

### 8.6 Group regression

Within each industry group with ≥ 5 companies, OLS regression
(`scipy.stats.linregress`) of composite performance on the culture score yields slope,
R², and p-value. Groups with zero variance in either variable are skipped (guards against
degenerate input); NaN results are discarded.

## 9. Replication Checklist

1. Store reviews with full text + ratings in PostgreSQL.
2. Implement the three scoring functions exactly as in Section 5 with the dictionaries in
   the appendices (lower-case substring matching; presence not frequency; each phrase
   counted once).
3. Persist per-review scores with NULL for unmentioned bipolar dimensions.
4. Aggregate with SQL AVG per company; track per-dimension evidence counts; apply the
   20/50-review confidence thresholds.
5. Apply the display transforms of Section 7.
6. Compute Pearson correlations vs the performance composite, then the
   correlation-weighted deviation scores and the combined score of Section 8.

---

"""


def fmt_weight(w):
    return ('%.4f' % w).rstrip('0').rstrip('.')


def term_table(d):
    lines = ["| Term | Weight |", "|---|---|"]
    for term, w in sorted(d.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"| {term} | {fmt_weight(w)} |")
    return "\n".join(lines)


parts = [PROSE]

# Appendix A — Hofstede
parts.append("\n# Appendix A — Hofstede Keyword Dictionaries (complete)\n")
parts.append("Score = (Pole B − Pole A) / (Pole A + Pole B). Pole A is listed first.\n")
total_h = 0
for dim, poles in HOFSTEDE_DIMENSIONS.items():
    pole_keys = list(poles.keys())
    parts.append(f"\n## A.{list(HOFSTEDE_DIMENSIONS).index(dim)+1} `{dim}`  (Pole A = {pole_keys[0]}, Pole B = {pole_keys[1]})\n")
    for i, pk in enumerate(pole_keys):
        label = "Pole A (score → −1)" if i == 0 else "Pole B (score → +1)"
        parts.append(f"\n### {pk} — {label} — {len(poles[pk])} terms\n")
        parts.append(term_table(poles[pk]))
        total_h += len(poles[pk])
parts.append(f"\n*Total Hofstede terms: {total_h}*\n")

# Appendix B — MIT
parts.append("\n# Appendix B — MIT Big 9 Keyword Dictionaries (complete)\n")
parts.append("Score = min(10, 2 × Σ matched weights).\n")
total_m = 0
for i, (dim, kws) in enumerate(MIT_BIG_9_KEYWORDS.items()):
    parts.append(f"\n## B.{i+1} `{dim}` — {len(kws)} terms\n")
    parts.append(term_table(kws))
    total_m += len(kws)
parts.append(f"\n*Total MIT terms: {total_m}*\n")

# Appendix C — Schroders
parts.append("\n# Appendix C — Schroders 18-Dimension Keyword Dictionaries (complete)\n")
parts.append("Score = (pos − neg) / (pos + neg). Weights: High = 1.0, Medium = 0.75, Low = 0.25.\n")
total_s = 0
for i, dim in enumerate(SCHRODERS_DIMENSIONS):
    info = SCHRODERS_DIM_INFO[dim]
    kw = SCHRODERS_KEYWORDS[dim]
    parts.append(f"\n## C.{i+1} `{dim}` — {info['title']} ({info['type']}; low = {info['left_label']}, high = {info['right_label']})\n")
    parts.append(f"\n*{info.get('description','')}*\n")
    parts.append(f"\n### Positive-direction terms (push score toward +1 / \"{info['right_label']}\") — {len(kw['positive'])} terms\n")
    parts.append(term_table(kw["positive"]))
    parts.append(f"\n### Negative-direction terms (push score toward −1 / \"{info['left_label']}\") — {len(kw['negative'])} terms\n")
    parts.append(term_table(kw["negative"]))
    total_s += len(kw["positive"]) + len(kw["negative"])
parts.append(f"\n*Total Schroders terms: {total_s}*\n")

with open(OUT, "w") as f:
    f.write("\n".join(parts))

print(f"Wrote {OUT}: {total_h} Hofstede + {total_m} MIT + {total_s} Schroders terms")
