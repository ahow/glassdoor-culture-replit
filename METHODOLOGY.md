# Culture Analytics Dashboard — Methodology & Technical Specification

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



# Appendix A — Hofstede Keyword Dictionaries (complete)

Score = (Pole B − Pole A) / (Pole A + Pole B). Pole A is listed first.


## A.1 `process_results`  (Pole A = process_oriented, Pole B = results_oriented)


### process_oriented — Pole A (score → −1) — 171 terms

| Term | Weight |
|---|---|
| documentation | 1 |
| procedures | 0.6667 |
| assurance | 0.5 |
| automated | 0.5 |
| bureaucratic processes | 0.5 |
| implementing | 0.5 |
| making process | 0.5 |
| manual processes | 0.5 |
| manual work | 0.5 |
| methodologies | 0.5 |
| methodology | 0.5 |
| methods | 0.5 |
| paperwork | 0.5 |
| processes | 0.5 |
| program | 0.5 |
| programs | 0.5 |
| software | 0.5 |
| softwares | 0.5 |
| some processes | 0.5 |
| workflow | 0.5 |
| bureaucratic | 0.4 |
| compliance | 0.4 |
| advisory | 0.3333 |
| formal | 0.3333 |
| guidance | 0.3333 |
| guidelines | 0.3333 |
| introduction | 0.3333 |
| planning | 0.3333 |
| policies | 0.3333 |
| procedure | 0.3333 |
| process | 0.3333 |
| process driven | 0.3333 |
| regulations | 0.3333 |
| structured | 0.3333 |
| systems | 0.3333 |
| and procedures | 0.25 |
| and processes | 0.25 |
| approval | 0.25 |
| approvals | 0.25 |
| automation | 0.25 |
| checking | 0.25 |
| commission and | 0.25 |
| decision makers | 0.25 |
| decisions made | 0.25 |
| documented | 0.25 |
| execution | 0.25 |
| flow and | 0.25 |
| follows | 0.25 |
| form | 0.25 |
| forms | 0.25 |
| guidance from | 0.25 |
| guide | 0.25 |
| implemented | 0.25 |
| interactive | 0.25 |
| long process | 0.25 |
| manual | 0.25 |
| much process | 0.25 |
| operations | 0.25 |
| overview | 0.25 |
| paper | 0.25 |
| practices | 0.25 |
| process improvement | 0.25 |
| process oriented | 0.25 |
| process was | 0.25 |
| processes and | 0.25 |
| processes are | 0.25 |
| processes that | 0.25 |
| program and | 0.25 |
| programs and | 0.25 |
| requirement | 0.25 |
| review and | 0.25 |
| rules | 0.25 |
| schedules and | 0.25 |
| software and | 0.25 |
| steps | 0.25 |
| systematic | 0.25 |
| the form | 0.25 |
| the program | 0.25 |
| the software | 0.25 |
| action | 0.2 |
| approaches | 0.2 |
| bureaucracy | 0.2 |
| decision making | 0.2 |
| making decisions | 0.2 |
| operational | 0.2 |
| work through | 0.2 |
| actions | 0.1667 |
| and process | 0.1667 |
| and structured | 0.1667 |
| approach | 0.1667 |
| communicated | 0.1667 |
| comprehensive | 0.1667 |
| criteria | 0.1667 |
| ensuring | 0.1667 |
| guidance and | 0.1667 |
| implement | 0.1667 |
| involves | 0.1667 |
| managements | 0.1667 |
| meetings and | 0.1667 |
| operating | 0.1667 |
| operation | 0.1667 |
| organized and | 0.1667 |
| oversight | 0.1667 |
| policies and | 0.1667 |
| policy | 0.1667 |
| policy and | 0.1667 |
| procedures and | 0.1667 |
| process and | 0.1667 |
| process good | 0.1667 |
| processes for | 0.1667 |
| processing | 0.1667 |
| security and | 0.1667 |
| structured and | 0.1667 |
| supervision | 0.1667 |
| systems and | 0.1667 |
| the following | 0.1667 |
| the policies | 0.1667 |
| the processes | 0.1667 |
| the systems | 0.1667 |
| very structured | 0.1667 |
| approachable and | 0.125 |
| bureaucratic and | 0.125 |
| compliance and | 0.125 |
| decisions that | 0.125 |
| enforced | 0.125 |
| from management | 0.125 |
| hectic work | 0.125 |
| how work | 0.125 |
| operations and | 0.125 |
| practices and | 0.125 |
| prepared work | 0.125 |
| process for | 0.125 |
| process the | 0.125 |
| process you | 0.125 |
| rules and | 0.125 |
| the bureaucracy | 0.125 |
| the process | 0.125 |
| the technical | 0.125 |
| decisions | 0.1111 |
| approach and | 0.1 |
| coordination | 0.1 |
| decision | 0.1 |
| design | 0.1 |
| detail | 0.1 |
| evaluations | 0.1 |
| organization and | 0.1 |
| planning and | 0.1 |
| task | 0.1 |
| the operations | 0.1 |
| the rules | 0.1 |
| bureaucracy and | 0.0833 |
| evaluation | 0.0833 |
| handling | 0.0833 |
| knowledge and | 0.0833 |
| straightforward | 0.0833 |
| tasks | 0.0833 |
| tasks and | 0.0833 |
| tasks that | 0.0833 |
| the task | 0.0833 |
| the tasks | 0.0833 |
| with management | 0.0833 |
| your management | 0.0833 |
| advanced | 0.0714 |
| management | 0.0714 |
| operate | 0.0714 |
| the decisions | 0.0714 |
| organizational | 0.0625 |
| management and | 0.0556 |
| responsibilities and | 0.0556 |
| responsibility and | 0.0556 |
| decisions and | 0.05 |

### results_oriented — Pole B (score → +1) — 167 terms

| Term | Weight |
|---|---|
| goals | 1 |
| impact | 1 |
| targets | 1 |
| outcomes | 0.6667 |
| achievements | 0.5 |
| ambitions | 0.5 |
| goal | 0.5 |
| impacting | 0.5 |
| impacts | 0.5 |
| improve | 0.5 |
| outcome | 0.5 |
| result | 0.5 |
| reward | 0.5 |
| winning | 0.5 |
| accomplish | 0.3333 |
| accomplishments | 0.3333 |
| competition | 0.3333 |
| delivering | 0.3333 |
| individual performance | 0.3333 |
| insights | 0.3333 |
| objective | 0.3333 |
| opportunity | 0.3333 |
| performance management | 0.3333 |
| planning | 0.3333 |
| research | 0.3333 |
| results | 0.3333 |
| sales goals | 0.3333 |
| strategies | 0.3333 |
| succeed | 0.3333 |
| teamwork | 0.3333 |
| things done | 0.3333 |
| accountability | 0.25 |
| advancement | 0.25 |
| ambition | 0.25 |
| and result | 0.25 |
| based performance | 0.25 |
| business decisions | 0.25 |
| decision makers | 0.25 |
| decisions made | 0.25 |
| execution | 0.25 |
| feedback | 0.25 |
| goals and | 0.25 |
| goals are | 0.25 |
| impactful | 0.25 |
| objectives | 0.25 |
| opportunities progress | 0.25 |
| perform | 0.25 |
| perform well | 0.25 |
| performance driven | 0.25 |
| progressing | 0.25 |
| response | 0.25 |
| result the | 0.25 |
| reward for | 0.25 |
| strategic | 0.25 |
| strategy | 0.25 |
| successful | 0.25 |
| tasks good | 0.25 |
| the goal | 0.25 |
| the goals | 0.25 |
| the result | 0.25 |
| with opportunity | 0.25 |
| your goals | 0.25 |
| achievement | 0.2 |
| action | 0.2 |
| advancing | 0.2 |
| capability | 0.2 |
| decision | 0.2 |
| decision making | 0.2 |
| detail | 0.2 |
| development | 0.2 |
| evaluations | 0.2 |
| from leadership | 0.2 |
| functioning | 0.2 |
| improvement | 0.2 |
| improving | 0.2 |
| making decisions | 0.2 |
| operational | 0.2 |
| productivity | 0.2 |
| success | 0.2 |
| task | 0.2 |
| achieved | 0.1667 |
| actions | 0.1667 |
| and develop | 0.1667 |
| and performance | 0.1667 |
| career you | 0.1667 |
| doing something | 0.1667 |
| effectively | 0.1667 |
| evaluation | 0.1667 |
| expectations and | 0.1667 |
| expectations for | 0.1667 |
| feedback and | 0.1667 |
| for success | 0.1667 |
| hardworking | 0.1667 |
| opportunity and | 0.1667 |
| perform the | 0.1667 |
| performance | 0.1667 |
| plans and | 0.1667 |
| processing | 0.1667 |
| productive | 0.1667 |
| progress | 0.1667 |
| promising | 0.1667 |
| promotion and | 0.1667 |
| situations | 0.1667 |
| succeed and | 0.1667 |
| success the | 0.1667 |
| tasks | 0.1667 |
| the competition | 0.1667 |
| the decision | 0.1667 |
| the expectations | 0.1667 |
| your success | 0.1667 |
| achieving | 0.1429 |
| determined | 0.1429 |
| management | 0.1429 |
| operate | 0.1429 |
| production | 0.1429 |
| responsibility | 0.1429 |
| advancement you | 0.125 |
| based work | 0.125 |
| compete with | 0.125 |
| decisions that | 0.125 |
| for improvement | 0.125 |
| for management | 0.125 |
| from management | 0.125 |
| how work | 0.125 |
| opportunities the | 0.125 |
| process for | 0.125 |
| process the | 0.125 |
| process you | 0.125 |
| product | 0.125 |
| the process | 0.125 |
| with leadership | 0.125 |
| achieve | 0.1111 |
| decisions | 0.1111 |
| approach and | 0.1 |
| approaches | 0.1 |
| creativity and | 0.1 |
| efficiency and | 0.1 |
| for advancement | 0.1 |
| hardworking and | 0.1 |
| improvement and | 0.1 |
| job | 0.1 |
| job work | 0.1 |
| learning and | 0.1 |
| opportunities for | 0.1 |
| performance but | 0.1 |
| planning and | 0.1 |
| progress and | 0.1 |
| responsibilities | 0.1 |
| strategy and | 0.1 |
| success and | 0.1 |
| the operations | 0.1 |
| the success | 0.1 |
| your performance | 0.1 |
| efficient | 0.0833 |
| performance and | 0.0833 |
| performance the | 0.0833 |
| tasks and | 0.0833 |
| tasks that | 0.0833 |
| the performance | 0.0833 |
| the task | 0.0833 |
| the tasks | 0.0833 |
| accountability for | 0.0714 |
| competition and | 0.0714 |
| the decisions | 0.0714 |
| responsibilities and | 0.0556 |
| responsibility and | 0.0556 |
| decisions and | 0.05 |

## A.2 `job_employee`  (Pole A = employee_oriented, Pole B = job_oriented)


### employee_oriented — Pole A (score → −1) — 192 terms

| Term | Weight |
|---|---|
| employee development | 1 |
| employee growth | 1 |
| employee satisfaction | 1 |
| personal development | 1 |
| wellbeing | 1 |
| about employees | 0.75 |
| employee friendly | 0.75 |
| employee well | 0.75 |
| about employee | 0.5 |
| among employees | 0.5 |
| benefits employees | 0.5 |
| best workplace | 0.5 |
| between employees | 0.5 |
| care employees | 0.5 |
| coworkers | 0.5 |
| current employees | 0.5 |
| dedicated employees | 0.5 |
| employ | 0.5 |
| employed | 0.5 |
| employee | 0.5 |
| employee appreciation | 0.5 |
| employee benefit | 0.5 |
| employee care | 0.5 |
| employee centric | 0.5 |
| employee engagement | 0.5 |
| employee experience | 0.5 |
| employee focused | 0.5 |
| employee good | 0.5 |
| employee morale | 0.5 |
| employee needs | 0.5 |
| employee oriented | 0.5 |
| employee perks | 0.5 |
| employee resource | 0.5 |
| employee support | 0.5 |
| employee training | 0.5 |
| employee welfare | 0.5 |
| employee work | 0.5 |
| employees | 0.5 |
| employees benefits | 0.5 |
| employees feel | 0.5 |
| employees get | 0.5 |
| employees good | 0.5 |
| employees great | 0.5 |
| employees like | 0.5 |
| employees management | 0.5 |
| employees many | 0.5 |
| employees more | 0.5 |
| employees some | 0.5 |
| employees which | 0.5 |
| excellent workplace | 0.5 |
| friendly employees | 0.5 |
| friendly working | 0.5 |
| good employee | 0.5 |
| good employees | 0.5 |
| good workplace | 0.5 |
| great employee | 0.5 |
| great employees | 0.5 |
| great workplace | 0.5 |
| hardworking people | 0.5 |
| healthy working | 0.5 |
| help employees | 0.5 |
| human resources | 0.5 |
| job satisfaction | 0.5 |
| level employee | 0.5 |
| level employees | 0.5 |
| management people | 0.5 |
| management supportive | 0.5 |
| new employees | 0.5 |
| nice employees | 0.5 |
| nice workplace | 0.5 |
| other employees | 0.5 |
| people development | 0.5 |
| people management | 0.5 |
| people managers | 0.5 |
| personal growth | 0.5 |
| supportive colleagues | 0.5 |
| supportive management | 0.5 |
| supportive managers | 0.5 |
| supportive staff | 0.5 |
| treat employees | 0.5 |
| treats employees | 0.5 |
| work balance | 0.5 |
| work culture | 0.5 |
| work employee | 0.5 |
| work managers | 0.5 |
| working culture | 0.5 |
| working employees | 0.5 |
| working life | 0.5 |
| worklife balance | 0.5 |
| workplace culture | 0.5 |
| workplace environment | 0.5 |
| workplace good | 0.5 |
| young employees | 0.5 |
| about work | 0.3333 |
| company employee | 0.3333 |
| company working | 0.3333 |
| employees company | 0.3333 |
| employees well | 0.3333 |
| focus employee | 0.3333 |
| having work | 0.3333 |
| loyal employees | 0.3333 |
| maintain work | 0.3333 |
| manage work | 0.3333 |
| office work | 0.3333 |
| organisation work | 0.3333 |
| people career | 0.3333 |
| stressful work | 0.3333 |
| supportive environment | 0.3333 |
| value employees | 0.3333 |
| values employees | 0.3333 |
| work colleagues | 0.3333 |
| work enviornment | 0.3333 |
| work environment | 0.3333 |
| work friendly | 0.3333 |
| work job | 0.3333 |
| work life | 0.3333 |
| work management | 0.3333 |
| work people | 0.3333 |
| work related | 0.3333 |
| work style | 0.3333 |
| working environment | 0.3333 |
| working place | 0.3333 |
| working work | 0.3333 |
| worklife | 0.3333 |
| workplace | 0.3 |
| being hired | 0.25 |
| career work | 0.25 |
| company people | 0.25 |
| company work | 0.25 |
| doing work | 0.25 |
| employee and | 0.25 |
| employee not | 0.25 |
| employee the | 0.25 |
| employee who | 0.25 |
| employee with | 0.25 |
| employee you | 0.25 |
| employees and | 0.25 |
| employees are | 0.25 |
| employees but | 0.25 |
| employees for | 0.25 |
| employees have | 0.25 |
| employees that | 0.25 |
| employees the | 0.25 |
| employees they | 0.25 |
| employees this | 0.25 |
| employees very | 0.25 |
| employees when | 0.25 |
| employees who | 0.25 |
| employees with | 0.25 |
| employees you | 0.25 |
| employer for | 0.25 |
| experienced employees | 0.25 |
| for employee | 0.25 |
| for employees | 0.25 |
| make work | 0.25 |
| manage people | 0.25 |
| management work | 0.25 |
| managing people | 0.25 |
| morale | 0.25 |
| not employee | 0.25 |
| profession | 0.25 |
| supportive work | 0.25 |
| talented employees | 0.25 |
| the employee | 0.25 |
| the employees | 0.25 |
| very employee | 0.25 |
| who work | 0.25 |
| with employee | 0.25 |
| with employees | 0.25 |
| work career | 0.25 |
| work company | 0.25 |
| work ethic | 0.25 |
| work quality | 0.25 |
| working experience | 0.25 |
| working people | 0.25 |
| working together | 0.25 |
| company doing | 0.2 |
| productivity | 0.2 |
| its employees | 0.1667 |
| the morale | 0.1667 |
| their employee | 0.1667 |
| their employees | 0.1667 |
| when working | 0.1667 |
| work being | 0.1667 |
| workplace for | 0.1667 |
| based work | 0.125 |
| morale and | 0.125 |
| organizational | 0.125 |
| workplace with | 0.125 |
| workplace and | 0.1 |
| job and | 0.0833 |
| the workplace | 0.0833 |

### job_oriented — Pole B (score → +1) — 178 terms

| Term | Weight |
|---|---|
| about working | 0.5 |
| balanced work | 0.5 |
| best working | 0.5 |
| between work | 0.5 |
| center work | 0.5 |
| challenging tasks | 0.5 |
| comfortable work | 0.5 |
| demanding work | 0.5 |
| difficult work | 0.5 |
| employee focused | 0.5 |
| find work | 0.5 |
| focus | 0.5 |
| focus work | 0.5 |
| from working | 0.5 |
| get work | 0.5 |
| getting work | 0.5 |
| hard work | 0.5 |
| hard working | 0.5 |
| hardwork | 0.5 |
| heavy work | 0.5 |
| learn work | 0.5 |
| less work | 0.5 |
| like work | 0.5 |
| low work | 0.5 |
| more focused | 0.5 |
| mundane work | 0.5 |
| off work | 0.5 |
| office | 0.5 |
| overworking | 0.5 |
| paced work | 0.5 |
| pay work | 0.5 |
| place work | 0.5 |
| project work | 0.5 |
| projects work | 0.5 |
| repetitive tasks | 0.5 |
| repetitive work | 0.5 |
| rewarding work | 0.5 |
| training work | 0.5 |
| want work | 0.5 |
| ways working | 0.5 |
| while working | 0.5 |
| work | 0.5 |
| work call | 0.5 |
| work challenging | 0.5 |
| work depends | 0.5 |
| work difficult | 0.5 |
| work done | 0.5 |
| work expected | 0.5 |
| work flow | 0.5 |
| work from | 0.5 |
| work get | 0.5 |
| work gets | 0.5 |
| work hard | 0.5 |
| work harder | 0.5 |
| work isn | 0.5 |
| work job | 0.5 |
| work lack | 0.5 |
| work less | 0.5 |
| work loads | 0.5 |
| work model | 0.5 |
| work most | 0.5 |
| work need | 0.5 |
| work opportunity | 0.5 |
| work options | 0.5 |
| work repetitive | 0.5 |
| work required | 0.5 |
| work smart | 0.5 |
| work time | 0.5 |
| work timings | 0.5 |
| work which | 0.5 |
| work while | 0.5 |
| work working | 0.5 |
| working from | 0.5 |
| working hard | 0.5 |
| working state | 0.5 |
| working time | 0.5 |
| working work | 0.5 |
| workload | 0.5 |
| workload good | 0.5 |
| workloads | 0.5 |
| productivity | 0.4 |
| doing work | 0.375 |
| make work | 0.375 |
| about work | 0.3333 |
| employee work | 0.3333 |
| engaging work | 0.3333 |
| focus employee | 0.3333 |
| focused | 0.3333 |
| focussed | 0.3333 |
| having work | 0.3333 |
| high work | 0.3333 |
| into work | 0.3333 |
| job performance | 0.3333 |
| jobs | 0.3333 |
| maintain work | 0.3333 |
| manage work | 0.3333 |
| open work | 0.3333 |
| organisation work | 0.3333 |
| out work | 0.3333 |
| professional work | 0.3333 |
| reasonable work | 0.3333 |
| stressful work | 0.3333 |
| tech work | 0.3333 |
| technical work | 0.3333 |
| work closely | 0.3333 |
| work enviornment | 0.3333 |
| work environment | 0.3333 |
| work friendly | 0.3333 |
| work life | 0.3333 |
| work management | 0.3333 |
| work people | 0.3333 |
| work within | 0.3333 |
| workforce | 0.3333 |
| working | 0.3333 |
| working environment | 0.3333 |
| working life | 0.3333 |
| working place | 0.3333 |
| worklife | 0.3333 |
| job work | 0.3 |
| task | 0.3 |
| are focused | 0.25 |
| are working | 0.25 |
| company work | 0.25 |
| flexible working | 0.25 |
| focused the | 0.25 |
| for working | 0.25 |
| have work | 0.25 |
| jobs and | 0.25 |
| management work | 0.25 |
| organization work | 0.25 |
| prepared work | 0.25 |
| productive | 0.25 |
| supportive work | 0.25 |
| tasks | 0.25 |
| tasks are | 0.25 |
| tasks can | 0.25 |
| tasks good | 0.25 |
| the jobs | 0.25 |
| work and | 0.25 |
| work are | 0.25 |
| work company | 0.25 |
| work ethic | 0.25 |
| work quality | 0.25 |
| work work | 0.25 |
| working and | 0.25 |
| working experience | 0.25 |
| working people | 0.25 |
| working together | 0.25 |
| employment | 0.2 |
| job | 0.2 |
| work through | 0.2 |
| workplace | 0.2 |
| focus and | 0.1667 |
| focused and | 0.1667 |
| hardworking | 0.1667 |
| processing | 0.1667 |
| productivity and | 0.1667 |
| the job | 0.1667 |
| the work | 0.1667 |
| the workforce | 0.1667 |
| the working | 0.1667 |
| when working | 0.1667 |
| with work | 0.1667 |
| with working | 0.1667 |
| work being | 0.1667 |
| working the | 0.1667 |
| workplace for | 0.1667 |
| operate | 0.1429 |
| jobs the | 0.125 |
| hardworking and | 0.1 |
| workplace and | 0.1 |
| job and | 0.0833 |
| tasks and | 0.0833 |
| tasks that | 0.0833 |
| the task | 0.0833 |
| the tasks | 0.0833 |
| the workplace | 0.0833 |
| working with | 0.0833 |

## A.3 `professional_parochial`  (Pole A = parochial, Pole B = professional)


### parochial — Pole A (score → −1) — 172 terms

| Term | Weight |
|---|---|
| company culture | 1 |
| based company | 0.75 |
| companies work | 0.75 |
| company environment | 0.75 |
| company from | 0.75 |
| company growing | 0.75 |
| company hard | 0.75 |
| company itself | 0.75 |
| company management | 0.75 |
| company name | 0.75 |
| company politics | 0.75 |
| corporate company | 0.75 |
| corporate culture | 0.75 |
| corporate world | 0.75 |
| corporation | 0.75 |
| company values | 0.6667 |
| about company | 0.5 |
| another company | 0.5 |
| best company | 0.5 |
| better company | 0.5 |
| companies | 0.5 |
| companies good | 0.5 |
| company all | 0.5 |
| company also | 0.5 |
| company always | 0.5 |
| company bad | 0.5 |
| company because | 0.5 |
| company big | 0.5 |
| company cons | 0.5 |
| company decent | 0.5 |
| company difficult | 0.5 |
| company easy | 0.5 |
| company even | 0.5 |
| company ever | 0.5 |
| company extremely | 0.5 |
| company feel | 0.5 |
| company get | 0.5 |
| company gives | 0.5 |
| company going | 0.5 |
| company grow | 0.5 |
| company growth | 0.5 |
| company high | 0.5 |
| company large | 0.5 |
| company learn | 0.5 |
| company like | 0.5 |
| company long | 0.5 |
| company looks | 0.5 |
| company lot | 0.5 |
| company makes | 0.5 |
| company match | 0.5 |
| company more | 0.5 |
| company nice | 0.5 |
| company nothing | 0.5 |
| company overall | 0.5 |
| company poor | 0.5 |
| company promotes | 0.5 |
| company really | 0.5 |
| company reputation | 0.5 |
| company some | 0.5 |
| company sometimes | 0.5 |
| company start | 0.5 |
| company still | 0.5 |
| company structure | 0.5 |
| company takes | 0.5 |
| company well | 0.5 |
| company wide | 0.5 |
| corp | 0.5 |
| corporate | 0.5 |
| corporate america | 0.5 |
| corporate environment | 0.5 |
| corporate structure | 0.5 |
| corporations | 0.5 |
| employees company | 0.5 |
| established company | 0.5 |
| every company | 0.5 |
| firm culture | 0.5 |
| firms | 0.5 |
| first company | 0.5 |
| friendly company | 0.5 |
| from company | 0.5 |
| good corporate | 0.5 |
| growing company | 0.5 |
| management company | 0.5 |
| management firm | 0.5 |
| many companies | 0.5 |
| most companies | 0.5 |
| name company | 0.5 |
| oriented company | 0.5 |
| other companies | 0.5 |
| other company | 0.5 |
| other firms | 0.5 |
| overall company | 0.5 |
| parent company | 0.5 |
| pay company | 0.5 |
| prestigious company | 0.5 |
| prestigious firm | 0.5 |
| private company | 0.5 |
| respected company | 0.5 |
| run company | 0.5 |
| same company | 0.5 |
| similar companies | 0.5 |
| strong company | 0.5 |
| typical corporate | 0.5 |
| within company | 0.5 |
| company | 0.375 |
| company people | 0.375 |
| company work | 0.375 |
| work company | 0.375 |
| company amazing | 0.3333 |
| company awesome | 0.3333 |
| company best | 0.3333 |
| company brand | 0.3333 |
| company cares | 0.3333 |
| company employee | 0.3333 |
| company encourages | 0.3333 |
| company good | 0.3333 |
| company great | 0.3333 |
| company just | 0.3333 |
| company many | 0.3333 |
| company needs | 0.3333 |
| company one | 0.3333 |
| company seems | 0.3333 |
| company truly | 0.3333 |
| company used | 0.3333 |
| company whole | 0.3333 |
| company worked | 0.3333 |
| company working | 0.3333 |
| corporate job | 0.3333 |
| corporate trust | 0.3333 |
| strong corporate | 0.3333 |
| team company | 0.3333 |
| whole company | 0.3333 |
| company doing | 0.3 |
| and company | 0.25 |
| companies and | 0.25 |
| companies are | 0.25 |
| companies but | 0.25 |
| companies the | 0.25 |
| company are | 0.25 |
| company but | 0.25 |
| company for | 0.25 |
| company its | 0.25 |
| company not | 0.25 |
| company should | 0.25 |
| company that | 0.25 |
| company the | 0.25 |
| company they | 0.25 |
| company was | 0.25 |
| company when | 0.25 |
| company who | 0.25 |
| corporate and | 0.25 |
| corporate experience | 0.25 |
| corporation with | 0.25 |
| diverse company | 0.25 |
| firms and | 0.25 |
| firms the | 0.25 |
| not company | 0.25 |
| that company | 0.25 |
| the companies | 0.25 |
| the corporate | 0.25 |
| the firm | 0.25 |
| very corporate | 0.25 |
| with company | 0.25 |
| company and | 0.1667 |
| company company | 0.1667 |
| company very | 0.1667 |
| for company | 0.1667 |
| the company | 0.1667 |
| this company | 0.1667 |
| company with | 0.125 |
| organizational | 0.125 |
| the organisation | 0.125 |

### professional — Pole B (score → +1) — 168 terms

| Term | Weight |
|---|---|
| career advancement | 1 |
| expert | 1 |
| industry knowledge | 1 |
| professional | 1 |
| professional development | 1 |
| specialist | 1 |
| technical skills | 1 |
| experienced professionals | 0.75 |
| good professional | 0.75 |
| great professional | 0.75 |
| highly professional | 0.75 |
| professional career | 0.75 |
| professional environment | 0.75 |
| professional experience | 0.75 |
| professional good | 0.75 |
| professional growth | 0.75 |
| professional life | 0.75 |
| professional people | 0.75 |
| professional working | 0.75 |
| professionalism | 0.75 |
| professionals | 0.75 |
| about career | 0.5 |
| academic | 0.5 |
| advance career | 0.5 |
| advancement opportunities | 0.5 |
| analyst | 0.5 |
| analyst associate | 0.5 |
| analyst experience | 0.5 |
| analyst intern | 0.5 |
| analyst role | 0.5 |
| begin career | 0.5 |
| build career | 0.5 |
| career development | 0.5 |
| career good | 0.5 |
| career great | 0.5 |
| career growth | 0.5 |
| career long | 0.5 |
| career nothing | 0.5 |
| career opportunities | 0.5 |
| career opportunity | 0.5 |
| career path | 0.5 |
| career progress | 0.5 |
| career progression | 0.5 |
| career prospects | 0.5 |
| career wise | 0.5 |
| careers | 0.5 |
| certification | 0.5 |
| certifications | 0.5 |
| clear career | 0.5 |
| competency | 0.5 |
| develop career | 0.5 |
| developer | 0.5 |
| engineer | 0.5 |
| excellent career | 0.5 |
| experts | 0.5 |
| future career | 0.5 |
| good career | 0.5 |
| growth career | 0.5 |
| highly skilled | 0.5 |
| investment professional | 0.5 |
| investment professionals | 0.5 |
| less career | 0.5 |
| limited career | 0.5 |
| long career | 0.5 |
| lots career | 0.5 |
| make career | 0.5 |
| management career | 0.5 |
| many career | 0.5 |
| much career | 0.5 |
| one career | 0.5 |
| operations specialist | 0.5 |
| opportunities career | 0.5 |
| own career | 0.5 |
| pay career | 0.5 |
| professional atmosphere | 0.5 |
| professional culture | 0.5 |
| professional work | 0.5 |
| professionally | 0.5 |
| programmer | 0.5 |
| progress career | 0.5 |
| relationship specialist | 0.5 |
| research analyst | 0.5 |
| salary career | 0.5 |
| senior analyst | 0.5 |
| skill development | 0.5 |
| software developer | 0.5 |
| software engineer | 0.5 |
| specialists | 0.5 |
| tech | 0.5 |
| tech people | 0.5 |
| technical knowledge | 0.5 |
| technologist | 0.5 |
| technologists | 0.5 |
| technology analyst | 0.5 |
| work analyst | 0.5 |
| young professionals | 0.5 |
| career | 0.375 |
| profession | 0.375 |
| competent people | 0.3333 |
| consultant | 0.3333 |
| consultants | 0.3333 |
| industry experience | 0.3333 |
| people career | 0.3333 |
| skilled | 0.3333 |
| skilled people | 0.3333 |
| talented colleagues | 0.3333 |
| tech work | 0.3333 |
| technical | 0.3333 |
| technical people | 0.3333 |
| technical work | 0.3333 |
| expertise | 0.3 |
| analyst and | 0.25 |
| and professional | 0.25 |
| and professionalism | 0.25 |
| are professional | 0.25 |
| career but | 0.25 |
| career can | 0.25 |
| career here | 0.25 |
| career not | 0.25 |
| career there | 0.25 |
| career they | 0.25 |
| career very | 0.25 |
| career work | 0.25 |
| experienced employees | 0.25 |
| for professional | 0.25 |
| have career | 0.25 |
| industry they | 0.25 |
| not career | 0.25 |
| not professional | 0.25 |
| professional and | 0.25 |
| professionalism and | 0.25 |
| professionally and | 0.25 |
| professionals and | 0.25 |
| skill | 0.25 |
| talented employees | 0.25 |
| tech and | 0.25 |
| the tech | 0.25 |
| their careers | 0.25 |
| there career | 0.25 |
| very professional | 0.25 |
| with career | 0.25 |
| work career | 0.25 |
| working people | 0.25 |
| you career | 0.25 |
| your professional | 0.25 |
| executives | 0.2 |
| qualified | 0.2 |
| skills | 0.2 |
| technological | 0.2 |
| technology work | 0.2 |
| career for | 0.1667 |
| career you | 0.1667 |
| competence | 0.1667 |
| skilled and | 0.1667 |
| skills the | 0.1667 |
| technical and | 0.1667 |
| your career | 0.1667 |
| career and | 0.125 |
| career with | 0.125 |
| competent and | 0.125 |
| skills and | 0.125 |
| the career | 0.125 |
| the skills | 0.125 |
| the technical | 0.125 |
| career the | 0.1 |
| expertise and | 0.1 |
| for advancement | 0.1 |
| management and | 0.0556 |

## A.4 `open_closed`  (Pole A = closed_system, Pole B = open_system)


### closed_system — Pole A (score → −1) — 180 terms

| Term | Weight |
|---|---|
| cliquey | 1 |
| politics | 1 |
| resistant | 1 |
| traditional | 1 |
| breaking | 0.5 |
| outsiders | 0.5 |
| socially | 0.5 |
| strong culture | 0.5 |
| struggle | 0.5 |
| tough culture | 0.5 |
| tricky | 0.5 |
| challenging | 0.3333 |
| circumstances | 0.3333 |
| against | 0.25 |
| against the | 0.25 |
| anti | 0.25 |
| associated | 0.25 |
| associated with | 0.25 |
| away with | 0.25 |
| back culture | 0.25 |
| challenging for | 0.25 |
| colleagues very | 0.25 |
| competitive the | 0.25 |
| complicated | 0.25 |
| concerning | 0.25 |
| confused | 0.25 |
| counterparts | 0.25 |
| culture from | 0.25 |
| culture hard | 0.25 |
| culture here | 0.25 |
| culture strong | 0.25 |
| culture there | 0.25 |
| culture very | 0.25 |
| difficult and | 0.25 |
| difficult change | 0.25 |
| difficulties | 0.25 |
| discouraged | 0.25 |
| discussion | 0.25 |
| discussions | 0.25 |
| disorganised | 0.25 |
| disorganized | 0.25 |
| disorganized and | 0.25 |
| established | 0.25 |
| failure | 0.25 |
| familiar with | 0.25 |
| fashion | 0.25 |
| friendly culture | 0.25 |
| from colleagues | 0.25 |
| how the | 0.25 |
| inclusivity | 0.25 |
| inside | 0.25 |
| internal | 0.25 |
| modern | 0.25 |
| narrow | 0.25 |
| not clear | 0.25 |
| outsider | 0.25 |
| political | 0.25 |
| politically | 0.25 |
| politics the | 0.25 |
| problem with | 0.25 |
| putting | 0.25 |
| some politics | 0.25 |
| spread | 0.25 |
| struggles | 0.25 |
| stuck with | 0.25 |
| style and | 0.25 |
| surrounded | 0.25 |
| the politics | 0.25 |
| the situation | 0.25 |
| them with | 0.25 |
| ties | 0.25 |
| understand how | 0.25 |
| unknown | 0.25 |
| wall | 0.25 |
| wear | 0.25 |
| cultural | 0.2 |
| social | 0.2 |
| broad | 0.1667 |
| colleagues | 0.1667 |
| colleagues the | 0.1667 |
| colleagues with | 0.1667 |
| communities | 0.1667 |
| community | 0.1667 |
| complicated and | 0.1667 |
| confusion | 0.1667 |
| consensus | 0.1667 |
| culture | 0.1667 |
| culture all | 0.1667 |
| culture bit | 0.1667 |
| culture but | 0.1667 |
| culture changing | 0.1667 |
| culture one | 0.1667 |
| culture that | 0.1667 |
| culture the | 0.1667 |
| culture they | 0.1667 |
| culture this | 0.1667 |
| culture with | 0.1667 |
| difficult | 0.1667 |
| difficulty | 0.1667 |
| discuss | 0.1667 |
| experience people | 0.1667 |
| exposed | 0.1667 |
| involved | 0.1667 |
| isolated | 0.1667 |
| living | 0.1667 |
| mutual | 0.1667 |
| peer | 0.1667 |
| progressive | 0.1667 |
| restrictive | 0.1667 |
| strict and | 0.1667 |
| strong | 0.1667 |
| the community | 0.1667 |
| there culture | 0.1667 |
| together | 0.1667 |
| tolerated | 0.1667 |
| unclear | 0.1667 |
| understanding the | 0.1667 |
| with colleagues | 0.1667 |
| determined | 0.1429 |
| apart | 0.125 |
| apart from | 0.125 |
| being made | 0.125 |
| changing | 0.125 |
| choosing | 0.125 |
| community and | 0.125 |
| complex | 0.125 |
| conflict | 0.125 |
| cooperative | 0.125 |
| culture some | 0.125 |
| dealt with | 0.125 |
| education | 0.125 |
| enforced | 0.125 |
| experience with | 0.125 |
| groups | 0.125 |
| how work | 0.125 |
| involved with | 0.125 |
| keeping | 0.125 |
| learning | 0.125 |
| respectable | 0.125 |
| respected | 0.125 |
| rules | 0.125 |
| rules and | 0.125 |
| secure | 0.125 |
| strict | 0.125 |
| the knowledge | 0.125 |
| workplace with | 0.125 |
| authority | 0.1 |
| career the | 0.1 |
| constructive | 0.1 |
| dealing | 0.1 |
| expertise | 0.1 |
| increasingly | 0.1 |
| knowledge | 0.1 |
| society | 0.1 |
| the business | 0.1 |
| the rules | 0.1 |
| welcoming | 0.1 |
| workplace | 0.1 |
| approach | 0.0833 |
| competitive with | 0.0833 |
| culturally | 0.0833 |
| culture and | 0.0833 |
| dealing with | 0.0833 |
| experiences | 0.0833 |
| hardworking | 0.0833 |
| influence | 0.0833 |
| regarded | 0.0833 |
| respect and | 0.0833 |
| respectful | 0.0833 |
| respectful and | 0.0833 |
| situations | 0.0833 |
| straightforward | 0.0833 |
| the culture | 0.0833 |
| the workplace | 0.0833 |
| working with | 0.0833 |
| competition and | 0.0714 |
| the industry | 0.0714 |
| understanding | 0.0714 |
| decisions | 0.0556 |
| design | 0.05 |

### open_system — Pole B (score → +1) — 172 terms

| Term | Weight |
|---|---|
| receptive | 1 |
| collaborative | 0.6667 |
| inclusive | 0.6667 |
| innovative | 0.5 |
| open minded | 0.5 |
| openness | 0.5 |
| positive experience | 0.5 |
| welcoming | 0.4 |
| collaboration | 0.3333 |
| diverse | 0.3333 |
| diverse culture | 0.3333 |
| diverse work | 0.3333 |
| diversity | 0.3333 |
| diversity good | 0.3333 |
| diversity work | 0.3333 |
| engaging | 0.3333 |
| focus diversity | 0.3333 |
| forward thinking | 0.3333 |
| ideas | 0.3333 |
| innovate | 0.3333 |
| insight into | 0.3333 |
| learning new | 0.3333 |
| learnings | 0.3333 |
| minds | 0.3333 |
| new ideas | 0.3333 |
| new opportunities | 0.3333 |
| new projects | 0.3333 |
| new skills | 0.3333 |
| open culture | 0.3333 |
| perspectives | 0.3333 |
| projects | 0.3333 |
| thinking | 0.3333 |
| welcoming environment | 0.3333 |
| alternatives | 0.25 |
| awareness | 0.25 |
| challenge and | 0.25 |
| challenging and | 0.25 |
| creativity | 0.25 |
| education and | 0.25 |
| emerging | 0.25 |
| engaging and | 0.25 |
| environment learn | 0.25 |
| experiance | 0.25 |
| experience | 0.25 |
| experience from | 0.25 |
| good learning | 0.25 |
| innovation | 0.25 |
| interests | 0.25 |
| involvement | 0.25 |
| learn | 0.25 |
| learn and | 0.25 |
| learn new | 0.25 |
| learned | 0.25 |
| learning | 0.25 |
| learning from | 0.25 |
| learning opportunities | 0.25 |
| learning the | 0.25 |
| learnt | 0.25 |
| lot learning | 0.25 |
| many learning | 0.25 |
| mind | 0.25 |
| newcomers | 0.25 |
| opportunities that | 0.25 |
| opportunities you | 0.25 |
| perspective | 0.25 |
| promote the | 0.25 |
| promote within | 0.25 |
| provides opportunities | 0.25 |
| rewarding experience | 0.25 |
| the learning | 0.25 |
| the open | 0.25 |
| vision and | 0.25 |
| willingness | 0.25 |
| adapting | 0.2 |
| cultural | 0.2 |
| expertise | 0.2 |
| alternative | 0.1667 |
| and collaborative | 0.1667 |
| collaborate | 0.1667 |
| collaborate with | 0.1667 |
| collaboration and | 0.1667 |
| collaborative and | 0.1667 |
| collaborative culture | 0.1667 |
| creative | 0.1667 |
| culture collaborative | 0.1667 |
| culture open | 0.1667 |
| culture within | 0.1667 |
| develop | 0.1667 |
| developing | 0.1667 |
| different opportunities | 0.1667 |
| diverse and | 0.1667 |
| diverse environment | 0.1667 |
| diverse group | 0.1667 |
| diversity and | 0.1667 |
| diversity inclusion | 0.1667 |
| diversity the | 0.1667 |
| empowering | 0.1667 |
| experience people | 0.1667 |
| experience working | 0.1667 |
| experiences | 0.1667 |
| for diversity | 0.1667 |
| good collaboration | 0.1667 |
| good diversity | 0.1667 |
| idea what | 0.1667 |
| ideas and | 0.1667 |
| inclusive and | 0.1667 |
| inclusive work | 0.1667 |
| innovation and | 0.1667 |
| innovative ideas | 0.1667 |
| intellectual | 0.1667 |
| intellectually stimulating | 0.1667 |
| into different | 0.1667 |
| new ways | 0.1667 |
| open work | 0.1667 |
| opportunities some | 0.1667 |
| projects and | 0.1667 |
| rewarding | 0.1667 |
| rewarding and | 0.1667 |
| skilled and | 0.1667 |
| skills the | 0.1667 |
| the diversity | 0.1667 |
| the projects | 0.1667 |
| thinking and | 0.1667 |
| training and | 0.1667 |
| welcoming culture | 0.1667 |
| work within | 0.1667 |
| advancement the | 0.125 |
| career with | 0.125 |
| changing | 0.125 |
| choosing | 0.125 |
| culture some | 0.125 |
| diverse company | 0.125 |
| experience with | 0.125 |
| experiences and | 0.125 |
| innovative and | 0.125 |
| inspire | 0.125 |
| making | 0.125 |
| opportunities the | 0.125 |
| perceived | 0.125 |
| valuable experience | 0.125 |
| welcoming and | 0.125 |
| approaches | 0.1 |
| career the | 0.1 |
| constructive | 0.1 |
| cooperation | 0.1 |
| creativity and | 0.1 |
| experience and | 0.1 |
| expertise and | 0.1 |
| for innovation | 0.1 |
| improving | 0.1 |
| increasingly | 0.1 |
| knowledge | 0.1 |
| learning and | 0.1 |
| qualified | 0.1 |
| skills | 0.1 |
| competence | 0.0833 |
| competitive with | 0.0833 |
| culturally | 0.0833 |
| culture | 0.0833 |
| culture and | 0.0833 |
| influence | 0.0833 |
| knowledge and | 0.0833 |
| promising | 0.0833 |
| regarded | 0.0833 |
| the culture | 0.0833 |
| with opportunities | 0.0833 |
| achieving | 0.0714 |
| competition and | 0.0714 |
| making good | 0.0714 |
| understanding | 0.0714 |
| achieve | 0.0556 |
| marketing | 0.0556 |

## A.5 `tight_loose`  (Pole A = loose_control, Pole B = tight_control)


### loose_control — Pole A (score → −1) — 159 terms

| Term | Weight |
|---|---|
| empowered | 1 |
| independent | 1 |
| ownership | 1 |
| autonomy | 0.6667 |
| empowerment | 0.6667 |
| entrepreneurial | 0.6667 |
| freedom | 0.6667 |
| flexible | 0.5 |
| independent agent | 0.5 |
| management flexible | 0.5 |
| organisations | 0.5 |
| own | 0.5 |
| self development | 0.5 |
| voluntary | 0.5 |
| company flexible | 0.3333 |
| empowering | 0.3333 |
| enterprise | 0.3333 |
| flexibility working | 0.3333 |
| government | 0.3333 |
| independence | 0.3333 |
| independently | 0.3333 |
| manage | 0.3333 |
| manages | 0.3333 |
| managing | 0.3333 |
| organisational | 0.3333 |
| self | 0.3333 |
| some management | 0.3333 |
| team flexible | 0.3333 |
| wealth | 0.3333 |
| ambition | 0.25 |
| ambitious | 0.25 |
| and manage | 0.25 |
| company | 0.25 |
| control | 0.25 |
| cooperative | 0.25 |
| discretionary | 0.25 |
| enables | 0.25 |
| entrepreneurial spirit | 0.25 |
| flexibility | 0.25 |
| flexible good | 0.25 |
| flexible working | 0.25 |
| freedom work | 0.25 |
| initiative | 0.25 |
| leadership the | 0.25 |
| manage people | 0.25 |
| manage your | 0.25 |
| manageable | 0.25 |
| management but | 0.25 |
| managing people | 0.25 |
| motivated and | 0.25 |
| organization | 0.25 |
| own work | 0.25 |
| ownership and | 0.25 |
| role | 0.25 |
| role and | 0.25 |
| roles and | 0.25 |
| self employed | 0.25 |
| some flexibility | 0.25 |
| the freedom | 0.25 |
| the role | 0.25 |
| the roles | 0.25 |
| the wealth | 0.25 |
| this organisation | 0.25 |
| well organised | 0.25 |
| authority | 0.2 |
| capabilities | 0.2 |
| capability | 0.2 |
| cooperation | 0.2 |
| coordination | 0.2 |
| employment | 0.2 |
| executives | 0.2 |
| functioning | 0.2 |
| organisation | 0.2 |
| organizations | 0.2 |
| administration | 0.1667 |
| autonomy and | 0.1667 |
| driven | 0.1667 |
| ease | 0.1667 |
| efficient | 0.1667 |
| financially | 0.1667 |
| flexibility for | 0.1667 |
| flexible work | 0.1667 |
| freedom and | 0.1667 |
| how manage | 0.1667 |
| influence | 0.1667 |
| institutional | 0.1667 |
| leaders | 0.1667 |
| manage and | 0.1667 |
| manage the | 0.1667 |
| management that | 0.1667 |
| management they | 0.1667 |
| management with | 0.1667 |
| managerial | 0.1667 |
| movement and | 0.1667 |
| organisation and | 0.1667 |
| organised | 0.1667 |
| organization but | 0.1667 |
| powerful | 0.1667 |
| progressive | 0.1667 |
| straightforward | 0.1667 |
| strong leadership | 0.1667 |
| the management | 0.1667 |
| work flexible | 0.1667 |
| your managers | 0.1667 |
| determined | 0.1429 |
| leadership | 0.1429 |
| management | 0.1429 |
| operate | 0.1429 |
| responsibility | 0.1429 |
| being made | 0.125 |
| business owners | 0.125 |
| competent and | 0.125 |
| complex | 0.125 |
| flexibility and | 0.125 |
| flexibility the | 0.125 |
| flexibility with | 0.125 |
| flexible and | 0.125 |
| flexible with | 0.125 |
| from management | 0.125 |
| governance | 0.125 |
| inspire | 0.125 |
| involved with | 0.125 |
| leadership that | 0.125 |
| much flexibility | 0.125 |
| organization with | 0.125 |
| organization you | 0.125 |
| organizational | 0.125 |
| the flexibility | 0.125 |
| the leadership | 0.125 |
| the organisation | 0.125 |
| with flexibility | 0.125 |
| with flexible | 0.125 |
| with leadership | 0.125 |
| advancing | 0.1 |
| efficiency and | 0.1 |
| efficiently | 0.1 |
| from leadership | 0.1 |
| organization and | 0.1 |
| strategy and | 0.1 |
| trust and | 0.1 |
| behaviour | 0.0833 |
| competence | 0.0833 |
| hardworking | 0.0833 |
| job and | 0.0833 |
| leadership with | 0.0833 |
| responsible | 0.0833 |
| the workplace | 0.0833 |
| with management | 0.0833 |
| with opportunities | 0.0833 |
| your management | 0.0833 |
| the decisions | 0.0714 |
| based work | 0.0625 |
| leadership and | 0.0625 |
| achieve | 0.0556 |
| decisions | 0.0556 |
| management and | 0.0556 |
| responsibilities and | 0.0556 |
| responsibility and | 0.0556 |
| decisions and | 0.05 |

### tight_control — Pole B (score → +1) — 186 terms

| Term | Weight |
|---|---|
| hierarchical | 1 |
| hierarchy | 1 |
| formal | 0.6667 |
| oversight | 0.6667 |
| policies | 0.6667 |
| structured | 0.6667 |
| administrative | 0.5 |
| approvals | 0.5 |
| control | 0.5 |
| control over | 0.5 |
| discipline | 0.5 |
| good structure | 0.5 |
| hierarchal | 0.5 |
| hierarchies | 0.5 |
| management all | 0.5 |
| management some | 0.5 |
| management structure | 0.5 |
| mandate | 0.5 |
| mandated | 0.5 |
| much hierarchy | 0.5 |
| premises | 0.5 |
| regulated | 0.5 |
| regulation | 0.5 |
| rules | 0.5 |
| strict | 0.5 |
| structure | 0.5 |
| bureaucracy | 0.4 |
| advisory | 0.3333 |
| centralized | 0.3333 |
| controlling | 0.3333 |
| duties | 0.3333 |
| guidance | 0.3333 |
| guidelines | 0.3333 |
| implement | 0.3333 |
| laws | 0.3333 |
| manage | 0.3333 |
| managerial | 0.3333 |
| manages | 0.3333 |
| managing | 0.3333 |
| organisational | 0.3333 |
| organised | 0.3333 |
| organized | 0.3333 |
| policy | 0.3333 |
| regulations | 0.3333 |
| rigorous | 0.3333 |
| supervision | 0.3333 |
| well structured | 0.3333 |
| and hierarchical | 0.25 |
| approval | 0.25 |
| behavior | 0.25 |
| behaviours | 0.25 |
| consistent | 0.25 |
| controlled | 0.25 |
| enforced | 0.25 |
| executive | 0.25 |
| friendly policies | 0.25 |
| governance | 0.25 |
| hierarchical and | 0.25 |
| hierarchical structure | 0.25 |
| hierarchy and | 0.25 |
| hierarchy good | 0.25 |
| highly bureaucratic | 0.25 |
| implemented | 0.25 |
| institutions | 0.25 |
| level management | 0.25 |
| management for | 0.25 |
| management generally | 0.25 |
| management just | 0.25 |
| management level | 0.25 |
| management style | 0.25 |
| management the | 0.25 |
| management this | 0.25 |
| management very | 0.25 |
| management which | 0.25 |
| management you | 0.25 |
| managers and | 0.25 |
| managing people | 0.25 |
| mandates | 0.25 |
| organization | 0.25 |
| organization for | 0.25 |
| organization very | 0.25 |
| organizational structure | 0.25 |
| policy for | 0.25 |
| practices | 0.25 |
| principles | 0.25 |
| rule | 0.25 |
| standards | 0.25 |
| strong management | 0.25 |
| structure and | 0.25 |
| structure that | 0.25 |
| structure the | 0.25 |
| supervisors and | 0.25 |
| the hierarchy | 0.25 |
| the policy | 0.25 |
| the structure | 0.25 |
| unorganized | 0.25 |
| very formal | 0.25 |
| very hierarchical | 0.25 |
| very organized | 0.25 |
| well organized | 0.25 |
| action | 0.2 |
| authority | 0.2 |
| bureaucratic | 0.2 |
| compliance | 0.2 |
| coordination | 0.2 |
| executives | 0.2 |
| operational | 0.2 |
| organisation | 0.2 |
| organizations | 0.2 |
| and structured | 0.1667 |
| behaviors | 0.1667 |
| behaviour | 0.1667 |
| boundaries | 0.1667 |
| ensuring | 0.1667 |
| good management | 0.1667 |
| guidance and | 0.1667 |
| high standards | 0.1667 |
| how manage | 0.1667 |
| institutional | 0.1667 |
| involves | 0.1667 |
| manage and | 0.1667 |
| manage the | 0.1667 |
| management excellent | 0.1667 |
| management that | 0.1667 |
| organisation and | 0.1667 |
| organisation with | 0.1667 |
| organized and | 0.1667 |
| policies and | 0.1667 |
| policy and | 0.1667 |
| strict and | 0.1667 |
| structured and | 0.1667 |
| style | 0.1667 |
| systems | 0.1667 |
| systems and | 0.1667 |
| the following | 0.1667 |
| the policies | 0.1667 |
| the systems | 0.1667 |
| upper management | 0.1667 |
| very structured | 0.1667 |
| your managers | 0.1667 |
| operate | 0.1429 |
| bureaucratic and | 0.125 |
| compliance and | 0.125 |
| follows | 0.125 |
| for management | 0.125 |
| handled | 0.125 |
| leadership that | 0.125 |
| logic | 0.125 |
| manage people | 0.125 |
| manageable | 0.125 |
| management work | 0.125 |
| operations | 0.125 |
| operations and | 0.125 |
| organization with | 0.125 |
| organization you | 0.125 |
| organizational | 0.125 |
| practices and | 0.125 |
| rules and | 0.125 |
| standards for | 0.125 |
| the bureaucracy | 0.125 |
| the leadership | 0.125 |
| the organisation | 0.125 |
| decisions | 0.1111 |
| design | 0.1 |
| organization and | 0.1 |
| planning and | 0.1 |
| standards and | 0.1 |
| the operations | 0.1 |
| the rules | 0.1 |
| workplace and | 0.1 |
| bureaucracy and | 0.0833 |
| communicated | 0.0833 |
| knowledge and | 0.0833 |
| leadership with | 0.0833 |
| processing | 0.0833 |
| tasks | 0.0833 |
| tasks and | 0.0833 |
| the tasks | 0.0833 |
| with management | 0.0833 |
| advanced | 0.0714 |
| technology | 0.0714 |
| the decisions | 0.0714 |
| accountability | 0.0625 |
| responsibilities and | 0.0556 |
| responsibility and | 0.0556 |
| decisions and | 0.05 |

## A.6 `pragmatic_normative`  (Pole A = normative, Pole B = pragmatic)


### normative — Pole A (score → −1) — 165 terms

| Term | Weight |
|---|---|
| ethical | 0.5 |
| moral | 0.5 |
| value | 0.5 |
| value work | 0.5 |
| integrity | 0.4 |
| feel valued | 0.3333 |
| good values | 0.3333 |
| values employees | 0.3333 |
| and responsibility | 0.25 |
| and value | 0.25 |
| and values | 0.25 |
| are responsible | 0.25 |
| are valued | 0.25 |
| based | 0.25 |
| based and | 0.25 |
| based off | 0.25 |
| based the | 0.25 |
| based upon | 0.25 |
| behave | 0.25 |
| behaviours | 0.25 |
| core values | 0.25 |
| decisions are | 0.25 |
| diligence | 0.25 |
| don value | 0.25 |
| entitlement | 0.25 |
| enviornment | 0.25 |
| great values | 0.25 |
| high integrity | 0.25 |
| lot responsibility | 0.25 |
| lots responsibility | 0.25 |
| make decisions | 0.25 |
| morals | 0.25 |
| not valued | 0.25 |
| objectives | 0.25 |
| philosophy | 0.25 |
| principles | 0.25 |
| strong values | 0.25 |
| system for | 0.25 |
| the data | 0.25 |
| the importance | 0.25 |
| the value | 0.25 |
| value and | 0.25 |
| value for | 0.25 |
| value the | 0.25 |
| value their | 0.25 |
| value your | 0.25 |
| valued | 0.25 |
| values | 0.25 |
| values and | 0.25 |
| values are | 0.25 |
| values its | 0.25 |
| values the | 0.25 |
| values work | 0.25 |
| you value | 0.25 |
| compliance | 0.2 |
| ethics | 0.2 |
| making decisions | 0.2 |
| and ethics | 0.1667 |
| and integrity | 0.1667 |
| approachable | 0.1667 |
| based merit | 0.1667 |
| behaviors | 0.1667 |
| behaviour | 0.1667 |
| belief | 0.1667 |
| commitment | 0.1667 |
| company values | 0.1667 |
| consideration for | 0.1667 |
| criteria | 0.1667 |
| equal | 0.1667 |
| ethic and | 0.1667 |
| laws | 0.1667 |
| merit based | 0.1667 |
| objective | 0.1667 |
| regard | 0.1667 |
| rights | 0.1667 |
| standards good | 0.1667 |
| the decision | 0.1667 |
| understanding the | 0.1667 |
| value employees | 0.1667 |
| valued and | 0.1667 |
| work ethics | 0.1667 |
| responsibility | 0.1429 |
| accountability | 0.125 |
| agreement | 0.125 |
| approachable and | 0.125 |
| attitude towards | 0.125 |
| attitudes | 0.125 |
| balanced | 0.125 |
| behavior | 0.125 |
| caring and | 0.125 |
| compassion | 0.125 |
| complex | 0.125 |
| compliance and | 0.125 |
| control | 0.125 |
| controlled | 0.125 |
| decision makers | 0.125 |
| decisions made | 0.125 |
| decisions that | 0.125 |
| empathy | 0.125 |
| ethic | 0.125 |
| ethical and | 0.125 |
| fulfilling | 0.125 |
| governance | 0.125 |
| implemented | 0.125 |
| justify | 0.125 |
| meaningful | 0.125 |
| merit | 0.125 |
| more responsibility | 0.125 |
| positive attitude | 0.125 |
| practices | 0.125 |
| practices and | 0.125 |
| qualities | 0.125 |
| quality life | 0.125 |
| respect | 0.125 |
| respect for | 0.125 |
| responsibility for | 0.125 |
| standards | 0.125 |
| standards for | 0.125 |
| take responsibility | 0.125 |
| trust | 0.125 |
| willingness | 0.125 |
| work ethic | 0.125 |
| decisions | 0.1111 |
| accountable | 0.1 |
| approach and | 0.1 |
| attitude | 0.1 |
| attitude and | 0.1 |
| authority | 0.1 |
| based experience | 0.1 |
| consideration | 0.1 |
| constructive | 0.1 |
| dealing | 0.1 |
| decision making | 0.1 |
| ethics and | 0.1 |
| for innovation | 0.1 |
| functioning | 0.1 |
| integrity and | 0.1 |
| responsibilities | 0.1 |
| standards and | 0.1 |
| trust and | 0.1 |
| actions | 0.0833 |
| approach | 0.0833 |
| bureaucracy and | 0.0833 |
| dealing with | 0.0833 |
| ensuring | 0.0833 |
| evaluation | 0.0833 |
| handling | 0.0833 |
| influence | 0.0833 |
| respect and | 0.0833 |
| respectful | 0.0833 |
| respectful and | 0.0833 |
| responsible | 0.0833 |
| situations | 0.0833 |
| accountability for | 0.0714 |
| achieving | 0.0714 |
| determined | 0.0714 |
| making good | 0.0714 |
| the decisions | 0.0714 |
| understanding | 0.0714 |
| based work | 0.0625 |
| leadership and | 0.0625 |
| management and | 0.0556 |
| responsibilities and | 0.0556 |
| responsibility and | 0.0556 |
| decisions and | 0.05 |

### pragmatic — Pole B (score → +1) — 165 terms

| Term | Weight |
|---|---|
| practical | 1 |
| realistic | 1 |
| efficiency | 0.6667 |
| economic | 0.5 |
| flexible | 0.5 |
| practically | 0.5 |
| product based | 0.5 |
| profitability | 0.5 |
| approachable | 0.3333 |
| customer focused | 0.3333 |
| effective | 0.3333 |
| performance based | 0.3333 |
| resourceful | 0.3333 |
| technologically | 0.3333 |
| advantages | 0.25 |
| and approachable | 0.25 |
| and efficient | 0.25 |
| based performance | 0.25 |
| driven people | 0.25 |
| economy | 0.25 |
| flexibility | 0.25 |
| flexibility not | 0.25 |
| flexibility work | 0.25 |
| flexible good | 0.25 |
| flexible working | 0.25 |
| for technology | 0.25 |
| industry can | 0.25 |
| less competitive | 0.25 |
| lot flexibility | 0.25 |
| lucrative | 0.25 |
| markets and | 0.25 |
| more efficient | 0.25 |
| more flexibility | 0.25 |
| performance driven | 0.25 |
| profitable | 0.25 |
| smart | 0.25 |
| smart driven | 0.25 |
| some flexibility | 0.25 |
| strategic | 0.25 |
| strong focus | 0.25 |
| technology can | 0.25 |
| the market | 0.25 |
| very approachable | 0.25 |
| very efficient | 0.25 |
| virtually | 0.25 |
| with business | 0.25 |
| with competitors | 0.25 |
| with industry | 0.25 |
| with market | 0.25 |
| efficiently | 0.2 |
| productivity | 0.2 |
| technological | 0.2 |
| business very | 0.1667 |
| businesses | 0.1667 |
| businesses and | 0.1667 |
| client focused | 0.1667 |
| company flexible | 0.1667 |
| competitive | 0.1667 |
| competitive and | 0.1667 |
| competitive for | 0.1667 |
| competitiveness | 0.1667 |
| consumer | 0.1667 |
| diversified | 0.1667 |
| driven | 0.1667 |
| effectively | 0.1667 |
| efficient | 0.1667 |
| efficient and | 0.1667 |
| effort and | 0.1667 |
| financially | 0.1667 |
| flexibility for | 0.1667 |
| flexibility great | 0.1667 |
| flexibility working | 0.1667 |
| flexible for | 0.1667 |
| flexible work | 0.1667 |
| focus and | 0.1667 |
| focused | 0.1667 |
| focused and | 0.1667 |
| focussed | 0.1667 |
| for business | 0.1667 |
| for industry | 0.1667 |
| industry | 0.1667 |
| industry for | 0.1667 |
| industry the | 0.1667 |
| industry very | 0.1667 |
| invaluable | 0.1667 |
| market | 0.1667 |
| market and | 0.1667 |
| market for | 0.1667 |
| more flexible | 0.1667 |
| performance | 0.1667 |
| process driven | 0.1667 |
| productive | 0.1667 |
| productivity and | 0.1667 |
| professionally | 0.1667 |
| reasonable work | 0.1667 |
| sophisticated | 0.1667 |
| strategies | 0.1667 |
| streamlined | 0.1667 |
| strongly | 0.1667 |
| technical | 0.1667 |
| technical and | 0.1667 |
| technologies | 0.1667 |
| technologies and | 0.1667 |
| technology and | 0.1667 |
| varying | 0.1667 |
| with customers | 0.1667 |
| work flexible | 0.1667 |
| advanced | 0.1429 |
| technology | 0.1429 |
| approachable and | 0.125 |
| based work | 0.125 |
| business and | 0.125 |
| business decisions | 0.125 |
| creativity | 0.125 |
| flexibility and | 0.125 |
| flexibility the | 0.125 |
| flexibility with | 0.125 |
| flexible and | 0.125 |
| flexible with | 0.125 |
| high performance | 0.125 |
| impactful | 0.125 |
| industry and | 0.125 |
| industry work | 0.125 |
| innovation | 0.125 |
| innovative | 0.125 |
| innovative and | 0.125 |
| manageable | 0.125 |
| much flexibility | 0.125 |
| product | 0.125 |
| strategy | 0.125 |
| the flexibility | 0.125 |
| the technical | 0.125 |
| the technology | 0.125 |
| with flexibility | 0.125 |
| with flexible | 0.125 |
| business | 0.1111 |
| marketing | 0.1111 |
| based experience | 0.1 |
| capabilities | 0.1 |
| creativity and | 0.1 |
| decision making | 0.1 |
| design | 0.1 |
| efficiency and | 0.1 |
| for innovation | 0.1 |
| increasingly | 0.1 |
| performance but | 0.1 |
| products | 0.1 |
| strategy and | 0.1 |
| technology work | 0.1 |
| approach | 0.0833 |
| competitive with | 0.0833 |
| performance and | 0.0833 |
| performance the | 0.0833 |
| the marketing | 0.0833 |
| the performance | 0.0833 |
| this industry | 0.0833 |
| with opportunities | 0.0833 |
| competition and | 0.0714 |
| industry with | 0.0714 |
| production | 0.0714 |
| the industry | 0.0714 |
| understanding | 0.0714 |
| achieve | 0.0556 |
| decisions | 0.0556 |
| decisions and | 0.05 |

*Total Hofstede terms: 2075*


# Appendix B — MIT Big 9 Keyword Dictionaries (complete)

Score = min(10, 2 × Σ matched weights).


## B.1 `agility` — 160 terms

| Term | Weight |
|---|---|
| dynamic | 1 |
| fast | 1 |
| fast moving | 1 |
| nimble | 1 |
| quick | 1 |
| rapid | 1 |
| speed | 1 |
| faster | 0.75 |
| quickly | 0.75 |
| rapidly | 0.75 |
| abilities | 0.5 |
| ability | 0.5 |
| ability move | 0.5 |
| able move | 0.5 |
| adapt | 0.5 |
| convenient | 0.5 |
| delay | 0.5 |
| easy move | 0.5 |
| fast growing | 0.5 |
| fast pace | 0.5 |
| fast paced | 0.5 |
| flex | 0.5 |
| flex time | 0.5 |
| flexible | 0.5 |
| immediate | 0.5 |
| immediately | 0.5 |
| lengthy | 0.5 |
| move fast | 0.5 |
| move quickly | 0.5 |
| moves | 0.5 |
| quicker | 0.5 |
| quickly great | 0.5 |
| readily | 0.5 |
| short | 0.5 |
| slow due | 0.5 |
| slow work | 0.5 |
| stronger | 0.5 |
| suddenly | 0.5 |
| timely | 0.5 |
| timings | 0.5 |
| streamlined | 0.3333 |
| urgency | 0.3333 |
| ability for | 0.25 |
| ability work | 0.25 |
| able learn | 0.25 |
| accelerated | 0.25 |
| adjustment | 0.25 |
| adjustments | 0.25 |
| and fast | 0.25 |
| and slow | 0.25 |
| being able | 0.25 |
| can move | 0.25 |
| challenge | 0.25 |
| changes | 0.25 |
| delays | 0.25 |
| difficult move | 0.25 |
| drag | 0.25 |
| easier | 0.25 |
| easy and | 0.25 |
| easy for | 0.25 |
| energetic | 0.25 |
| equipped | 0.25 |
| extremely | 0.25 |
| fast and | 0.25 |
| fast track | 0.25 |
| fast you | 0.25 |
| flex work | 0.25 |
| flexible time | 0.25 |
| good slow | 0.25 |
| gradually | 0.25 |
| heavy | 0.25 |
| heavy and | 0.25 |
| intensive | 0.25 |
| longer | 0.25 |
| mobility | 0.25 |
| mobility great | 0.25 |
| mobility the | 0.25 |
| mobility you | 0.25 |
| movement | 0.25 |
| moving | 0.25 |
| moving and | 0.25 |
| moving the | 0.25 |
| paced | 0.25 |
| power | 0.25 |
| quickly and | 0.25 |
| quickly the | 0.25 |
| quickly you | 0.25 |
| really slow | 0.25 |
| rushed | 0.25 |
| shifting | 0.25 |
| simple | 0.25 |
| slow adopt | 0.25 |
| slow paced | 0.25 |
| small | 0.25 |
| some flexibility | 0.25 |
| sometimes slow | 0.25 |
| sound | 0.25 |
| steady | 0.25 |
| stiff | 0.25 |
| taking time | 0.25 |
| the ability | 0.25 |
| the fast | 0.25 |
| timings and | 0.25 |
| too fast | 0.25 |
| too slow | 0.25 |
| very fast | 0.25 |
| very quickly | 0.25 |
| very slow | 0.25 |
| very slowly | 0.25 |
| work fast | 0.25 |
| you move | 0.25 |
| your ability | 0.25 |
| capability | 0.2 |
| efficiently | 0.2 |
| difficult | 0.1667 |
| difficulty | 0.1667 |
| efficient | 0.1667 |
| efficient and | 0.1667 |
| flexibility great | 0.1667 |
| flexible for | 0.1667 |
| little flexibility | 0.1667 |
| more flexible | 0.1667 |
| movement and | 0.1667 |
| skilled | 0.1667 |
| stable | 0.1667 |
| straightforward | 0.1667 |
| strong | 0.1667 |
| strongly | 0.1667 |
| determined | 0.1429 |
| balanced | 0.125 |
| changing | 0.125 |
| flexibility | 0.125 |
| flexibility and | 0.125 |
| flexibility the | 0.125 |
| flexibility with | 0.125 |
| flexible and | 0.125 |
| flexible good | 0.125 |
| flexible with | 0.125 |
| high performance | 0.125 |
| impactful | 0.125 |
| limited flexibility | 0.125 |
| much flexibility | 0.125 |
| progressing | 0.125 |
| skill | 0.125 |
| the flexibility | 0.125 |
| with flexibility | 0.125 |
| with flexible | 0.125 |
| adapting | 0.1 |
| advancing | 0.1 |
| detail | 0.1 |
| performance but | 0.1 |
| skills | 0.1 |
| your performance | 0.1 |
| achieved | 0.0833 |
| effectively | 0.0833 |
| performance and | 0.0833 |
| progress | 0.0833 |
| the performance | 0.0833 |
| advanced | 0.0714 |
| achieve | 0.0556 |

## B.2 `collaboration` — 187 terms

| Term | Weight |
|---|---|
| communicate | 1 |
| partnership | 1 |
| team | 1 |
| team player | 1 |
| collaborating | 0.75 |
| collaboration across | 0.75 |
| collaborative team | 0.75 |
| collaborative work | 0.75 |
| people collaborative | 0.75 |
| team collaboration | 0.75 |
| team work | 0.75 |
| team working | 0.75 |
| work together | 0.75 |
| collaboration | 0.6667 |
| collaborative | 0.6667 |
| collaborative culture | 0.6667 |
| teamwork | 0.6667 |
| together | 0.6667 |
| across teams | 0.5 |
| between teams | 0.5 |
| collaborate | 0.5 |
| collaborative environment | 0.5 |
| communicating | 0.5 |
| communication between | 0.5 |
| conflicts | 0.5 |
| connected | 0.5 |
| culture collaboration | 0.5 |
| culture team | 0.5 |
| each team | 0.5 |
| friendly team | 0.5 |
| gathering | 0.5 |
| global team | 0.5 |
| good collaboration | 0.5 |
| good colleagues | 0.5 |
| good organization | 0.5 |
| good teamwork | 0.5 |
| great collaborative | 0.5 |
| great teamwork | 0.5 |
| helpful team | 0.5 |
| interaction | 0.5 |
| joiners | 0.5 |
| joining | 0.5 |
| leadership team | 0.5 |
| meeting | 0.5 |
| meetings | 0.5 |
| one another | 0.5 |
| one team | 0.5 |
| own team | 0.5 |
| part team | 0.5 |
| partner | 0.5 |
| partners | 0.5 |
| partners group | 0.5 |
| people team | 0.5 |
| reconciliation | 0.5 |
| shared | 0.5 |
| some team | 0.5 |
| strong team | 0.5 |
| support team | 0.5 |
| supportive team | 0.5 |
| team based | 0.5 |
| team bonding | 0.5 |
| team building | 0.5 |
| team culture | 0.5 |
| team dependent | 0.5 |
| team dynamics | 0.5 |
| team environment | 0.5 |
| team friendly | 0.5 |
| team good | 0.5 |
| team lead | 0.5 |
| team leader | 0.5 |
| team leaders | 0.5 |
| team management | 0.5 |
| team mates | 0.5 |
| team meetings | 0.5 |
| team member | 0.5 |
| team members | 0.5 |
| team nice | 0.5 |
| team oriented | 0.5 |
| team people | 0.5 |
| team players | 0.5 |
| team politics | 0.5 |
| team some | 0.5 |
| team structure | 0.5 |
| team support | 0.5 |
| team worked | 0.5 |
| teammates | 0.5 |
| teams | 0.5 |
| teams work | 0.5 |
| within team | 0.5 |
| work team | 0.5 |
| working team | 0.5 |
| working together | 0.5 |
| cooperation | 0.4 |
| cooperative | 0.375 |
| business partners | 0.3333 |
| centralized | 0.3333 |
| colleagues | 0.3333 |
| community | 0.3333 |
| consensus | 0.3333 |
| corp | 0.3333 |
| culture collaborative | 0.3333 |
| discuss | 0.3333 |
| involved | 0.3333 |
| organized | 0.3333 |
| participate | 0.3333 |
| peer | 0.3333 |
| sharing | 0.3333 |
| team company | 0.3333 |
| team flexible | 0.3333 |
| work colleagues | 0.3333 |
| agreement | 0.25 |
| and collaboration | 0.25 |
| apart | 0.25 |
| collaboration with | 0.25 |
| colleagues and | 0.25 |
| colleagues you | 0.25 |
| communicate with | 0.25 |
| communication with | 0.25 |
| conflict | 0.25 |
| connect with | 0.25 |
| coworkers and | 0.25 |
| friends with | 0.25 |
| group you | 0.25 |
| groups | 0.25 |
| joining the | 0.25 |
| organization that | 0.25 |
| organization the | 0.25 |
| organization work | 0.25 |
| our team | 0.25 |
| our work | 0.25 |
| people and | 0.25 |
| people with | 0.25 |
| sharing and | 0.25 |
| team and | 0.25 |
| team but | 0.25 |
| team team | 0.25 |
| team the | 0.25 |
| team they | 0.25 |
| team very | 0.25 |
| team with | 0.25 |
| team your | 0.25 |
| teammates and | 0.25 |
| teams and | 0.25 |
| teams the | 0.25 |
| teams with | 0.25 |
| the colleagues | 0.25 |
| the group | 0.25 |
| the team | 0.25 |
| the teams | 0.25 |
| together and | 0.25 |
| very collaborative | 0.25 |
| work with | 0.25 |
| you join | 0.25 |
| your colleagues | 0.25 |
| coordination | 0.2 |
| organisation | 0.2 |
| organizations | 0.2 |
| social | 0.2 |
| work through | 0.2 |
| workplace | 0.2 |
| and collaborative | 0.1667 |
| collaborate with | 0.1667 |
| collaboration and | 0.1667 |
| collaborative and | 0.1667 |
| colleagues the | 0.1667 |
| colleagues with | 0.1667 |
| communicated | 0.1667 |
| group and | 0.1667 |
| groups and | 0.1667 |
| meetings and | 0.1667 |
| organisation with | 0.1667 |
| organization but | 0.1667 |
| the community | 0.1667 |
| with colleagues | 0.1667 |
| leadership | 0.1429 |
| company with | 0.125 |
| compete with | 0.125 |
| involved with | 0.125 |
| teamwork and | 0.125 |
| workplace with | 0.125 |
| organization and | 0.1 |
| workplace and | 0.1 |
| competitive with | 0.0833 |
| leadership with | 0.0833 |
| the workplace | 0.0833 |
| working with | 0.0833 |
| leadership and | 0.0625 |

## B.3 `customer_orientation` — 187 terms

| Term | Weight |
|---|---|
| client | 1 |
| customer | 1 |
| customer experience | 1 |
| customer service | 1 |
| customers | 1 |
| about clients | 0.5 |
| client centric | 0.5 |
| client experience | 0.5 |
| client facing | 0.5 |
| client needs | 0.5 |
| client service | 0.5 |
| client services | 0.5 |
| clients | 0.5 |
| clients first | 0.5 |
| clients good | 0.5 |
| clients great | 0.5 |
| customer focused | 0.5 |
| great clients | 0.5 |
| marketplace | 0.5 |
| new clients | 0.5 |
| services | 0.5 |
| services firm | 0.5 |
| services industry | 0.5 |
| client focused | 0.3333 |
| consumer | 0.3333 |
| service | 0.3333 |
| service based | 0.3333 |
| services company | 0.3333 |
| and client | 0.25 |
| and clients | 0.25 |
| and customer | 0.25 |
| and customers | 0.25 |
| brand reputation | 0.25 |
| business for | 0.25 |
| business great | 0.25 |
| business lines | 0.25 |
| business needs | 0.25 |
| business practices | 0.25 |
| business side | 0.25 |
| business the | 0.25 |
| business they | 0.25 |
| business unit | 0.25 |
| business with | 0.25 |
| cab service | 0.25 |
| centric company | 0.25 |
| client and | 0.25 |
| client base | 0.25 |
| client first | 0.25 |
| client interaction | 0.25 |
| client relationship | 0.25 |
| clientele | 0.25 |
| clients and | 0.25 |
| clients are | 0.25 |
| clients but | 0.25 |
| clients can | 0.25 |
| clients not | 0.25 |
| clients that | 0.25 |
| clients the | 0.25 |
| clients who | 0.25 |
| clients with | 0.25 |
| clients you | 0.25 |
| company excellent | 0.25 |
| company review | 0.25 |
| company this | 0.25 |
| company trying | 0.25 |
| consulting | 0.25 |
| core business | 0.25 |
| customer and | 0.25 |
| customers and | 0.25 |
| customers are | 0.25 |
| demand | 0.25 |
| different business | 0.25 |
| dissatisfaction | 0.25 |
| experienced staff | 0.25 |
| financial service | 0.25 |
| for client | 0.25 |
| for clients | 0.25 |
| for customers | 0.25 |
| for sales | 0.25 |
| great business | 0.25 |
| high demand | 0.25 |
| its clients | 0.25 |
| lines business | 0.25 |
| market leader | 0.25 |
| our clients | 0.25 |
| promotional | 0.25 |
| provider | 0.25 |
| purchase | 0.25 |
| sales | 0.25 |
| sales agent | 0.25 |
| sales and | 0.25 |
| sales job | 0.25 |
| sales training | 0.25 |
| sales you | 0.25 |
| salesman | 0.25 |
| service representative | 0.25 |
| services and | 0.25 |
| shop | 0.25 |
| the client | 0.25 |
| the clients | 0.25 |
| the customer | 0.25 |
| the customers | 0.25 |
| the retail | 0.25 |
| the sales | 0.25 |
| their business | 0.25 |
| their clients | 0.25 |
| vendor | 0.25 |
| vendors | 0.25 |
| with client | 0.25 |
| with clients | 0.25 |
| wonderful company | 0.25 |
| worth clients | 0.25 |
| your business | 0.25 |
| your clients | 0.25 |
| about company | 0.1667 |
| advertising | 0.1667 |
| business partners | 0.1667 |
| business very | 0.1667 |
| businesses | 0.1667 |
| businesses and | 0.1667 |
| companies | 0.1667 |
| company amazing | 0.1667 |
| company and | 0.1667 |
| company awesome | 0.1667 |
| company best | 0.1667 |
| company brand | 0.1667 |
| company cares | 0.1667 |
| company company | 0.1667 |
| company encourages | 0.1667 |
| company feel | 0.1667 |
| company get | 0.1667 |
| company good | 0.1667 |
| company great | 0.1667 |
| company just | 0.1667 |
| company learn | 0.1667 |
| company many | 0.1667 |
| company one | 0.1667 |
| company overall | 0.1667 |
| company reputation | 0.1667 |
| company seems | 0.1667 |
| company truly | 0.1667 |
| company used | 0.1667 |
| company very | 0.1667 |
| company whole | 0.1667 |
| company wide | 0.1667 |
| company worked | 0.1667 |
| consultant | 0.1667 |
| consultants | 0.1667 |
| employee care | 0.1667 |
| enterprise | 0.1667 |
| experience work | 0.1667 |
| for business | 0.1667 |
| for company | 0.1667 |
| for industry | 0.1667 |
| its employees | 0.1667 |
| job experience | 0.1667 |
| market | 0.1667 |
| market and | 0.1667 |
| market for | 0.1667 |
| new business | 0.1667 |
| sales goals | 0.1667 |
| service the | 0.1667 |
| servicing | 0.1667 |
| technical people | 0.1667 |
| the company | 0.1667 |
| the service | 0.1667 |
| their employee | 0.1667 |
| this company | 0.1667 |
| whole company | 0.1667 |
| with customers | 0.1667 |
| within company | 0.1667 |
| business and | 0.125 |
| business owners | 0.125 |
| company | 0.125 |
| company with | 0.125 |
| corporate experience | 0.125 |
| experience with | 0.125 |
| experienced employees | 0.125 |
| company doing | 0.1 |
| products | 0.1 |
| service and | 0.1 |
| the business | 0.1 |
| industry | 0.0833 |
| this industry | 0.0833 |
| industry with | 0.0714 |
| the industry | 0.0714 |
| satisfaction | 0.0625 |

## B.4 `diversity` — 173 terms

| Term | Weight |
|---|---|
| inclusion | 1 |
| inclusive environment | 1 |
| multicultural | 1 |
| about diversity | 0.75 |
| culture diversity | 0.75 |
| diverse | 0.6667 |
| diversity | 0.6667 |
| inclusive | 0.6667 |
| commitment diversity | 0.5 |
| differences | 0.5 |
| differently | 0.5 |
| differs | 0.5 |
| diverse culture | 0.5 |
| diverse environment | 0.5 |
| diverse group | 0.5 |
| diverse work | 0.5 |
| diverse workforce | 0.5 |
| diversity good | 0.5 |
| diversity inclusion | 0.5 |
| diversity work | 0.5 |
| great diversity | 0.5 |
| inclusive culture | 0.5 |
| minorities | 0.5 |
| racial | 0.5 |
| welcoming | 0.4 |
| focus diversity | 0.3333 |
| good diversity | 0.3333 |
| inclusive work | 0.3333 |
| opportunities across | 0.3333 |
| perspectives | 0.3333 |
| about different | 0.25 |
| across all | 0.25 |
| across multiple | 0.25 |
| ambiguous | 0.25 |
| and diverse | 0.25 |
| and diversity | 0.25 |
| and inclusion | 0.25 |
| and inclusive | 0.25 |
| are different | 0.25 |
| around different | 0.25 |
| based culture | 0.25 |
| color | 0.25 |
| comparison | 0.25 |
| comparison other | 0.25 |
| culture for | 0.25 |
| culture friendly | 0.25 |
| culture less | 0.25 |
| culture limited | 0.25 |
| culture lot | 0.25 |
| culture lots | 0.25 |
| culture many | 0.25 |
| culture more | 0.25 |
| culture opportunity | 0.25 |
| culture overall | 0.25 |
| culture some | 0.25 |
| culture varies | 0.25 |
| culture work | 0.25 |
| culture working | 0.25 |
| differ | 0.25 |
| different areas | 0.25 |
| different from | 0.25 |
| different than | 0.25 |
| discriminatory | 0.25 |
| disparity | 0.25 |
| diverse company | 0.25 |
| diversity not | 0.25 |
| each | 0.25 |
| environment | 0.25 |
| environment all | 0.25 |
| environment and | 0.25 |
| environment limited | 0.25 |
| environment some | 0.25 |
| equality | 0.25 |
| equally | 0.25 |
| few opportunities | 0.25 |
| from different | 0.25 |
| inclusion and | 0.25 |
| like culture | 0.25 |
| limited and | 0.25 |
| minority | 0.25 |
| mix | 0.25 |
| multi | 0.25 |
| neutral | 0.25 |
| opportunities limited | 0.25 |
| other groups | 0.25 |
| overall culture | 0.25 |
| particular | 0.25 |
| race | 0.25 |
| restricted | 0.25 |
| restrictions | 0.25 |
| separate | 0.25 |
| the different | 0.25 |
| the various | 0.25 |
| varies | 0.25 |
| variety | 0.25 |
| various | 0.25 |
| very diverse | 0.25 |
| very inclusive | 0.25 |
| very specific | 0.25 |
| with diverse | 0.25 |
| cultural | 0.2 |
| appearance | 0.1667 |
| boundaries | 0.1667 |
| communities | 0.1667 |
| confusion | 0.1667 |
| culturally | 0.1667 |
| culture | 0.1667 |
| culture all | 0.1667 |
| culture bit | 0.1667 |
| culture but | 0.1667 |
| culture changing | 0.1667 |
| culture one | 0.1667 |
| culture open | 0.1667 |
| culture supportive | 0.1667 |
| culture that | 0.1667 |
| culture the | 0.1667 |
| culture they | 0.1667 |
| culture this | 0.1667 |
| culture with | 0.1667 |
| culture within | 0.1667 |
| diverse and | 0.1667 |
| diversified | 0.1667 |
| diversity and | 0.1667 |
| diversity the | 0.1667 |
| for diversity | 0.1667 |
| freedom | 0.1667 |
| freedom and | 0.1667 |
| group and | 0.1667 |
| inclusive and | 0.1667 |
| independence | 0.1667 |
| independently | 0.1667 |
| isolated | 0.1667 |
| many opportunities | 0.1667 |
| open culture | 0.1667 |
| opportunities some | 0.1667 |
| opportunities with | 0.1667 |
| opportunities within | 0.1667 |
| opportunity | 0.1667 |
| opportunity and | 0.1667 |
| restrictive | 0.1667 |
| supportive culture | 0.1667 |
| the diversity | 0.1667 |
| there culture | 0.1667 |
| transparency and | 0.1667 |
| unclear | 0.1667 |
| unique | 0.1667 |
| welcoming culture | 0.1667 |
| welcoming environment | 0.1667 |
| apart | 0.125 |
| apart from | 0.125 |
| choosing | 0.125 |
| community and | 0.125 |
| conflict | 0.125 |
| groups | 0.125 |
| opportunities | 0.125 |
| opportunities the | 0.125 |
| the opportunities | 0.125 |
| unqualified | 0.125 |
| welcoming and | 0.125 |
| adapting | 0.1 |
| experience and | 0.1 |
| opportunities and | 0.1 |
| opportunities for | 0.1 |
| qualified | 0.1 |
| culture and | 0.0833 |
| experiences | 0.0833 |
| respect and | 0.0833 |
| respectful | 0.0833 |
| respectful and | 0.0833 |
| situations | 0.0833 |
| the culture | 0.0833 |
| with opportunities | 0.0833 |
| design | 0.05 |

## B.5 `execution` — 191 terms

| Term | Weight |
|---|---|
| deliver | 1 |
| delivery | 1 |
| execute | 1 |
| follow through | 1 |
| act | 0.5 |
| deliverables | 0.5 |
| delivered | 0.5 |
| delivering | 0.5 |
| executed | 0.5 |
| execution | 0.5 |
| gets done | 0.5 |
| operates | 0.5 |
| performed | 0.5 |
| accomplish | 0.3333 |
| doing something | 0.3333 |
| duties | 0.3333 |
| getting work | 0.3333 |
| operating | 0.3333 |
| operation | 0.3333 |
| outcomes | 0.3333 |
| procedures | 0.3333 |
| process good | 0.3333 |
| succeed | 0.3333 |
| things done | 0.3333 |
| accomplished | 0.25 |
| accountability | 0.25 |
| accountable for | 0.25 |
| being done | 0.25 |
| coming work | 0.25 |
| commit | 0.25 |
| completing | 0.25 |
| completion | 0.25 |
| cutting and | 0.25 |
| doing things | 0.25 |
| exec | 0.25 |
| expect work | 0.25 |
| expectations | 0.25 |
| expects | 0.25 |
| expects you | 0.25 |
| finish | 0.25 |
| firing | 0.25 |
| for operations | 0.25 |
| get done | 0.25 |
| job after | 0.25 |
| job done | 0.25 |
| job with | 0.25 |
| jobs for | 0.25 |
| more responsibilities | 0.25 |
| next job | 0.25 |
| operations | 0.25 |
| passing | 0.25 |
| perform | 0.25 |
| perform well | 0.25 |
| planned | 0.25 |
| prepared | 0.25 |
| prepared for | 0.25 |
| prepared work | 0.25 |
| process great | 0.25 |
| producing | 0.25 |
| promises | 0.25 |
| push | 0.25 |
| pushing | 0.25 |
| submit | 0.25 |
| succeed the | 0.25 |
| succeed you | 0.25 |
| this job | 0.25 |
| you perform | 0.25 |
| action | 0.2 |
| functioning | 0.2 |
| operational | 0.2 |
| responsibilities | 0.2 |
| success | 0.2 |
| task | 0.2 |
| work through | 0.2 |
| achieved | 0.1667 |
| and process | 0.1667 |
| efforts | 0.1667 |
| excellence | 0.1667 |
| expectations and | 0.1667 |
| expectations for | 0.1667 |
| for excellence | 0.1667 |
| for success | 0.1667 |
| into work | 0.1667 |
| job for | 0.1667 |
| out work | 0.1667 |
| perform the | 0.1667 |
| performing | 0.1667 |
| plans and | 0.1667 |
| procedure | 0.1667 |
| procedures and | 0.1667 |
| process and | 0.1667 |
| processes for | 0.1667 |
| results | 0.1667 |
| service | 0.1667 |
| service the | 0.1667 |
| servicing | 0.1667 |
| succeed and | 0.1667 |
| success the | 0.1667 |
| tasks | 0.1667 |
| the expectations | 0.1667 |
| the job | 0.1667 |
| the processes | 0.1667 |
| the service | 0.1667 |
| the work | 0.1667 |
| the working | 0.1667 |
| urgency | 0.1667 |
| with work | 0.1667 |
| with working | 0.1667 |
| work closely | 0.1667 |
| working | 0.1667 |
| working the | 0.1667 |
| your success | 0.1667 |
| achieving | 0.1429 |
| operate | 0.1429 |
| advancement you | 0.125 |
| ambition | 0.125 |
| approval | 0.125 |
| approvals | 0.125 |
| doing work | 0.125 |
| follows | 0.125 |
| for improvement | 0.125 |
| for management | 0.125 |
| fulfilling | 0.125 |
| handled | 0.125 |
| hectic work | 0.125 |
| jobs the | 0.125 |
| make work | 0.125 |
| more responsibility | 0.125 |
| operations and | 0.125 |
| progressing | 0.125 |
| responsibility for | 0.125 |
| successful | 0.125 |
| take responsibility | 0.125 |
| tasks good | 0.125 |
| with leadership | 0.125 |
| achieve | 0.1111 |
| accountable | 0.1 |
| achievement | 0.1 |
| advancing | 0.1 |
| bureaucracy | 0.1 |
| company doing | 0.1 |
| dealing | 0.1 |
| employment | 0.1 |
| evaluations | 0.1 |
| from leadership | 0.1 |
| improvement | 0.1 |
| improvement and | 0.1 |
| job | 0.1 |
| job work | 0.1 |
| making decisions | 0.1 |
| planning and | 0.1 |
| progress and | 0.1 |
| service and | 0.1 |
| success and | 0.1 |
| the operations | 0.1 |
| the success | 0.1 |
| your performance | 0.1 |
| bureaucracy and | 0.0833 |
| communicated | 0.0833 |
| dealing with | 0.0833 |
| effectively | 0.0833 |
| ensuring | 0.0833 |
| evaluation | 0.0833 |
| handling | 0.0833 |
| job and | 0.0833 |
| leadership with | 0.0833 |
| performance | 0.0833 |
| performance and | 0.0833 |
| performance the | 0.0833 |
| productive | 0.0833 |
| promising | 0.0833 |
| responsible | 0.0833 |
| tasks and | 0.0833 |
| tasks that | 0.0833 |
| the performance | 0.0833 |
| the task | 0.0833 |
| the tasks | 0.0833 |
| with management | 0.0833 |
| working with | 0.0833 |
| your management | 0.0833 |
| accountability for | 0.0714 |
| leadership | 0.0714 |
| management | 0.0714 |
| production | 0.0714 |
| responsibility | 0.0714 |
| leadership and | 0.0625 |
| satisfaction | 0.0625 |
| management and | 0.0556 |
| responsibilities and | 0.0556 |
| responsibility and | 0.0556 |
| decisions and | 0.05 |

## B.6 `innovation` — 180 terms

| Term | Weight |
|---|---|
| new products | 1 |
| creative | 0.6667 |
| new ideas | 0.6667 |
| creativity | 0.5 |
| developed | 0.5 |
| development great | 0.5 |
| exciting projects | 0.5 |
| good development | 0.5 |
| idea | 0.5 |
| improvements | 0.5 |
| innovation | 0.5 |
| innovations | 0.5 |
| innovative | 0.5 |
| innovative ideas | 0.5 |
| interesting projects | 0.5 |
| new tech | 0.5 |
| new things | 0.5 |
| some projects | 0.5 |
| something new | 0.5 |
| advancement within | 0.3333 |
| advancements | 0.3333 |
| develop | 0.3333 |
| developing | 0.3333 |
| forward thinking | 0.3333 |
| ideas | 0.3333 |
| innovate | 0.3333 |
| new business | 0.3333 |
| new opportunities | 0.3333 |
| new projects | 0.3333 |
| new ways | 0.3333 |
| projects | 0.3333 |
| sophisticated | 0.3333 |
| technologically | 0.3333 |
| technologies | 0.3333 |
| thinking | 0.3333 |
| advancement | 0.25 |
| advancement for | 0.25 |
| advancement great | 0.25 |
| and developing | 0.25 |
| and ideas | 0.25 |
| and innovation | 0.25 |
| and innovative | 0.25 |
| brand new | 0.25 |
| creating | 0.25 |
| designed | 0.25 |
| develop and | 0.25 |
| development for | 0.25 |
| development good | 0.25 |
| development the | 0.25 |
| development very | 0.25 |
| development you | 0.25 |
| emerging | 0.25 |
| exciting and | 0.25 |
| exciting work | 0.25 |
| for creativity | 0.25 |
| great development | 0.25 |
| ideas are | 0.25 |
| improved | 0.25 |
| inspiring | 0.25 |
| interesting tasks | 0.25 |
| introduced | 0.25 |
| knowledge the | 0.25 |
| making and | 0.25 |
| making great | 0.25 |
| making more | 0.25 |
| making the | 0.25 |
| many new | 0.25 |
| many projects | 0.25 |
| new | 0.25 |
| new and | 0.25 |
| new initiatives | 0.25 |
| new technologies | 0.25 |
| new technology | 0.25 |
| newly | 0.25 |
| product | 0.25 |
| product and | 0.25 |
| product development | 0.25 |
| products and | 0.25 |
| products that | 0.25 |
| products the | 0.25 |
| project and | 0.25 |
| projects good | 0.25 |
| projects great | 0.25 |
| projects that | 0.25 |
| projects the | 0.25 |
| projects with | 0.25 |
| research and | 0.25 |
| science | 0.25 |
| stepping stone | 0.25 |
| studies | 0.25 |
| technologies great | 0.25 |
| technology for | 0.25 |
| technology great | 0.25 |
| technology that | 0.25 |
| technology the | 0.25 |
| technology they | 0.25 |
| the development | 0.25 |
| the product | 0.25 |
| the products | 0.25 |
| the project | 0.25 |
| very innovative | 0.25 |
| with new | 0.25 |
| your ideas | 0.25 |
| achievement | 0.2 |
| development | 0.2 |
| improving | 0.2 |
| products | 0.2 |
| technological | 0.2 |
| achieved | 0.1667 |
| alternative | 0.1667 |
| and develop | 0.1667 |
| challenges | 0.1667 |
| challenges and | 0.1667 |
| development and | 0.1667 |
| development work | 0.1667 |
| entrepreneurial | 0.1667 |
| ideas and | 0.1667 |
| innovation and | 0.1667 |
| insight into | 0.1667 |
| insights | 0.1667 |
| intellectual | 0.1667 |
| intellectually stimulating | 0.1667 |
| introduction | 0.1667 |
| minds | 0.1667 |
| projects and | 0.1667 |
| promising | 0.1667 |
| technologies and | 0.1667 |
| technology and | 0.1667 |
| the projects | 0.1667 |
| thinking and | 0.1667 |
| unique | 0.1667 |
| achieving | 0.1429 |
| technology | 0.1429 |
| advancement and | 0.125 |
| advancement the | 0.125 |
| advancement you | 0.125 |
| experiences and | 0.125 |
| industry and | 0.125 |
| innovative and | 0.125 |
| inspire | 0.125 |
| learning | 0.125 |
| strategic | 0.125 |
| strategy | 0.125 |
| successful | 0.125 |
| the knowledge | 0.125 |
| the technology | 0.125 |
| achieve | 0.1111 |
| adapting | 0.1 |
| creativity and | 0.1 |
| design | 0.1 |
| experience and | 0.1 |
| expertise | 0.1 |
| expertise and | 0.1 |
| for advancement | 0.1 |
| for innovation | 0.1 |
| hardworking and | 0.1 |
| improvement | 0.1 |
| improvement and | 0.1 |
| increasingly | 0.1 |
| knowledge | 0.1 |
| learning and | 0.1 |
| opportunities and | 0.1 |
| opportunities for | 0.1 |
| progress and | 0.1 |
| success | 0.1 |
| success and | 0.1 |
| technology work | 0.1 |
| the success | 0.1 |
| experiences | 0.0833 |
| knowledge and | 0.0833 |
| productive | 0.0833 |
| progress | 0.0833 |
| tasks that | 0.0833 |
| the task | 0.0833 |
| advanced | 0.0714 |
| competition and | 0.0714 |
| industry with | 0.0714 |
| making good | 0.0714 |
| production | 0.0714 |
| the industry | 0.0714 |

## B.7 `integrity` — 167 terms

| Term | Weight |
|---|---|
| honest | 1 |
| transparency | 1 |
| transparent | 1 |
| honesty | 0.6667 |
| trustworthy | 0.6667 |
| credibility | 0.5 |
| ethical | 0.5 |
| fidelity | 0.5 |
| genuine | 0.5 |
| moral | 0.5 |
| trust | 0.5 |
| trusted | 0.5 |
| truth | 0.5 |
| integrity | 0.4 |
| belief | 0.3333 |
| commitment | 0.3333 |
| fairness | 0.3333 |
| loyalty | 0.3333 |
| respects | 0.3333 |
| sincere | 0.3333 |
| ethics | 0.3 |
| and honest | 0.25 |
| and trust | 0.25 |
| appropriately | 0.25 |
| attractive | 0.25 |
| believes | 0.25 |
| checks | 0.25 |
| clear | 0.25 |
| contributing | 0.25 |
| dirty | 0.25 |
| empathetic | 0.25 |
| empathy | 0.25 |
| ethical company | 0.25 |
| fake | 0.25 |
| fidelity and | 0.25 |
| fidelity for | 0.25 |
| fidelity good | 0.25 |
| fidelity the | 0.25 |
| fidelity very | 0.25 |
| for fidelity | 0.25 |
| giving | 0.25 |
| good communication | 0.25 |
| good culture | 0.25 |
| good reputation | 0.25 |
| having good | 0.25 |
| high integrity | 0.25 |
| honest review | 0.25 |
| honestly | 0.25 |
| insecurity | 0.25 |
| intelligent | 0.25 |
| interpersonal | 0.25 |
| loyal | 0.25 |
| money | 0.25 |
| money good | 0.25 |
| morals | 0.25 |
| privacy | 0.25 |
| pure | 0.25 |
| qualities | 0.25 |
| relationship | 0.25 |
| reliable | 0.25 |
| reputable | 0.25 |
| reputation for | 0.25 |
| reputation good | 0.25 |
| reputation the | 0.25 |
| reputed | 0.25 |
| respect | 0.25 |
| secretive | 0.25 |
| secure | 0.25 |
| secured | 0.25 |
| security | 0.25 |
| selfish | 0.25 |
| taste | 0.25 |
| the fidelity | 0.25 |
| the reputation | 0.25 |
| the truth | 0.25 |
| transparency from | 0.25 |
| trust the | 0.25 |
| valid | 0.25 |
| with fidelity | 0.25 |
| quality | 0.2 |
| and ethics | 0.1667 |
| and integrity | 0.1667 |
| appreciation | 0.1667 |
| caring | 0.1667 |
| character | 0.1667 |
| clean | 0.1667 |
| comfort | 0.1667 |
| corporate trust | 0.1667 |
| deserve | 0.1667 |
| doing good | 0.1667 |
| engaging | 0.1667 |
| enjoyable | 0.1667 |
| ethic and | 0.1667 |
| exposed | 0.1667 |
| happiness | 0.1667 |
| intelligence | 0.1667 |
| letting | 0.1667 |
| polite | 0.1667 |
| quality people | 0.1667 |
| quality the | 0.1667 |
| respect the | 0.1667 |
| respected and | 0.1667 |
| respected the | 0.1667 |
| respectful | 0.1667 |
| security and | 0.1667 |
| sharing | 0.1667 |
| strong reputation | 0.1667 |
| supportive and | 0.1667 |
| thoughtful | 0.1667 |
| transparency and | 0.1667 |
| valuable | 0.1667 |
| well respected | 0.1667 |
| with respect | 0.1667 |
| work ethics | 0.1667 |
| worthy | 0.1667 |
| agreement | 0.125 |
| attitude towards | 0.125 |
| caring and | 0.125 |
| compassion | 0.125 |
| dealt with | 0.125 |
| ethic | 0.125 |
| ethical and | 0.125 |
| experiences and | 0.125 |
| justify | 0.125 |
| keeping | 0.125 |
| logic | 0.125 |
| meaningful | 0.125 |
| perceived | 0.125 |
| quality and | 0.125 |
| quality life | 0.125 |
| quality work | 0.125 |
| respect for | 0.125 |
| respectable | 0.125 |
| respected | 0.125 |
| strict | 0.125 |
| the quality | 0.125 |
| unqualified | 0.125 |
| valued | 0.125 |
| willingness | 0.125 |
| accountable | 0.1 |
| compliance | 0.1 |
| consideration | 0.1 |
| constructive | 0.1 |
| cooperation | 0.1 |
| ethics and | 0.1 |
| experience and | 0.1 |
| integrity and | 0.1 |
| job | 0.1 |
| society | 0.1 |
| the rules | 0.1 |
| trust and | 0.1 |
| behaviour | 0.0833 |
| communicated | 0.0833 |
| culturally | 0.0833 |
| culture and | 0.0833 |
| dealing with | 0.0833 |
| effectively | 0.0833 |
| respect and | 0.0833 |
| respectful and | 0.0833 |
| responsible | 0.0833 |
| accountability for | 0.0714 |
| making good | 0.0714 |
| responsibility | 0.0714 |
| the decisions | 0.0714 |
| understanding | 0.0714 |
| accountability | 0.0625 |
| responsibility and | 0.0556 |

## B.8 `performance` — 188 terms

| Term | Weight |
|---|---|
| high performers | 1 |
| meritocracy | 1 |
| performance culture | 1 |
| high performing | 0.75 |
| excellence | 0.6667 |
| high standards | 0.6667 |
| awards | 0.5 |
| culture excellence | 0.5 |
| good performance | 0.5 |
| high end | 0.5 |
| high level | 0.5 |
| high performer | 0.5 |
| high quality | 0.5 |
| highly competitive | 0.5 |
| performance good | 0.5 |
| performance great | 0.5 |
| superior | 0.5 |
| talent | 0.5 |
| talents | 0.5 |
| than performance | 0.5 |
| top talent | 0.5 |
| high performance | 0.375 |
| accomplishments | 0.3333 |
| based merit | 0.3333 |
| company performance | 0.3333 |
| competetive | 0.3333 |
| individual performance | 0.3333 |
| merit based | 0.3333 |
| performance | 0.3333 |
| performance based | 0.3333 |
| performance management | 0.3333 |
| performing | 0.3333 |
| strong leadership | 0.3333 |
| well structured | 0.3333 |
| accountability | 0.25 |
| and merit | 0.25 |
| are highly | 0.25 |
| are outstanding | 0.25 |
| assessment | 0.25 |
| based performance | 0.25 |
| best talent | 0.25 |
| competent | 0.25 |
| competitive good | 0.25 |
| competitive great | 0.25 |
| culture highly | 0.25 |
| extremely competitive | 0.25 |
| fast track | 0.25 |
| for performance | 0.25 |
| good leadership | 0.25 |
| good organisation | 0.25 |
| good talent | 0.25 |
| high compensation | 0.25 |
| high expectations | 0.25 |
| high integrity | 0.25 |
| high pace | 0.25 |
| high standard | 0.25 |
| higher level | 0.25 |
| higher management | 0.25 |
| highly regarded | 0.25 |
| highly talented | 0.25 |
| leadership from | 0.25 |
| leadership good | 0.25 |
| leadership great | 0.25 |
| management high | 0.25 |
| merit | 0.25 |
| merit increases | 0.25 |
| meritocratic | 0.25 |
| opportunities high | 0.25 |
| organisation good | 0.25 |
| perform well | 0.25 |
| performance bonus | 0.25 |
| performance driven | 0.25 |
| performance review | 0.25 |
| performance reviews | 0.25 |
| promoted | 0.25 |
| promoted and | 0.25 |
| promoted within | 0.25 |
| promotion based | 0.25 |
| seniority | 0.25 |
| standards | 0.25 |
| standards great | 0.25 |
| superiors | 0.25 |
| talent and | 0.25 |
| talent development | 0.25 |
| talent pool | 0.25 |
| talent retention | 0.25 |
| talent the | 0.25 |
| talented employees | 0.25 |
| talented individuals | 0.25 |
| the ranks | 0.25 |
| top level | 0.25 |
| top management | 0.25 |
| top performer | 0.25 |
| top performers | 0.25 |
| top tier | 0.25 |
| achievement | 0.2 |
| evaluations | 0.2 |
| improvement | 0.2 |
| improving | 0.2 |
| quality | 0.2 |
| success | 0.2 |
| achieved | 0.1667 |
| advancement within | 0.1667 |
| advancements | 0.1667 |
| and performance | 0.1667 |
| competence | 0.1667 |
| competent people | 0.1667 |
| competition | 0.1667 |
| competitive | 0.1667 |
| competitive and | 0.1667 |
| competitive for | 0.1667 |
| competitiveness | 0.1667 |
| effective | 0.1667 |
| efficiency | 0.1667 |
| employees well | 0.1667 |
| evaluation | 0.1667 |
| for excellence | 0.1667 |
| good management | 0.1667 |
| high work | 0.1667 |
| job performance | 0.1667 |
| leaders | 0.1667 |
| management excellent | 0.1667 |
| promotion and | 0.1667 |
| resourceful | 0.1667 |
| skilled people | 0.1667 |
| standards good | 0.1667 |
| talented colleagues | 0.1667 |
| the competition | 0.1667 |
| the talent | 0.1667 |
| upper management | 0.1667 |
| achieving | 0.1429 |
| leadership | 0.1429 |
| advancement | 0.125 |
| advancement and | 0.125 |
| career | 0.125 |
| career and | 0.125 |
| compete with | 0.125 |
| competent and | 0.125 |
| for improvement | 0.125 |
| leadership that | 0.125 |
| objectives | 0.125 |
| perform | 0.125 |
| quality and | 0.125 |
| quality work | 0.125 |
| skill | 0.125 |
| skills and | 0.125 |
| standards for | 0.125 |
| strong management | 0.125 |
| the career | 0.125 |
| the leadership | 0.125 |
| the skills | 0.125 |
| work quality | 0.125 |
| achieve | 0.1111 |
| accountable | 0.1 |
| based experience | 0.1 |
| capabilities | 0.1 |
| capability | 0.1 |
| efficiency and | 0.1 |
| efficiently | 0.1 |
| executives | 0.1 |
| for advancement | 0.1 |
| from leadership | 0.1 |
| improvement and | 0.1 |
| performance but | 0.1 |
| qualified | 0.1 |
| skills | 0.1 |
| standards and | 0.1 |
| success and | 0.1 |
| the success | 0.1 |
| your performance | 0.1 |
| competitive with | 0.0833 |
| efficient | 0.0833 |
| leadership with | 0.0833 |
| performance and | 0.0833 |
| performance the | 0.0833 |
| regarded | 0.0833 |
| the performance | 0.0833 |
| accountability for | 0.0714 |
| advanced | 0.0714 |
| competition and | 0.0714 |
| making good | 0.0714 |
| management | 0.0714 |
| production | 0.0714 |
| based work | 0.0625 |
| leadership and | 0.0625 |
| organizational | 0.0625 |
| management and | 0.0556 |
| responsibilities and | 0.0556 |

## B.9 `respect` — 183 terms

| Term | Weight |
|---|---|
| safe | 1 |
| supportive | 1 |
| caring | 0.6667 |
| care | 0.5 |
| care about | 0.5 |
| cared about | 0.5 |
| caring about | 0.5 |
| concern | 0.5 |
| concerns | 0.5 |
| feelings | 0.5 |
| genuinely care | 0.5 |
| good care | 0.5 |
| helping people | 0.5 |
| people care | 0.5 |
| respect | 0.5 |
| support | 0.5 |
| supportive good | 0.5 |
| supportive people | 0.5 |
| taken care | 0.5 |
| takes care | 0.5 |
| taking care | 0.5 |
| treat people | 0.5 |
| treated | 0.5 |
| treated like | 0.5 |
| treated well | 0.5 |
| valued | 0.5 |
| consideration | 0.4 |
| appreciation | 0.3333 |
| comfort | 0.3333 |
| deserve | 0.3333 |
| emotionally | 0.3333 |
| feel valued | 0.3333 |
| loyalty | 0.3333 |
| peace | 0.3333 |
| polite | 0.3333 |
| regard | 0.3333 |
| respectful | 0.3333 |
| respects | 0.3333 |
| supportive culture | 0.3333 |
| supportive environment | 0.3333 |
| thoughtful | 0.3333 |
| well respected | 0.3333 |
| worthy | 0.3333 |
| and care | 0.25 |
| and caring | 0.25 |
| and respect | 0.25 |
| and respected | 0.25 |
| and respectful | 0.25 |
| appreciation for | 0.25 |
| being very | 0.25 |
| care and | 0.25 |
| care for | 0.25 |
| care more | 0.25 |
| care the | 0.25 |
| care their | 0.25 |
| care you | 0.25 |
| cared | 0.25 |
| compassion | 0.25 |
| concern for | 0.25 |
| concerned about | 0.25 |
| concerned with | 0.25 |
| deserving | 0.25 |
| empathy | 0.25 |
| environment supportive | 0.25 |
| ethical | 0.25 |
| favour | 0.25 |
| feel comfortable | 0.25 |
| for individuals | 0.25 |
| for support | 0.25 |
| good health | 0.25 |
| handle the | 0.25 |
| healthy | 0.25 |
| hearted | 0.25 |
| helpful people | 0.25 |
| judged | 0.25 |
| lives | 0.25 |
| love | 0.25 |
| moral | 0.25 |
| morals | 0.25 |
| patient | 0.25 |
| peaceful | 0.25 |
| protecting | 0.25 |
| qualities | 0.25 |
| regard for | 0.25 |
| respectable | 0.25 |
| respected | 0.25 |
| role you | 0.25 |
| rude | 0.25 |
| seem care | 0.25 |
| support and | 0.25 |
| support for | 0.25 |
| support good | 0.25 |
| support the | 0.25 |
| support you | 0.25 |
| support your | 0.25 |
| supporting | 0.25 |
| supportive the | 0.25 |
| take care | 0.25 |
| the health | 0.25 |
| the support | 0.25 |
| they care | 0.25 |
| tolerate | 0.25 |
| treat their | 0.25 |
| treat you | 0.25 |
| treating | 0.25 |
| treats you | 0.25 |
| truly care | 0.25 |
| with supportive | 0.25 |
| your health | 0.25 |
| attitude | 0.2 |
| ethics | 0.2 |
| integrity | 0.2 |
| character | 0.1667 |
| consideration for | 0.1667 |
| culture supportive | 0.1667 |
| doing good | 0.1667 |
| emotional | 0.1667 |
| enjoyable | 0.1667 |
| fairness | 0.1667 |
| good values | 0.1667 |
| honesty | 0.1667 |
| letting | 0.1667 |
| living the | 0.1667 |
| quality people | 0.1667 |
| quality the | 0.1667 |
| respect the | 0.1667 |
| respected and | 0.1667 |
| respected the | 0.1667 |
| rights | 0.1667 |
| sincere | 0.1667 |
| supportive and | 0.1667 |
| tolerated | 0.1667 |
| trustworthy | 0.1667 |
| valuable | 0.1667 |
| valued and | 0.1667 |
| with respect | 0.1667 |
| responsibility | 0.1429 |
| attitude towards | 0.125 |
| attitudes | 0.125 |
| balanced | 0.125 |
| caring and | 0.125 |
| dealt with | 0.125 |
| ethic | 0.125 |
| ethical and | 0.125 |
| keeping | 0.125 |
| meaningful | 0.125 |
| morale | 0.125 |
| morale and | 0.125 |
| more responsibility | 0.125 |
| positive attitude | 0.125 |
| quality life | 0.125 |
| respect for | 0.125 |
| responsibility for | 0.125 |
| secure | 0.125 |
| supportive work | 0.125 |
| take responsibility | 0.125 |
| the quality | 0.125 |
| trust | 0.125 |
| valuable experience | 0.125 |
| welcoming and | 0.125 |
| attitude and | 0.1 |
| dealing | 0.1 |
| ethics and | 0.1 |
| integrity and | 0.1 |
| responsibilities | 0.1 |
| society | 0.1 |
| trust and | 0.1 |
| welcoming | 0.1 |
| culturally | 0.0833 |
| culture | 0.0833 |
| culture and | 0.0833 |
| dealing with | 0.0833 |
| handling | 0.0833 |
| regarded | 0.0833 |
| respect and | 0.0833 |
| respectful and | 0.0833 |
| responsible | 0.0833 |
| the culture | 0.0833 |
| accountability for | 0.0714 |
| understanding | 0.0714 |
| accountability | 0.0625 |
| responsibilities and | 0.0556 |
| responsibility and | 0.0556 |

*Total MIT terms: 1616*


# Appendix C — Schroders 18-Dimension Keyword Dictionaries (complete)

Score = (pos − neg) / (pos + neg). Weights: High = 1.0, Medium = 0.75, Low = 0.25.


## C.1 `d01` — Purpose & Mission Orientation (attribute; low = Weak, high = Strong)


*How strongly the organisation is driven by a clear sense of purpose and mission. A higher score means a stronger shared sense of why the company exists.*


### Positive-direction terms (push score toward +1 / "Strong") — 90 terms

| Term | Weight |
|---|---|
| aspiration | 1 |
| aspire | 1 |
| belief system | 1 |
| bigger picture | 1 |
| cause | 1 |
| committed | 1 |
| ethos | 1 |
| fulfillment | 1 |
| greater good | 1 |
| higher purpose | 1 |
| impact | 1 |
| inspire | 1 |
| meaning | 1 |
| meaningful | 1 |
| mission | 1 |
| north star | 1 |
| passion | 1 |
| purpose | 1 |
| purpose-driven | 1 |
| purposeful | 1 |
| purposive | 1 |
| shared purpose | 1 |
| social good | 1 |
| unifying | 1 |
| values | 1 |
| vision | 1 |
| altruistic | 0.75 |
| ambition | 0.75 |
| authentic | 0.75 |
| belief | 0.75 |
| betterment | 0.75 |
| calling | 0.75 |
| charter | 0.75 |
| commitment | 0.75 |
| community | 0.75 |
| compass | 0.75 |
| conscientious | 0.75 |
| contribution | 0.75 |
| conviction | 0.75 |
| creed | 0.75 |
| dedicated | 0.75 |
| devoted | 0.75 |
| devotion | 0.75 |
| direction | 0.75 |
| driven | 0.75 |
| empowerment | 0.75 |
| enriching | 0.75 |
| galvanise | 0.75 |
| genuine | 0.75 |
| guiding | 0.75 |
| humanitarian | 0.75 |
| idealistic | 0.75 |
| ideals | 0.75 |
| intentional | 0.75 |
| legacy | 0.75 |
| manifesto | 0.75 |
| motive | 0.75 |
| noble | 0.75 |
| pledge | 0.75 |
| principle | 0.75 |
| principled | 0.75 |
| rallying | 0.75 |
| resolute | 0.75 |
| sincere | 0.75 |
| societal | 0.75 |
| soul | 0.75 |
| stakeholder | 0.75 |
| stewardship | 0.75 |
| sustainability | 0.75 |
| transcendent | 0.75 |
| transformative | 0.75 |
| unified | 0.75 |
| vocation | 0.75 |
| why | 0.75 |
| catalyst | 0.25 |
| charitable | 0.25 |
| clarion | 0.25 |
| covenant | 0.25 |
| crusade | 0.25 |
| enlightened | 0.25 |
| essence | 0.25 |
| groundbreaking | 0.25 |
| heartfelt | 0.25 |
| imperative | 0.25 |
| mandate | 0.25 |
| narrative | 0.25 |
| overarching | 0.25 |
| servant | 0.25 |
| story | 0.25 |
| symbolic | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 9 terms

| Term | Weight |
|---|---|
| aimless | 1 |
| directionless | 1 |
| mercenary | 1 |
| pointless | 1 |
| profit-only | 1 |
| purposeless | 1 |
| rudderless | 1 |
| cynical | 0.75 |
| short-sighted | 0.75 |

## C.2 `d02` — Long-term / Future Orientation (attribute; low = Weak, high = Strong)


*How much the organisation plans for the long term rather than chasing short-term results. A higher score means more focus on the future.*


### Positive-direction terms (push score toward +1 / "Strong") — 89 terms

| Term | Weight |
|---|---|
| anchor investment | 1 |
| big picture | 1 |
| continuity | 1 |
| decade | 1 |
| decade ahead | 1 |
| decade-long | 1 |
| decades | 1 |
| deep-rooted | 1 |
| endure | 1 |
| enduring | 1 |
| five years | 1 |
| five-year | 1 |
| foresight | 1 |
| forward-looking | 1 |
| future | 1 |
| generational | 1 |
| generationally | 1 |
| horizon | 1 |
| intergenerational | 1 |
| invest for future | 1 |
| invest in people | 1 |
| invest in training | 1 |
| legacy | 1 |
| long view | 1 |
| long-term | 1 |
| longevity | 1 |
| multi-generational | 1 |
| multi-year | 1 |
| patience | 1 |
| patient | 1 |
| perpetuity | 1 |
| planted seeds | 1 |
| stake in future | 1 |
| stewardship | 1 |
| sustainable | 1 |
| sustained | 1 |
| ten-year | 1 |
| think ahead | 1 |
| value creation | 1 |
| visionary | 1 |
| anchor | 0.75 |
| architecting | 0.75 |
| bedrock | 0.75 |
| build | 0.75 |
| century | 0.75 |
| climate | 0.75 |
| commit | 0.75 |
| compounding | 0.75 |
| consistent | 0.75 |
| credibility | 0.75 |
| delayed | 0.75 |
| durable | 0.75 |
| evergreen | 0.75 |
| evolve | 0.75 |
| evolving | 0.75 |
| forecast | 0.75 |
| foundational | 0.75 |
| harvest | 0.75 |
| infrastructure | 0.75 |
| institutional | 0.75 |
| invest | 0.75 |
| permanent | 0.75 |
| perpetual | 0.75 |
| perpetuate | 0.75 |
| perseverance | 0.75 |
| persist | 0.75 |
| pipeline | 0.75 |
| plan | 0.75 |
| platform | 0.75 |
| preserve | 0.75 |
| projection | 0.75 |
| resilient | 0.75 |
| roadmap | 0.75 |
| slow and steady | 0.75 |
| sow | 0.75 |
| stability | 0.75 |
| steady | 0.75 |
| strategic | 0.75 |
| succession | 0.75 |
| timeless | 0.75 |
| trajectory | 0.75 |
| accumulate | 0.25 |
| annual plan | 0.25 |
| capital | 0.25 |
| cycle | 0.25 |
| landmark | 0.25 |
| mature | 0.25 |
| protect | 0.25 |
| trustworthy | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 13 terms

| Term | Weight |
|---|---|
| earnings driven | 1 |
| myopic | 1 |
| next quarter | 1 |
| quarter-to-quarter | 1 |
| quarterly | 1 |
| reactive | 1 |
| short-sighted | 1 |
| short-term | 1 |
| firefighting | 0.75 |
| immediate | 0.75 |
| survive | 0.75 |
| now | 0.25 |
| overnight | 0.25 |

## C.3 `d03` — Collaborative vs Individualistic (bipolar; low = Individualistic, high = Collaborative)


*Whether people tend to work together as a team (higher score) or mostly work on their own (lower score).*


### Positive-direction terms (push score toward +1 / "Collaborative") — 76 terms

| Term | Weight |
|---|---|
| camaraderie | 1 |
| co-create | 1 |
| co-operate | 1 |
| cohesive | 1 |
| collaboration | 1 |
| collaborative spirit | 1 |
| collective | 1 |
| collegial | 1 |
| common goal | 1 |
| cooperative | 1 |
| cross-functional | 1 |
| cross-team | 1 |
| inter-departmental | 1 |
| interdependent | 1 |
| knowledge sharing | 1 |
| mutual support | 1 |
| no silos | 1 |
| one team | 1 |
| open collaboration | 1 |
| partnering | 1 |
| pulling together | 1 |
| recognise team | 1 |
| share credit | 1 |
| solidarity | 1 |
| team player | 1 |
| teamwork | 1 |
| together | 1 |
| together forward | 1 |
| together we | 1 |
| united | 1 |
| we not i | 1 |
| acknowledge | 0.75 |
| align | 0.75 |
| aligned | 0.75 |
| alliance | 0.75 |
| bridge | 0.75 |
| coalition | 0.75 |
| community | 0.75 |
| consensus | 0.75 |
| contribute | 0.75 |
| coordinate | 0.75 |
| enterprise-wide | 0.75 |
| fellowship | 0.75 |
| flat structure | 0.75 |
| give credit | 0.75 |
| group | 0.75 |
| help | 0.75 |
| inclusive | 0.75 |
| joint | 0.75 |
| merge ideas | 0.75 |
| mutual | 0.75 |
| networked | 0.75 |
| no i in team | 0.75 |
| open door | 0.75 |
| partner | 0.75 |
| peer | 0.75 |
| pool | 0.75 |
| share | 0.75 |
| shared | 0.75 |
| support | 0.75 |
| synergy | 0.75 |
| unified | 0.75 |
| assist | 0.25 |
| band | 0.25 |
| blend | 0.25 |
| brotherhood | 0.25 |
| buddy | 0.25 |
| cluster | 0.25 |
| co-author | 0.25 |
| co-worker | 0.25 |
| comrade | 0.25 |
| guild | 0.25 |
| hive | 0.25 |
| interlocking | 0.25 |
| pack | 0.25 |
| tribe | 0.25 |

### Negative-direction terms (push score toward −1 / "Individualistic") — 20 terms

| Term | Weight |
|---|---|
| autonomous | 1 |
| competitive internally | 1 |
| disconnected | 1 |
| fragmented | 1 |
| hoarding | 1 |
| independent | 1 |
| isolated | 1 |
| lone wolf | 1 |
| self-directed | 1 |
| self-reliant | 1 |
| silo | 1 |
| siloed | 1 |
| turf | 1 |
| credit | 0.75 |
| individual | 0.75 |
| ownership | 0.75 |
| solo | 0.75 |
| accountability | 0.25 |
| hero | 0.25 |
| star | 0.25 |

## C.4 `d04` — Hierarchical vs Egalitarian (bipolar; low = Egalitarian, high = Hierarchical)


*Whether decisions flow through clear levels of authority and seniority (higher score) or power and say are shared more equally (lower score).*


### Positive-direction terms (push score toward +1 / "Hierarchical") — 82 terms

| Term | Weight |
|---|---|
| ask permission | 1 |
| authority | 1 |
| boss | 1 |
| bottleneck | 1 |
| bureaucracy | 1 |
| centralized | 1 |
| chain of command | 1 |
| clique | 1 |
| closed door | 1 |
| command | 1 |
| decisions above | 1 |
| deference | 1 |
| deferential | 1 |
| directive | 1 |
| fixed roles | 1 |
| gatekeeping | 1 |
| hierarchy | 1 |
| hierarchy stifles | 1 |
| inaccessible | 1 |
| inner circle | 1 |
| layers | 1 |
| micromanagement | 1 |
| need approval | 1 |
| obey | 1 |
| old boys | 1 |
| org chart | 1 |
| override | 1 |
| permission | 1 |
| power distance | 1 |
| rank | 1 |
| red tape | 1 |
| reporting line | 1 |
| rigid structure | 1 |
| seniority | 1 |
| sign-off | 1 |
| stifling | 1 |
| subordinate | 1 |
| superior | 1 |
| tier | 1 |
| top-down | 1 |
| two-tier | 1 |
| unapproachable | 1 |
| untouchable | 1 |
| veto | 1 |
| approval | 0.75 |
| blocked | 0.75 |
| career ladder | 0.75 |
| class | 0.75 |
| constrained | 0.75 |
| control | 0.75 |
| distant | 0.75 |
| escalate | 0.75 |
| formal | 0.75 |
| grade | 0.75 |
| inflexible | 0.75 |
| ladder | 0.75 |
| level | 0.75 |
| locked | 0.75 |
| management | 0.75 |
| politics | 0.75 |
| power | 0.75 |
| prescribed | 0.75 |
| protocols | 0.75 |
| report to | 0.75 |
| rigid | 0.75 |
| senior | 0.75 |
| silos | 0.75 |
| status | 0.75 |
| tenure | 0.75 |
| title | 0.75 |
| accountable to | 0.25 |
| band | 0.25 |
| c-suite | 0.25 |
| convention | 0.25 |
| director | 0.25 |
| executive | 0.25 |
| officer | 0.25 |
| order | 0.25 |
| policy | 0.25 |
| promotion | 0.25 |
| traditional | 0.25 |
| vp | 0.25 |

### Negative-direction terms (push score toward −1 / "Egalitarian") — 15 terms

| Term | Weight |
|---|---|
| democratic | 1 |
| egalitarian | 1 |
| empowered | 1 |
| equal voice | 1 |
| everyone equal | 1 |
| flat | 1 |
| horizontal | 1 |
| junior can challenge | 1 |
| leaders listen | 1 |
| no hierarchy | 1 |
| open door | 1 |
| self-managing | 1 |
| access | 0.75 |
| approachable | 0.75 |
| distributed | 0.75 |

## C.5 `d05` — Psychological Safety & Openness (attribute; low = Weak, high = Strong)


*How safe people feel to speak up, share ideas and admit mistakes without fear. A higher score means a more open, trusting environment.*


### Positive-direction terms (push score toward +1 / "Strong") — 69 terms

| Term | Weight |
|---|---|
| act on feedback | 1 |
| address concerns | 1 |
| admit mistakes | 1 |
| authentic | 1 |
| candid | 1 |
| candour | 1 |
| challenge | 1 |
| comfort to raise | 1 |
| dissent | 1 |
| embrace error | 1 |
| escalate safely | 1 |
| fail forward | 1 |
| fearless | 1 |
| feedback | 1 |
| forthright | 1 |
| frankness | 1 |
| heard | 1 |
| honest | 1 |
| inclusive debate | 1 |
| invite feedback | 1 |
| learn from failure | 1 |
| listen | 1 |
| no blame | 1 |
| no fear | 1 |
| no hidden agenda | 1 |
| no judgement | 1 |
| no retaliation | 1 |
| non-judgmental | 1 |
| open | 1 |
| openness | 1 |
| plain speaking | 1 |
| psychological safety | 1 |
| question | 1 |
| radical honesty | 1 |
| raise concerns | 1 |
| raise hand | 1 |
| safe | 1 |
| safe environment | 1 |
| safe space | 1 |
| safe to fail | 1 |
| speak out | 1 |
| speak up | 1 |
| straight talk | 1 |
| transparent | 1 |
| trust | 1 |
| voice | 1 |
| vulnerability | 1 |
| welcome criticism | 1 |
| bold | 0.75 |
| brave | 0.75 |
| call out | 0.75 |
| clear air | 0.75 |
| constructive | 0.75 |
| courage | 0.75 |
| courageous | 0.75 |
| direct | 0.75 |
| empathy | 0.75 |
| experiment | 0.75 |
| express | 0.75 |
| integrity | 0.75 |
| protection | 0.75 |
| respected | 0.75 |
| respond | 0.75 |
| secure | 0.75 |
| understood | 0.75 |
| unfiltered | 0.75 |
| welcoming | 0.75 |
| whistleblower | 0.75 |
| asylum | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 32 terms

| Term | Weight |
|---|---|
| afraid | 1 |
| blame | 1 |
| bullying | 1 |
| cannot question | 1 |
| closed | 1 |
| code of silence | 1 |
| cover up | 1 |
| defensive | 1 |
| dismiss | 1 |
| fear of speaking | 1 |
| fearful | 1 |
| hide mistakes | 1 |
| ignored | 1 |
| intimidation | 1 |
| marginalised | 1 |
| muted | 1 |
| no feedback | 1 |
| omerta | 1 |
| opaque | 1 |
| punished | 1 |
| retaliation | 1 |
| retribution | 1 |
| ridiculed | 1 |
| secretive | 1 |
| shoot messenger | 1 |
| shut down | 1 |
| silence | 1 |
| suppressed | 1 |
| swept under carpet | 1 |
| taboo | 1 |
| unspeakable | 1 |
| walking on eggshells | 1 |

## C.6 `d06` — Caring & People-Centricity (attribute; low = Weak, high = Strong)


*How genuinely the organisation cares about its people and their wellbeing. A higher score means people feel more supported.*


### Positive-direction terms (push score toward +1 / "Strong") — 86 terms

| Term | Weight |
|---|---|
| accommodating | 1 |
| appreciated | 1 |
| balance | 1 |
| belonging | 1 |
| care | 1 |
| check in | 1 |
| coach | 1 |
| compassion | 1 |
| dignity | 1 |
| empathy | 1 |
| fair treatment | 1 |
| family | 1 |
| flourish | 1 |
| friendly | 1 |
| fulfilment | 1 |
| happiness | 1 |
| health | 1 |
| humane | 1 |
| invest in people | 1 |
| kindness | 1 |
| look after | 1 |
| mental health | 1 |
| mentor | 1 |
| morale | 1 |
| non-discriminatory | 1 |
| nurture | 1 |
| parental | 1 |
| pastoral | 1 |
| people first | 1 |
| recognition | 1 |
| respect | 1 |
| support | 1 |
| thriving | 1 |
| understanding | 1 |
| valued | 1 |
| warm | 1 |
| welfare | 1 |
| wellbeing | 1 |
| whole person | 1 |
| work-life | 1 |
| acknowledge | 0.75 |
| amicable | 0.75 |
| approachable | 0.75 |
| attachment | 0.75 |
| benefits | 0.75 |
| blossom | 0.75 |
| bond | 0.75 |
| career | 0.75 |
| celebrate | 0.75 |
| collegial | 0.75 |
| community | 0.75 |
| develop | 0.75 |
| energised | 0.75 |
| engagement | 0.75 |
| equal | 0.75 |
| flexible | 0.75 |
| generous | 0.75 |
| gracious | 0.75 |
| growth | 0.75 |
| health days | 0.75 |
| holistic | 0.75 |
| humanitarian | 0.75 |
| inclusive | 0.75 |
| inspired | 0.75 |
| joy | 0.75 |
| leave | 0.75 |
| listen | 0.75 |
| loyalty | 0.75 |
| motivated | 0.75 |
| opportunity | 0.75 |
| promotion | 0.75 |
| protected | 0.75 |
| retention | 0.75 |
| reward | 0.75 |
| safe | 0.75 |
| sensitively | 0.75 |
| sick leave | 0.75 |
| tactful | 0.75 |
| charitable | 0.25 |
| genial | 0.25 |
| gym | 0.25 |
| insurance | 0.25 |
| perks | 0.25 |
| social | 0.25 |
| subsidy | 0.25 |
| team events | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 15 terms

| Term | Weight |
|---|---|
| burnout | 1 |
| cold | 1 |
| dehumanising | 1 |
| disposable | 1 |
| exhausted | 1 |
| exploited | 1 |
| heartless | 1 |
| ignored | 1 |
| no support | 1 |
| overworked | 1 |
| replaceable | 1 |
| sink or swim | 1 |
| transactional | 1 |
| treated like a number | 1 |
| uncaring | 1 |

## C.7 `d07` — Performance & Execution Orientation (attribute; low = Weak, high = Strong)


*How focused the organisation is on delivering results and getting things done. A higher score means a stronger drive to execute.*


### Positive-direction terms (push score toward +1 / "Strong") — 83 terms

| Term | Weight |
|---|---|
| a players | 1 |
| accountability | 1 |
| achieve | 1 |
| ambition | 1 |
| best in class | 1 |
| clear expectations | 1 |
| competitive culture | 1 |
| continuous improvement | 1 |
| data-driven | 1 |
| deadline | 1 |
| deliver | 1 |
| demanding | 1 |
| disciplined | 1 |
| driven | 1 |
| driven to succeed | 1 |
| effectiveness | 1 |
| efficiency | 1 |
| exceed | 1 |
| excel | 1 |
| execution | 1 |
| focused | 1 |
| forced ranking | 1 |
| goal | 1 |
| high achiever | 1 |
| high bar | 1 |
| high performance | 1 |
| kpis | 1 |
| laser focused | 1 |
| measurement | 1 |
| meet target | 1 |
| merit | 1 |
| metrics | 1 |
| milestone | 1 |
| number one | 1 |
| objective | 1 |
| on-time | 1 |
| operational excellence | 1 |
| outcome | 1 |
| outperform | 1 |
| output | 1 |
| overachiever | 1 |
| overdeliver | 1 |
| ownership | 1 |
| pay for performance | 1 |
| performance | 1 |
| pressure | 1 |
| quota | 1 |
| raise the bar | 1 |
| rank and yank | 1 |
| relentless | 1 |
| results | 1 |
| reward performance | 1 |
| scorecard | 1 |
| stretch | 1 |
| targets | 1 |
| top performer | 1 |
| track record | 1 |
| vitality curve | 1 |
| win | 1 |
| agile execution | 0.75 |
| analytics | 0.75 |
| benchmark | 0.75 |
| bonus | 0.75 |
| challenge | 0.75 |
| champion | 0.75 |
| competitive | 0.75 |
| growth | 0.75 |
| hustle | 0.75 |
| incentive | 0.75 |
| market leader | 0.75 |
| profit | 0.75 |
| push | 0.75 |
| revenue | 0.75 |
| rigorous | 0.75 |
| sales | 0.75 |
| speed | 0.75 |
| sprint | 0.75 |
| standards | 0.75 |
| talent | 0.75 |
| timely | 0.75 |
| winner | 0.75 |
| accolade | 0.25 |
| trophy | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 16 terms

| Term | Weight |
|---|---|
| complacent | 1 |
| fail to deliver | 1 |
| ineffective | 1 |
| lazy | 1 |
| low performance | 1 |
| mediocre | 1 |
| miss target | 1 |
| no accountability | 1 |
| no consequences | 1 |
| poor results | 1 |
| underperform | 1 |
| unproductive | 1 |
| coast | 0.75 |
| inefficient | 0.75 |
| slow | 0.75 |
| stagnant | 0.75 |

## C.8 `d08` — Process & Rule Orientation (attribute; low = Weak, high = Strong)


*How much the organisation relies on defined processes, rules and procedures. A higher score means more structure and formality.*


### Positive-direction terms (push score toward +1 / "Strong") — 92 terms

| Term | Weight |
|---|---|
| accuracy | 1 |
| approval | 1 |
| approvals process | 1 |
| audit | 1 |
| box-ticking | 1 |
| bureaucracy | 1 |
| bureaucratic | 1 |
| careful | 1 |
| checklist | 1 |
| clearance | 1 |
| codified | 1 |
| compliance | 1 |
| consistent | 1 |
| control | 1 |
| defined | 1 |
| detail-oriented | 1 |
| diligent | 1 |
| discipline | 1 |
| documented | 1 |
| enforcement | 1 |
| escalation | 1 |
| formalised | 1 |
| framework | 1 |
| gating | 1 |
| governance | 1 |
| guidelines | 1 |
| hierarchy of approvals | 1 |
| inflexible | 1 |
| inspection | 1 |
| methodical | 1 |
| meticulous | 1 |
| monitoring | 1 |
| order | 1 |
| oversight | 1 |
| paperwork | 1 |
| penalties | 1 |
| permission | 1 |
| policy | 1 |
| precision | 1 |
| predictable | 1 |
| prescribed | 1 |
| procedure | 1 |
| process | 1 |
| protocol | 1 |
| quality control | 1 |
| red tape | 1 |
| regulated | 1 |
| reliable | 1 |
| repeatable | 1 |
| rigid | 1 |
| rigorous | 1 |
| routine | 1 |
| rules | 1 |
| sign off | 1 |
| sign-off | 1 |
| six sigma | 1 |
| slow | 1 |
| sops | 1 |
| specification | 1 |
| stage-gate | 1 |
| standard | 1 |
| stifling | 1 |
| structured | 1 |
| systematic | 1 |
| systematic approach | 1 |
| thorough | 1 |
| tightly controlled | 1 |
| too many rules | 1 |
| tqm | 1 |
| validate | 1 |
| verify | 1 |
| workflow | 1 |
| zero tolerance | 1 |
| accreditation | 0.75 |
| certification | 0.75 |
| certify | 0.75 |
| change management | 0.75 |
| consequences | 0.75 |
| crackdown | 0.75 |
| flowchart | 0.75 |
| instruction manual | 0.75 |
| iso | 0.75 |
| lean | 0.75 |
| mandate | 0.75 |
| micromanagement | 0.75 |
| requirements | 0.75 |
| review | 0.75 |
| rubber stamp | 0.75 |
| template | 0.75 |
| tracking | 0.75 |
| waterfall | 0.75 |
| notarize | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 13 terms

| Term | Weight |
|---|---|
| ad hoc | 1 |
| autonomous | 1 |
| chaotic | 1 |
| disorganised | 1 |
| flexible | 1 |
| freewheeling | 1 |
| inconsistent | 1 |
| informal | 1 |
| lack of autonomy | 1 |
| make it up | 1 |
| no process | 1 |
| unstructured | 1 |
| loose | 0.75 |

## C.9 `d09` — Integrity & Ethical Responsibility (attribute; low = Weak, high = Strong)


*How strongly the organisation acts honestly and takes ethical responsibility. A higher score means stronger ethics and accountability.*


### Positive-direction terms (push score toward +1 / "Strong") — 62 terms

| Term | Weight |
|---|---|
| above board | 1 |
| accountable | 1 |
| anti-corruption | 1 |
| authentic | 1 |
| beyond reproach | 1 |
| candour | 1 |
| code of conduct | 1 |
| duty | 1 |
| ethical | 1 |
| ethics training | 1 |
| fair | 1 |
| fairness | 1 |
| forthright | 1 |
| good character | 1 |
| honest | 1 |
| honour | 1 |
| honourable | 1 |
| impeccable | 1 |
| incorruptible | 1 |
| integrity | 1 |
| just | 1 |
| justice | 1 |
| moral | 1 |
| moral compass | 1 |
| no conflicts | 1 |
| no misconduct | 1 |
| ombudsman | 1 |
| principled | 1 |
| probity | 1 |
| rectitude | 1 |
| responsibility | 1 |
| right thing | 1 |
| sincerity | 1 |
| spotless record | 1 |
| straight | 1 |
| transparent | 1 |
| trustworthy | 1 |
| unimpeachable | 1 |
| upright | 1 |
| values | 1 |
| whistleblowing | 1 |
| zero tolerance | 1 |
| audit | 0.75 |
| clean | 0.75 |
| compliance | 0.75 |
| consistent | 0.75 |
| dependable | 0.75 |
| dignity | 0.75 |
| equity | 0.75 |
| genuine | 0.75 |
| law-abiding | 0.75 |
| obligation | 0.75 |
| openness | 0.75 |
| pledge | 0.75 |
| promise | 0.75 |
| reliable | 0.75 |
| respect | 0.75 |
| righteous | 0.75 |
| rightful | 0.75 |
| rule of law | 0.75 |
| speak up | 0.75 |
| virtue | 0.75 |

### Negative-direction terms (push score toward −1 / "Weak") — 40 terms

| Term | Weight |
|---|---|
| bend the rules | 1 |
| bribery | 1 |
| conflict of interest | 1 |
| corrupt | 1 |
| cover-up | 1 |
| creative accounting | 1 |
| cutting corners | 1 |
| deceptive | 1 |
| deceptive practice | 1 |
| dishonest | 1 |
| ends justify means | 1 |
| exploit | 1 |
| false claims | 1 |
| fraud | 1 |
| gaming the system | 1 |
| greed | 1 |
| greenwashing | 1 |
| hidden | 1 |
| kickback | 1 |
| lie | 1 |
| loophole | 1 |
| manipulation | 1 |
| misconduct | 1 |
| mislead | 1 |
| misleading | 1 |
| opaque | 1 |
| propaganda | 1 |
| redwashing | 1 |
| scandal | 1 |
| secretive | 1 |
| self-serving | 1 |
| socialwashing | 1 |
| suspicious | 1 |
| unethical | 1 |
| whitewash | 1 |
| window dressing | 1 |
| exaggerated | 0.75 |
| inflated | 0.75 |
| questionable | 0.75 |
| spin | 0.75 |

## C.10 `d10` — Innovation & Risk Appetite (attribute; low = Weak, high = Strong)


*How willing the organisation is to try new ideas and take sensible risks. A higher score means a bolder, more inventive culture.*


### Positive-direction terms (push score toward +1 / "Strong") — 88 terms

| Term | Weight |
|---|---|
| adventurous | 1 |
| bet | 1 |
| bold | 1 |
| breakthrough | 1 |
| calculated risk | 1 |
| challenge norms | 1 |
| creative | 1 |
| creative freedom | 1 |
| curiosity | 1 |
| cutting-edge | 1 |
| daring | 1 |
| discovery | 1 |
| disrupt | 1 |
| divergent | 1 |
| entrepreneurial | 1 |
| experiment | 1 |
| explore | 1 |
| fail fast | 1 |
| first mover | 1 |
| freedom to experiment | 1 |
| frontier | 1 |
| futuristic | 1 |
| hackathon | 1 |
| ideas | 1 |
| imagination | 1 |
| incubate | 1 |
| innovation | 1 |
| invent | 1 |
| invention | 1 |
| ip | 1 |
| iterate | 1 |
| labs | 1 |
| lateral thinking | 1 |
| moonshot | 1 |
| new ideas | 1 |
| no limits | 1 |
| novel | 1 |
| novel approach | 1 |
| open-minded | 1 |
| patent | 1 |
| pioneer | 1 |
| possibility | 1 |
| prototype | 1 |
| question everything | 1 |
| r&d | 1 |
| redesign | 1 |
| reimagine | 1 |
| reinvent | 1 |
| research | 1 |
| rethink | 1 |
| revolutionary | 1 |
| risk | 1 |
| sandbox | 1 |
| shake up | 1 |
| startup | 1 |
| test and learn | 1 |
| trailblazer | 1 |
| transform | 1 |
| unconventional | 1 |
| unconventional thinking | 1 |
| venture | 1 |
| visionary | 1 |
| accelerate | 0.75 |
| ai | 0.75 |
| ambitious | 0.75 |
| aspirational | 0.75 |
| avant-garde | 0.75 |
| boundless | 0.75 |
| catalyse | 0.75 |
| courageous | 0.75 |
| digital | 0.75 |
| energising | 0.75 |
| exciting | 0.75 |
| exhilarating | 0.75 |
| gamble | 0.75 |
| ignite | 0.75 |
| inspiring | 0.75 |
| new | 0.75 |
| overhaul | 0.75 |
| potential | 0.75 |
| provocative | 0.75 |
| rebuild | 0.75 |
| spark | 0.75 |
| stimulating | 0.75 |
| surprising | 0.75 |
| technology | 0.75 |
| unexpected | 0.75 |
| unlimited | 0.75 |

### Negative-direction terms (push score toward −1 / "Weak") — 15 terms

| Term | Weight |
|---|---|
| averse | 1 |
| complacent | 1 |
| conservative | 1 |
| no change | 1 |
| not invented here | 1 |
| old way | 1 |
| play it safe | 1 |
| resist change | 1 |
| risk-averse | 1 |
| risk-free | 1 |
| stagnant | 1 |
| status quo | 1 |
| stuck | 1 |
| fearful | 0.75 |
| tradition | 0.75 |

## C.11 `d11` — Agility & Adaptability (attribute; low = Weak, high = Strong)


*How quickly and easily the organisation adapts to change. A higher score means it responds faster to new situations.*


### Positive-direction terms (push score toward +1 / "Strong") — 87 terms

| Term | Weight |
|---|---|
| absorb shock | 1 |
| act fast | 1 |
| adaptable | 1 |
| adjust | 1 |
| agile | 1 |
| ambidextrous | 1 |
| anti-fragile | 1 |
| bounce back | 1 |
| change | 1 |
| change-ready | 1 |
| continuous feedback | 1 |
| course correct | 1 |
| customer signal | 1 |
| decisive | 1 |
| digital agility | 1 |
| disruption-ready | 1 |
| dynamic | 1 |
| evolve | 1 |
| fast | 1 |
| fast decision | 1 |
| feedback loop | 1 |
| flexible | 1 |
| fluid | 1 |
| improvise | 1 |
| iterate | 1 |
| lean | 1 |
| minimum viable | 1 |
| move fast | 1 |
| mvp | 1 |
| nimble | 1 |
| on the fly | 1 |
| pivot | 1 |
| quick | 1 |
| rapid | 1 |
| re-prioritise | 1 |
| react | 1 |
| real-time | 1 |
| recalibrate | 1 |
| refocused | 1 |
| reorient | 1 |
| repositioned | 1 |
| resilience | 1 |
| reskilled | 1 |
| responsive | 1 |
| responsive culture | 1 |
| restructure | 1 |
| scrappy | 1 |
| scrum | 1 |
| shift | 1 |
| sprint | 1 |
| transform | 1 |
| transformation | 1 |
| upskilled | 1 |
| velocity | 1 |
| versatile | 1 |
| dual-track | 0.75 |
| experiment | 0.75 |
| kanban | 0.75 |
| market-driven | 0.75 |
| modular | 0.75 |
| opportunistic | 0.75 |
| reconfigured | 0.75 |
| recover | 0.75 |
| redeploy | 0.75 |
| redeployed | 0.75 |
| redirected | 0.75 |
| reorganise | 0.75 |
| retooled | 0.75 |
| retrained | 0.75 |
| scalable | 0.75 |
| seize | 0.75 |
| switchable | 0.75 |
| test | 0.75 |
| unconstrained | 0.75 |
| unencumbered | 0.75 |
| update | 0.75 |
| upgrade | 0.75 |
| withstand | 0.75 |
| freewheeling | 0.25 |
| instinctive | 0.25 |
| intuitive | 0.25 |
| lightweight | 0.25 |
| mobile | 0.25 |
| porous | 0.25 |
| portable | 0.25 |
| rechanneled | 0.25 |
| spontaneous | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 15 terms

| Term | Weight |
|---|---|
| bloated | 1 |
| bogged down | 1 |
| bureaucratic | 1 |
| change-averse | 1 |
| dinosaur | 1 |
| inflexible | 1 |
| outdated | 1 |
| resistant | 1 |
| rigid | 1 |
| slow to change | 1 |
| status quo | 1 |
| stuck in ways | 1 |
| too slow | 1 |
| unable to adapt | 1 |
| legacy | 0.75 |

## C.12 `d12` — Continuous Learning & Development (attribute; low = Weak, high = Strong)


*How much the organisation invests in helping its people learn and grow. A higher score means more emphasis on development.*


### Positive-direction terms (push score toward +1 / "Strong") — 93 terms

| Term | Weight |
|---|---|
| 360 feedback | 1 |
| adapt knowledge | 1 |
| after action review | 1 |
| beginner's mind | 1 |
| best practice | 1 |
| career development | 1 |
| challenging work | 1 |
| coaching | 1 |
| communities of practice | 1 |
| continuous | 1 |
| continuous education | 1 |
| courses | 1 |
| cross-train | 1 |
| curiosity | 1 |
| data-driven learning | 1 |
| develop talent | 1 |
| development | 1 |
| education | 1 |
| enriching | 1 |
| evolve thinking | 1 |
| fail and learn | 1 |
| feedback | 1 |
| grow talent | 1 |
| growth | 1 |
| growth mindset | 1 |
| humble | 1 |
| improvement | 1 |
| inquisitive | 1 |
| invest in learning | 1 |
| invest in yourself | 1 |
| knowledge | 1 |
| knowledge sharing | 1 |
| l&d | 1 |
| learn from mistakes | 1 |
| learning | 1 |
| learning culture | 1 |
| lessons learned | 1 |
| lifelong learning | 1 |
| mentorship | 1 |
| micro-learning | 1 |
| multi-skilled | 1 |
| on-the-job | 1 |
| open to learning | 1 |
| opportunity to grow | 1 |
| peer learning | 1 |
| personal development | 1 |
| post-mortem | 1 |
| professional development | 1 |
| promote from within | 1 |
| reflect | 1 |
| reflection | 1 |
| reskill | 1 |
| retrospective | 1 |
| rotation | 1 |
| sabbatical | 1 |
| secondment | 1 |
| self-aware | 1 |
| self-improvement | 1 |
| shadowing | 1 |
| stretch assignment | 1 |
| test hypothesis | 1 |
| training | 1 |
| update views | 1 |
| upskill | 1 |
| workshop | 1 |
| absorb | 0.75 |
| academic | 0.75 |
| adaptable | 0.75 |
| appraisal | 0.75 |
| bite-sized | 0.75 |
| buddy | 0.75 |
| case study | 0.75 |
| certificate | 0.75 |
| conference | 0.75 |
| e-learning | 0.75 |
| experiment | 0.75 |
| goal-setting | 0.75 |
| intellectual | 0.75 |
| library | 0.75 |
| online course | 0.75 |
| performance review | 0.75 |
| qualification | 0.75 |
| reading | 0.75 |
| research | 0.75 |
| seminar | 0.75 |
| stimulating | 0.75 |
| study | 0.75 |
| versatile | 0.75 |
| book club | 0.25 |
| diploma | 0.25 |
| podcast | 0.25 |
| smart | 0.25 |
| talented | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 10 terms

| Term | Weight |
|---|---|
| complacent | 1 |
| fixed mindset | 1 |
| know-it-all | 1 |
| no development | 1 |
| no learning | 1 |
| no training | 1 |
| resistant to feedback | 1 |
| stagnant | 1 |
| unwilling to learn | 1 |
| arrogant | 0.75 |

## C.13 `d13` — Customer / Stakeholder Focus (attribute; low = Weak, high = Strong)


*How strongly the organisation focuses on the needs of customers and stakeholders. A higher score means a more customer-centric culture.*


### Positive-direction terms (push score toward +1 / "Strong") — 88 terms

| Term | Weight |
|---|---|
| advocate | 1 |
| anticipate needs | 1 |
| client | 1 |
| co-create | 1 |
| co-design | 1 |
| community engagement | 1 |
| customer | 1 |
| customer first | 1 |
| customer insight | 1 |
| customer obsession | 1 |
| customer problem | 1 |
| customer value | 1 |
| customer voice | 1 |
| customer-centric | 1 |
| delight | 1 |
| empathy | 1 |
| exceed expectations | 1 |
| experience | 1 |
| external focus | 1 |
| feedback | 1 |
| frictionless | 1 |
| go above and beyond | 1 |
| journey | 1 |
| listening | 1 |
| long-term relationship | 1 |
| loyalty | 1 |
| market research | 1 |
| needs | 1 |
| nps | 1 |
| obsess | 1 |
| outside-in | 1 |
| proactive | 1 |
| public good | 1 |
| public interest | 1 |
| recommend | 1 |
| referral | 1 |
| relationship | 1 |
| reputation | 1 |
| resolve | 1 |
| respond | 1 |
| responsive | 1 |
| retention | 1 |
| satisfaction | 1 |
| serve | 1 |
| serve the customer | 1 |
| service | 1 |
| social impact | 1 |
| solve | 1 |
| solve for customer | 1 |
| stakeholder | 1 |
| touchpoint | 1 |
| user | 1 |
| user research | 1 |
| user testing | 1 |
| value for customer | 1 |
| voice of customer | 1 |
| accessible | 0.75 |
| ambassador | 0.75 |
| assist | 0.75 |
| brand | 0.75 |
| civic | 0.75 |
| co-innovate | 0.75 |
| community | 0.75 |
| complaint | 0.75 |
| dedication | 0.75 |
| ecosystem | 0.75 |
| effortless | 0.75 |
| esg | 0.75 |
| external | 0.75 |
| focus group | 0.75 |
| help | 0.75 |
| investor | 0.75 |
| outward | 0.75 |
| partner | 0.75 |
| persona | 0.75 |
| regulator | 0.75 |
| review | 0.75 |
| seamless | 0.75 |
| shareholder | 0.75 |
| society | 0.75 |
| survey | 0.75 |
| trust | 0.75 |
| available | 0.25 |
| government | 0.25 |
| ngo | 0.25 |
| partner network | 0.25 |
| supply chain | 0.25 |
| vendor | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 10 terms

| Term | Weight |
|---|---|
| attrition | 1 |
| churn | 1 |
| customer complaints | 1 |
| dissatisfaction | 1 |
| ignore customer | 1 |
| internal focus | 1 |
| inward looking | 1 |
| navel-gazing | 1 |
| poor service | 1 |
| unresponsive | 1 |

## C.14 `d14` — Competitive Assertiveness (attribute; low = Weak, high = Strong)


*How competitive and assertive the organisation is in pursuing its goals and winning. A higher score means a more driven, ambitious culture.*


### Positive-direction terms (push score toward +1 / "Strong") — 87 terms

| Term | Weight |
|---|---|
| adversarial | 1 |
| aggressive | 1 |
| alpha | 1 |
| ambitious | 1 |
| assertive | 1 |
| backstab | 1 |
| battle | 1 |
| beat | 1 |
| champion | 1 |
| compete | 1 |
| competitive | 1 |
| confrontational | 1 |
| conquer | 1 |
| crush | 1 |
| cut-throat | 1 |
| cutthroat | 1 |
| destroy | 1 |
| dominant | 1 |
| dominate | 1 |
| enemy | 1 |
| fierce | 1 |
| fight | 1 |
| first place | 1 |
| hard-charging | 1 |
| hard-driving | 1 |
| hunger | 1 |
| hustle | 1 |
| internal competition | 1 |
| kill it | 1 |
| league table | 1 |
| market leader | 1 |
| market share | 1 |
| never give up | 1 |
| number one | 1 |
| one-upmanship | 1 |
| opponent | 1 |
| outclass | 1 |
| outmanoeuvre | 1 |
| outperform | 1 |
| powerhouse | 1 |
| predatory | 1 |
| ranking | 1 |
| relentless | 1 |
| relentless pursuit | 1 |
| rivalry | 1 |
| ruthless | 1 |
| sabotage | 1 |
| self-promote | 1 |
| step over | 1 |
| superiority | 1 |
| supremacy | 1 |
| territorial | 1 |
| toxic competition | 1 |
| turf war | 1 |
| undermine | 1 |
| warfare | 1 |
| win | 1 |
| winner takes all | 1 |
| zero-sum | 1 |
| acquisition | 0.75 |
| boastful | 0.75 |
| bold | 0.75 |
| bragging | 0.75 |
| clash | 0.75 |
| driven | 0.75 |
| elbow | 0.75 |
| force | 0.75 |
| in-your-face | 0.75 |
| invincible | 0.75 |
| leverage | 0.75 |
| muscle | 0.75 |
| opportunist | 0.75 |
| political | 0.75 |
| possessive | 0.75 |
| power | 0.75 |
| pressure | 0.75 |
| rank | 0.75 |
| show off | 0.75 |
| strength | 0.75 |
| takeover | 0.75 |
| threat | 0.75 |
| trophy | 0.75 |
| turf | 0.75 |
| unbeatable | 0.75 |
| upstage | 0.75 |
| award | 0.25 |
| loud | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 15 terms

| Term | Weight |
|---|---|
| accommodating | 1 |
| avoid conflict | 1 |
| collaborative | 1 |
| conflict-averse | 1 |
| consensus-seeking | 1 |
| cooperative | 1 |
| harmony | 1 |
| non-confrontational | 1 |
| passive | 1 |
| play nice | 1 |
| seek consensus | 1 |
| submissive | 1 |
| deferential | 0.75 |
| humble | 0.75 |
| risk-averse | 0.75 |

## C.15 `d15` — Diversity, Equity & Inclusion (attribute; low = Weak, high = Strong)


*How much the organisation values and supports a diverse and inclusive workforce. A higher score means a stronger commitment to inclusion.*


### Positive-direction terms (push score toward +1 / "Strong") — 82 terms

| Term | Weight |
|---|---|
| accessibility | 1 |
| advocacy | 1 |
| affinity group | 1 |
| age diversity | 1 |
| all are welcome | 1 |
| allyship | 1 |
| bame | 1 |
| barrier-free | 1 |
| belonging | 1 |
| blind recruitment | 1 |
| celebrate difference | 1 |
| champion diversity | 1 |
| cognitive diversity | 1 |
| dei strategy | 1 |
| different thinking | 1 |
| disability | 1 |
| diverse hiring | 1 |
| diverse perspectives | 1 |
| diverse team | 1 |
| diversity | 1 |
| diversity target | 1 |
| embrace difference | 1 |
| employee resource group | 1 |
| equal | 1 |
| equal chance | 1 |
| equal footing | 1 |
| equal opportunity | 1 |
| equal voice | 1 |
| equitable access | 1 |
| equity | 1 |
| erg | 1 |
| ethnicity | 1 |
| everyone welcome | 1 |
| fair hiring | 1 |
| fairness | 1 |
| female leadership | 1 |
| gender | 1 |
| gender balance | 1 |
| heterogeneous | 1 |
| inclusion | 1 |
| inclusive | 1 |
| inclusive leader | 1 |
| inclusive team | 1 |
| intersectionality | 1 |
| level playing field | 1 |
| lgbtq | 1 |
| mentor | 1 |
| multicultural | 1 |
| neurodiversity | 1 |
| no barriers | 1 |
| people of colour | 1 |
| place for everyone | 1 |
| race | 1 |
| remove obstacles | 1 |
| representation | 1 |
| respect difference | 1 |
| safe space | 1 |
| sponsor | 1 |
| unconscious bias | 1 |
| varied backgrounds | 1 |
| welcoming | 1 |
| women in leadership | 1 |
| cultural celebration | 0.75 |
| gender neutral | 0.75 |
| generational | 0.75 |
| global | 0.75 |
| international | 0.75 |
| levelling | 0.75 |
| lift up | 0.75 |
| meritocracy | 0.75 |
| minority | 0.75 |
| nationality | 0.75 |
| openness | 0.75 |
| perspective | 0.75 |
| privilege | 0.75 |
| pronouns | 0.75 |
| socioeconomic | 0.75 |
| warm | 0.75 |
| culture | 0.25 |
| faith | 0.25 |
| prayer room | 0.25 |
| religion | 0.25 |

### Negative-direction terms (push score toward −1 / "Weak") — 20 terms

| Term | Weight |
|---|---|
| bias | 1 |
| boys club | 1 |
| discrimination | 1 |
| exclusion | 1 |
| gender pay gap | 1 |
| glass ceiling | 1 |
| harassment | 1 |
| homogeneous | 1 |
| marginalised | 1 |
| overlooked | 1 |
| pay gap | 1 |
| racism | 1 |
| sexism | 1 |
| sidelining | 1 |
| structural inequality | 1 |
| systemic | 1 |
| tokenism | 1 |
| underrepresented | 1 |
| wage gap | 1 |
| whitewash | 1 |

## C.16 `d16` — Profession-Focused vs Company-Focused (bipolar; low = Company-Focused, high = Profession-Focused)


*Whether people identify mainly with their profession and craft (higher score) or with the company they work for (lower score).*


### Positive-direction terms (push score toward +1 / "Profession-Focused") — 94 terms

| Term | Weight |
|---|---|
| accredited | 1 |
| accurate | 1 |
| autonomous thinking | 1 |
| best practice | 1 |
| beyond company | 1 |
| body of knowledge | 1 |
| broader market | 1 |
| certified | 1 |
| chartered | 1 |
| code of ethics | 1 |
| continuous professional education | 1 |
| cpd | 1 |
| cpe | 1 |
| craft | 1 |
| credentials | 1 |
| cross-sector | 1 |
| deep expertise | 1 |
| discipline | 1 |
| domain knowledge | 1 |
| employable | 1 |
| evidence-based | 1 |
| expert | 1 |
| expertise | 1 |
| external benchmark | 1 |
| external presentation | 1 |
| external recognition | 1 |
| global standard | 1 |
| impartial | 1 |
| independent | 1 |
| independent judgement | 1 |
| industry norm | 1 |
| industry-wide | 1 |
| international best practice | 1 |
| market value | 1 |
| mastery | 1 |
| neutrality | 1 |
| objectivity | 1 |
| own judgement | 1 |
| own standards | 1 |
| peer community | 1 |
| peer review | 1 |
| peer reviewed | 1 |
| personal ethics | 1 |
| portable skills | 1 |
| practitioner | 1 |
| profession | 1 |
| professional | 1 |
| professional conduct | 1 |
| professional development | 1 |
| professionally led | 1 |
| proficiency | 1 |
| publication | 1 |
| qualification | 1 |
| reliable | 1 |
| research-led | 1 |
| rigorous | 1 |
| robust | 1 |
| sector standard | 1 |
| self-regulated | 1 |
| sme | 1 |
| society membership | 1 |
| specialism | 1 |
| standards | 1 |
| subject matter | 1 |
| technical | 1 |
| thought leadership | 1 |
| transferable | 1 |
| accountant | 0.75 |
| analyst | 0.75 |
| analytical | 0.75 |
| architect | 0.75 |
| association | 0.75 |
| challenge | 0.75 |
| conference | 0.75 |
| consistent | 0.75 |
| consultant | 0.75 |
| critique | 0.75 |
| data-driven | 0.75 |
| doctor | 0.75 |
| engineer | 0.75 |
| guild | 0.75 |
| institute | 0.75 |
| lawyer | 0.75 |
| licensed | 0.75 |
| meticulous | 0.75 |
| networking | 0.75 |
| precise | 0.75 |
| question | 0.75 |
| regulated | 0.75 |
| scientist | 0.75 |
| validate | 0.75 |
| verify | 0.75 |
| deductive | 0.25 |
| inductive | 0.25 |

### Negative-direction terms (push score toward −1 / "Company-Focused") — 11 terms

| Term | Weight |
|---|---|
| clannish | 1 |
| company loyalty | 1 |
| company way | 1 |
| insular | 1 |
| internal focus | 1 |
| inward | 1 |
| not invented here | 1 |
| our way | 1 |
| parochial | 1 |
| proprietary only | 1 |
| tribal | 1 |

## C.17 `d17` — Internally Driven vs Externally Driven (bipolar; low = Externally Driven, high = Internally Driven)


*Whether the organisation follows its own internal standards and beliefs (higher score) or is guided mainly by outside customer and market expectations (lower score).*


### Positive-direction terms (push score toward +1 / "Internally Driven") — 69 terms

| Term | Weight |
|---|---|
| ahead of curve | 1 |
| ahead of the pack | 1 |
| assertive position | 1 |
| back ourselves | 1 |
| benchmark setter | 1 |
| bold bet | 1 |
| built on belief | 1 |
| category creator | 1 |
| clear conviction | 1 |
| committed to view | 1 |
| confident stance | 1 |
| conviction | 1 |
| create demand | 1 |
| create the future | 1 |
| credible | 1 |
| define the market | 1 |
| firm belief | 1 |
| first to market | 1 |
| independent stance | 1 |
| independent voice | 1 |
| independent-minded | 1 |
| inside-out | 1 |
| internally driven | 1 |
| know what's right | 1 |
| lead the way | 1 |
| mission-led | 1 |
| opinionated | 1 |
| own agenda | 1 |
| own compass | 1 |
| own direction | 1 |
| own north star | 1 |
| own research | 1 |
| own thesis | 1 |
| own view | 1 |
| own way | 1 |
| path-breaker | 1 |
| pioneering | 1 |
| principled | 1 |
| purpose-led | 1 |
| resolutely | 1 |
| self-determined | 1 |
| self-reliant | 1 |
| self-sufficient | 1 |
| set agenda | 1 |
| set the pace | 1 |
| shape market | 1 |
| standard bearer | 1 |
| steadfast | 1 |
| strong view | 1 |
| thought leader | 1 |
| true to self | 1 |
| trust gut | 1 |
| unwavering | 1 |
| values-led | 1 |
| visionary | 1 |
| assertive | 0.75 |
| authentic | 0.75 |
| consistent | 0.75 |
| contrarian | 0.75 |
| counter-intuitive | 0.75 |
| dependable | 0.75 |
| genuine | 0.75 |
| iconoclast | 0.75 |
| lighthouse | 0.75 |
| maverick | 0.75 |
| north star | 0.75 |
| reliable | 0.75 |
| self-confident | 0.75 |
| trustworthy | 0.75 |

### Negative-direction terms (push score toward −1 / "Externally Driven") — 35 terms

| Term | Weight |
|---|---|
| client-led | 1 |
| consensus-seeking | 1 |
| copycat | 1 |
| customer-driven | 1 |
| customer-led strategy | 1 |
| demand-driven | 1 |
| external pressure | 1 |
| fast follower | 1 |
| focus group dependent | 1 |
| follow industry | 1 |
| follow the customer | 1 |
| follower | 1 |
| imitator | 1 |
| market research dependent | 1 |
| market signal | 1 |
| market-led | 1 |
| me-too | 1 |
| nps obsessed | 1 |
| outside-in | 1 |
| partner-led | 1 |
| reactive | 1 |
| replicate competitor | 1 |
| survey-driven | 1 |
| trend-following | 1 |
| wait and see | 1 |
| act on feedback | 0.75 |
| benchmark-driven | 0.75 |
| data-dependent | 0.75 |
| investor-driven | 0.75 |
| peer comparison | 0.75 |
| regulatory-driven | 0.75 |
| replicate | 0.75 |
| responsive | 0.75 |
| stakeholder demand | 0.75 |
| test with customers | 0.75 |

## C.18 `d18` — Strong vs Loose Social Norms (bipolar; low = Loose Social Norms, high = Strong Social Norms)


*Whether the workplace has strong, clearly enforced rules of behaviour (higher score) or a more relaxed, flexible atmosphere (lower score).*


### Positive-direction terms (push score toward +1 / "Strong Social Norms") — 78 terms

| Term | Weight |
|---|---|
| accountability to norms | 1 |
| agenda required | 1 |
| appearance matters | 1 |
| belong or leave | 1 |
| boundaries | 1 |
| brand standards | 1 |
| business dress | 1 |
| communication standards | 1 |
| conduct standards | 1 |
| conform or else | 1 |
| conformity | 1 |
| corporate image | 1 |
| cultural norms | 1 |
| cultural police | 1 |
| decorum | 1 |
| deference | 1 |
| deviance punished | 1 |
| disciplinary | 1 |
| dress code | 1 |
| etiquette | 1 |
| expectations clear | 1 |
| expected behaviour | 1 |
| expected to conform | 1 |
| fit in | 1 |
| formal | 1 |
| formal communication | 1 |
| formal environment | 1 |
| formal review | 1 |
| formality | 1 |
| go through proper channels | 1 |
| judged | 1 |
| lateness punished | 1 |
| meeting etiquette | 1 |
| norm compliance | 1 |
| norm enforcement | 1 |
| norm violation | 1 |
| norms enforced | 1 |
| official channels | 1 |
| on time | 1 |
| ostracised | 1 |
| outcasted | 1 |
| peer pressure | 1 |
| penalised | 1 |
| policed | 1 |
| pressure to conform | 1 |
| professional conduct | 1 |
| protocol | 1 |
| punctuality | 1 |
| punished for deviation | 1 |
| reprimanded | 1 |
| respectful address | 1 |
| sanctioned | 1 |
| scrutinised | 1 |
| smart attire | 1 |
| social norms | 1 |
| social pressure | 1 |
| strict | 1 |
| strict dress | 1 |
| structured meetings | 1 |
| tight | 1 |
| title used | 1 |
| uniform | 1 |
| warned | 1 |
| watched | 1 |
| written records | 1 |
| ceremony | 0.75 |
| documented | 0.75 |
| dress to impress | 0.75 |
| hr process | 0.75 |
| last name | 0.75 |
| minutes taken | 0.75 |
| monitored | 0.75 |
| observed | 0.75 |
| ritual | 0.75 |
| suit | 0.75 |
| tie | 0.75 |
| tradition | 0.75 |
| sir | 0.25 |

### Negative-direction terms (push score toward −1 / "Loose Social Norms") — 25 terms

| Term | Weight |
|---|---|
| anything goes | 1 |
| casual | 1 |
| chaotic | 1 |
| flexible norms | 1 |
| free spirit | 1 |
| free-for-all | 1 |
| freestyle | 1 |
| hands off | 1 |
| informal | 1 |
| laissez-faire | 1 |
| loose | 1 |
| no rules | 1 |
| permissive | 1 |
| relaxed | 1 |
| tolerant | 1 |
| eccentric | 0.75 |
| non-conformist | 0.75 |
| quirky | 0.75 |
| unorthodox | 0.75 |
| barefoot | 0.25 |
| beer fridge | 0.25 |
| game room | 0.25 |
| jeans | 0.25 |
| playful | 0.25 |
| tattoos | 0.25 |

*Total Schroders terms: 1824*
