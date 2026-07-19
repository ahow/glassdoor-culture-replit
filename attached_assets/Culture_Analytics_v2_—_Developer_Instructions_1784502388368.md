# Culture Analytics — Developer Instructions for v2 Methodology

**Context.** Independent methodology review of the current Schroders 18-dimension framework has identified a severe redundancy problem: on the 830 companies with 500+ reviews (i.e. those with the most reliable scores), a single principal component explains 94.8% of the variance across all 18 dimensions, and multiple dimension pairs correlate at r ≈ 1.00. This means the framework as currently scored is effectively unidimensional — it measures general review positivity, not 18 distinct cultural attributes. The R² claims in the current client report rest on this framework and are not defensible in their current form.

This document sets out the changes required to rebuild the framework so it produces genuinely independent, investment-relevant signals that can be validated out-of-sample.

**Reading order.** Work through the phases in sequence. Phases 1–2 are prerequisites for Phase 3 (bipolar dimension design). Do not skip ahead — designing new dimensions before fixing the scoring engine will reproduce the current problems on a new set of labels.

**Success criterion for v2.** After the rebuild, on companies with ≥100 scored reviews:
- No pairwise dimension correlation should exceed |r| = 0.7
- PC1 should explain no more than 40% of variance
- At least 8 dimensions should have VIF < 5
- At least 3 dimensions should have leave-one-out cross-validated |r| > 0.05 with financial performance at sub-industry level, significant at p<0.05 after Benjamini-Hochberg FDR correction

If we cannot meet these thresholds after the rebuild, the framework should be reported as a smaller number of factors rather than 18 dimensions.

---

## Phase 1 — Fix the scoring engine (prerequisite work)

**Goal.** Ensure that when reviews contain language relevant to a dimension, the scorer captures it correctly and distinguishes positive from negative usage.

### 1.1 Word-boundary matching

**Current problem.** `culture_scoring.py:3788` uses `phrase in text_lower`. This means "ethical" matches inside "unethical", "not helpful" is not distinguished from "helpful", "no work-life balance" scores identically to "great work-life balance". Substring matching cannot support bipolar scoring — a bipolar rebuild without this fix will produce the same collisions.

**Required change.** Replace substring matching with regex word-boundary matching:

```python
import re

def _compile_phrase(phrase: str) -> re.Pattern:
    """Compile a keyword phrase to a word-boundary regex.
    Multi-word phrases match with flexible whitespace between tokens."""
    tokens = phrase.lower().split()
    # \b at start and end; \s+ between tokens (handles multi-space, tabs, newlines)
    pattern = r'\b' + r'\s+'.join(re.escape(t) for t in tokens) + r'\b'
    return re.compile(pattern, re.IGNORECASE)

# Pre-compile all dictionary phrases at module import (do this once, not per review)
COMPILED_KEYWORDS = {
    dim: {
        pole: {phrase: (_compile_phrase(phrase), weight) for phrase, weight in terms.items()}
        for pole, terms in poles.items()
    }
    for dim, poles in SCHRODERS_KEYWORDS.items()
}

def score_review_with_dictionary(review_text: str) -> dict:
    text_lower = review_text.lower()
    scores = {}
    for dim, poles in COMPILED_KEYWORDS.items():
        pos, neg = 0.0, 0.0
        for phrase, (pattern, weight) in poles['positive'].items():
            if pattern.search(text_lower):
                pos += weight
        for phrase, (pattern, weight) in poles['negative'].items():
            if pattern.search(text_lower):
                neg += weight
        if pos + neg > 0:
            scores[dim] = (pos - neg) / (pos + neg)
            scores[f'{dim}_evidence'] = pos + neg
        else:
            scores[dim] = None
            scores[f'{dim}_evidence'] = 0
    return scores
```

**Validation.** Before proceeding, run a diff test: for 500 random reviews, compare scores before and after the change on 3 sample dimensions. Expect meaningful score changes for reviews containing negated forms (e.g. "not innovative", "unethical", "no career development"). If fewer than 5% of reviews show a score change, the regex isn't firing correctly.

### 1.2 Negation-scope handling

**Current problem.** Even with word boundaries, "employees are not treated well" contains "treated well" which will fire a positive-pole term. English negation typically operates within a 3–5 word window; the scorer needs to detect this.

**Required change.** Implement a two-pass approach:

```python
NEGATION_MARKERS = {
    'no', 'not', 'never', 'nothing', 'none', 'nobody', 'nowhere',
    'lacks', 'lacking', 'lack', 'without', 'absence',
    'poor', 'weak', 'insufficient', 'inadequate', 'minimal',
    "n't",  # captures "isn't", "don't", "doesn't", "won't" via substring
    'hardly', 'barely', 'rarely', 'seldom',
}

def _find_matches_with_negation(text: str, pattern: re.Pattern, window: int = 5) -> tuple[int, int]:
    """Return (positive_matches, negation_matches) for a keyword pattern.
    A match is 'negated' if a negation marker appears within `window` tokens before it."""
    tokens = text.split()
    positive_count = 0
    negated_count = 0
    for m in pattern.finditer(text):
        # Which token does this match start in?
        char_pos = m.start()
        token_idx = len(text[:char_pos].split())
        window_start = max(0, token_idx - window)
        preceding = tokens[window_start:token_idx]
        # Check for negation markers, but not "no" if followed by punctuation (e.g. "No. The company...")
        has_negation = any(
            tok.lower().rstrip('.,;:!?') in NEGATION_MARKERS
            or "n't" in tok.lower()
            for tok in preceding
        )
        if has_negation:
            negated_count += 1
        else:
            positive_count += 1
    return positive_count, negated_count
```

Then in `score_review_with_dictionary`, use these counts:

```python
# For each positive-pole term:
pos_matches, negated_pos = _find_matches_with_negation(text_lower, pattern, window=5)
pos += weight * pos_matches
neg += weight * negated_pos  # negated positive = evidence for negative pole

# For each negative-pole term:
neg_matches, negated_neg = _find_matches_with_negation(text_lower, pattern, window=5)
neg += weight * neg_matches
pos += weight * negated_neg  # negated negative = evidence for positive pole (rare but real)
```

**Validation.** Hand-label 200 reviews on 3 dimensions (positive / negative / not mentioned). The engine should agree with human labels on ≥75% of reviews. If it's below 60%, the negation window or marker list needs iteration.

### 1.3 Persist dictionary version alongside every scored review

**Current problem.** The `review_culture_scores` table stores scores without recording which dictionary version produced them. If dictionaries evolve, we can't tell which rows are stale.

**Required change.** Add a `dictionary_version` column (VARCHAR, e.g. '2026-08-01-v2') to `review_culture_scores`. Populate it on every insert. Add a `scoring_engine_version` column (e.g. 'v2.0-regex-negation') that tracks the code version. When either changes, mark affected companies as needing re-scoring.

### 1.4 Add unit tests

Currently the repo has zero tests. Before making methodology changes, freeze the following as regression tests (in `test_culture_scoring.py`):

```python
def test_negation_flips_polarity():
    # Positive-pole phrase in isolation
    assert score_review_with_dictionary("great work-life balance")['d06'] > 0
    # Same phrase after negation
    assert score_review_with_dictionary("no work-life balance")['d06'] < 0
    assert score_review_with_dictionary("not a great work-life balance")['d06'] < 0

def test_word_boundary_no_substring_collision():
    # "ethical" must not match inside "unethical"
    result = score_review_with_dictionary("this is unethical behaviour")
    assert result['d09'] < 0  # integrity should be negative
    # Confirm ethical alone still fires positive
    result = score_review_with_dictionary("very ethical culture")
    assert result['d09'] > 0

def test_evidence_null_when_no_mentions():
    result = score_review_with_dictionary("the office has good coffee")
    assert result['d02'] is None  # long-term orientation not mentioned

def test_multi_dimension_scoring():
    text = "innovative culture but poor work-life balance"
    result = score_review_with_dictionary(text)
    assert result['d10'] > 0  # innovation positive
    assert result['d06'] < 0  # caring negative (poor + work-life balance)
```

Add tests for at least 5 dimensions before proceeding. These lock in correct behaviour so future dictionary changes don't silently break things.

---

## Phase 2 — Rebuild dictionaries symmetrically

**Goal.** Bring the negative-pole dictionaries up to parity with the positive-pole dictionaries, so both poles fire at comparable rates in a corpus of ordinary reviews. Currently negative dictionaries average 18 terms per dimension vs 83 for positive.

### 2.1 Expand negative-pole dictionaries to ~60+ terms each

**Method — three parallel sources:**

**Source A: Systematic antonym generation.** For each positive-pole term, algorithmically generate:
- Direct antonyms (from WordNet or an antonym API): "ethical" → "unethical", "corrupt", "dishonest"
- Negated forms: "innovative" → "not innovative", "uninnovative", "lacks innovation"
- Absence markers: "innovation" → "no innovation", "little innovation", "innovation lacking"

Discount weight: 0.7× the positive-pole term's weight (accounts for lower confidence that antonym truly captures the opposite pole).

**Source B: Corpus mining.** Take a stratified sample of 100,000 reviews. For each dimension, find sentences that contain (positive-pole term + negation marker within 5 words) and extract the noun/adjective phrases. Rank by frequency. Take the top 50 for each dimension into a candidate list.

**Source C: Expert review.** For each dimension, one domain expert (this can be Andy for the investment-relevance dimensions or another Schroders analyst) reviews the top 100 candidates from A+B combined and marks keep / drop / adjust-weight. This is where domain knowledge earns its keep.

**Target after expansion:** each dimension has ~60–100 negative-pole terms, weight-matched to the positive pole. Store all negative-pole dictionaries in `schroders_keywords.py` with the same structure.

### 2.2 Cross-loading penalty applied symmetrically

Currently the `effective_weight = base / (1 + 0.5 × cross_load_count)` formula (METHODOLOGY.md Section 4) is applied to positive-pole terms only. Apply it to negative-pole terms too, and count cross-loading across *all poles* (positive and negative) of *all dimensions*. This will down-weight terms that appear in many places.

### 2.3 Validation against hand labels

Before rolling out, hand-label a random 500 reviews on 5 selected dimensions (a mix of the currently-saturated ones and the currently-independent ones — e.g., d06 Caring, d13 Customer, d02 Long-term, d17 Internal, d09 Integrity). For each review, an analyst marks: strong-positive / mild-positive / mixed / mild-negative / strong-negative / not mentioned.

Score correlation (Pearson) between engine output and human labels on the hand-labelled set. **Threshold to proceed: |r| > 0.5 on every tested dimension.** If any dimension fails this, the dictionary needs another iteration before use.

---

## Phase 3 — Redesign the dimension set

**Goal.** Replace the current 18 dimensions with a smaller number of genuinely independent bipolar constructs, chosen for investment relevance and validated statistically.

### 3.1 Pre-specify the candidate dimensions (before any data analysis)

The current 18 mix "attribute" and "bipolar" formulations without consistent design principles. Redesign around these criteria:

1. **Each dimension must be genuinely bipolar** — both poles are legitimate cultural states, not just presence/absence of a desirable trait
2. **Each dimension must have direct investment relevance** — an analyst should be able to articulate why one pole predicts better business outcomes in specific contexts, or why the trade-off between poles matters
3. **Dimensions must be pairwise near-orthogonal by construction** — the semantic definitions should not overlap

**Proposed starting set — 12 candidate bipoles.** These are Andy's proposal informed by Schroders' investment framework and the current dimensions that survived the redundancy analysis. Some of the pole framings come from established organisational research (Hofstede, Denison, Cameron & Quinn). The developer should not add dimensions to this list without discussion; the whole point is to test which of these 12 survive validation, not to inflate the count again.

| ID | Pole A (score → -1) | Pole B (score → +1) | Investment thesis |
|----|---------------------|---------------------|-------------------|
| b01 | Short-term / quarterly | Long-term / patient | Long-term orientation predicts capex quality and R&D productivity |
| b02 | Cost-focused / thrifty | Growth-focused / investing | Both are legitimate; investment strategy depends on which is expected. Sig for value vs growth thesis alignment |
| b03 | Hierarchical / top-down | Egalitarian / distributed | Egalitarian predicts faster decision-making and better retention in knowledge industries |
| b04 | Rules-driven / process | Judgement-driven / adaptive | Rules-driven is right for regulated / high-stakes; judgement-driven for volatile / creative |
| b05 | Individual performance | Team performance | Individual outperformance for sales/trading; team outperformance for engineering/product |
| b06 | Insular / internally-driven | Externally-focused / market-driven | Externally-focused predicts better response to disruption |
| b07 | Risk-averse | Risk-taking | Risk-taking correlates with growth optionality; risk-aversion with drawdown protection |
| b08 | Political / tenure-based | Meritocratic / performance-based | Meritocracy correlates with talent retention and productivity |
| b09 | Toxic / high-turnover | Supportive / low-turnover | Directly predicts retention costs and hiring quality |
| b10 | Chaotic / strategy churn | Stable / consistent | Stable strategy correlates with execution quality |
| b11 | Compliance-minimising | Integrity-maximising | Integrity-max predicts fewer regulatory/reputational tail events |
| b12 | Homogeneous | Diverse & inclusive | Where empirical evidence supports it, DEI correlates with decision quality |

**Note on dimensions dropped from the current 18:**
- d01 Purpose: bundled into b02 (growth-focused implies purpose) and b06 (external focus implies serving external purpose)
- d05 Psychological Safety: partly in b09 (toxic vs supportive) and b03 (egalitarian gives voice)
- d10 Innovation & Risk: split into b02 (investing implies innovation appetite) and b07 (risk-taking is separately measured)
- d11 Agility: subsumed into b04 (judgement-driven implies faster adaptation)
- d12 Learning: subsumed into b09 (supportive employers invest in learning) — kept as a facet, not a dimension
- d14 Competitive Assertiveness: dropped as it conflates internal drive with external posture and neither pole is clearly better
- d15 DEI: kept as b12 but with narrower scope — inclusion specifically, since it has stronger empirical support
- d16 Profession-focus: dropped as investment relevance is unclear and it correlated heavily with the general factor
- d18 Tight/Loose Norms: partly subsumed into b04 (rules vs judgement) and b03 (hierarchy)

**Alternative dimensions to consider but not include in the initial run** — noted here so the developer knows the design space was considered:
- Ownership mentality (agency vs ownership)
- Stakeholder breadth (shareholder-primacy vs multi-stakeholder)
- Time-to-market vs perfectionism
- Autonomy vs control

**Do not add these to v2.** They can be tested in a v3 iteration only after v2 has been validated. Adding more dimensions without validation is what got the current framework into its present state.

### 3.2 Build the bipolar dictionaries for the 12 candidate dimensions

For each of the 12, follow the process from Phase 2 (both poles at ~60–100 terms each, corpus-mined + expert-reviewed). This is the largest single work item and should be budgeted at 4–6 weeks for one developer + intermittent expert input.

Store the new dictionary as `schroders_v2_keywords.py`, keeping v1 available for regression comparison.

### 3.3 Statistical validation of the 12 candidates

Run these tests in order. Each is a stop-gate — failing means iterate before proceeding to the next.

**Test A — Ceiling saturation.** On the full universe with the new dictionaries, no dimension should have more than 15% of companies (with ≥100 reviews) at |score| > 0.95. If a dimension saturates, its negative pole is under-populated — return to dictionary building.

**Test B — Pairwise correlation.** Compute the 12×12 correlation matrix on companies with ≥100 scored reviews. **No pairwise |r| should exceed 0.7.** If any pair does, the two dimensions are measuring the same thing — either merge them or redefine one.

**Test C — Principal components.** PCA on the standardised 12-dimension scores. **PC1 should explain no more than 40% of variance**; ideally 25–35%. If PC1 is higher, a general positivity factor remains and the negation/bipolar work in Phase 1 hasn't fully worked.

**Test D — VIF.** Each dimension's VIF against the other 11 should be < 5. Dimensions with VIF > 5 are contributing little independent information and are candidates for removal.

**Test E — Hand-label agreement.** As in Phase 2, |r| > 0.5 between engine scores and 500 hand-labelled examples per dimension.

**Test F — Sector universality.** Repeat Test B stratified by GICS sector. If dimension correlations are radically different in Financials vs Technology (say, some pairs correlate at 0.8 in one sector and 0.2 in another), the dimensions may need sector-specific scoring — flag but don't necessarily fix in v2.

### 3.4 Investment-signal validation (the important test)

The above tests establish that the dimensions are statistically distinct. This test establishes that they carry investment signal.

**Method:** For each of the 12 dimensions, compute its Pearson correlation with the composite financial performance score (ROE 5y, TSR 5y, op margin 5y, revenue growth 5y — as currently constructed) across companies with ≥100 reviews.

Then, **for each sub-industry with ≥15 companies**, run a leave-one-out cross-validation:
- Hold out one company at a time
- Fit a linear regression of performance on the 12 dimension scores using the remaining companies
- Predict the held-out company's performance
- Compute out-of-sample R² across all held-out predictions

**Report per dimension:** in-sample |r| with performance, out-of-sample |r| after LOO, and Benjamini-Hochberg-corrected p-value.

**Retention criteria:**
- Dimension retained if LOO cross-validated |r| > 0.05 in at least 5 sub-industries at BH-corrected p<0.05
- Dimensions failing this in all sub-industries are dropped (they may still be interesting descriptively but don't carry investment signal)
- If fewer than 5 dimensions survive, we should not claim "culture drives performance" as a headline — we should report specific dimensions in specific peer groups

### 3.5 Composite score construction (only after 3.4 completes)

The current composite (`hofstede×5 + mit + schroders×5`) is a scale-hack. Replace with a principled construction:

**Option 1 — Equal-weighted composite of surviving bipoles.** Simple, defensible, no free parameters.

**Option 2 — Cross-validated regression weights.** For each sub-industry, use k-fold cross-validation to derive dimension weights, then average across folds. Weights are constrained (e.g. ridge regression, L2 penalty λ chosen by CV) to prevent overfitting.

**Do not use** the current construction from METHODOLOGY.md Section 8.3 where `corr_d × deviation_d` is summed and then regressed against the same performance target on the same sample. This is the in-sample construction problem that produces the inflated R² claims in the current client report.

Report both in-sample and out-of-sample R² for the composite. **Any client-facing claim must reference the out-of-sample number.**

---

## Phase 4 — Rebuild the R² correlation matrix endpoint

**Goal.** Replace the current `/api/correlation-matrix` endpoint (`app.py:3891-4096`) with a version that produces honest, out-of-sample R² numbers.

### 4.1 Current endpoint behaviour (to be removed)

The current endpoint:
1. For each industry group, computes per-dimension Pearson correlations with composite performance
2. Uses those correlations as weights to build `framework_score = Σ corr_d × (value_d − mean_d(G))`
3. Regresses `framework_score` on composite performance in the same group
4. Reports the R² of this regression

The problem: the framework_score is optimised on the target via step 2, then measured against the same target in step 3. R² is inflated by construction.

### 4.2 New endpoint behaviour

For each industry group with ≥10 companies:

```python
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import LeaveOneOut, cross_val_predict

def compute_group_r2(dim_scores: pd.DataFrame, performance: pd.Series) -> dict:
    """dim_scores: n_companies × n_dimensions
       performance: n_companies
    Returns in-sample and out-of-sample R² using LOO cross-validation."""
    X = dim_scores.values
    y = performance.values
    if len(y) < 10:
        return None

    # Fit ridge with CV to pick lambda
    ridge = RidgeCV(alphas=[0.01, 0.1, 1.0, 10.0, 100.0], cv=5)
    ridge.fit(X, y)
    in_sample_r2 = ridge.score(X, y)

    # LOO cross-validation
    loo_preds = cross_val_predict(ridge, X, y, cv=LeaveOneOut())
    ss_res = ((y - loo_preds) ** 2).sum()
    ss_tot = ((y - y.mean()) ** 2).sum()
    out_of_sample_r2 = 1 - ss_res / ss_tot

    return {
        'in_sample_r2': in_sample_r2,
        'out_of_sample_r2': out_of_sample_r2,
        'n': len(y),
        'lambda': ridge.alpha_,
        'coefficients': dict(zip(dim_scores.columns, ridge.coef_)),
    }
```

**Key behavioural changes from current:**
- Uses ridge regression instead of the correlation-weighted deviation sum → prevents overfitting when n dimensions ≈ n companies
- Reports both in-sample and out-of-sample R² separately → so we can see the shrinkage
- Uses LOO cross-validation → gives a genuine out-of-sample estimate
- Cross-validates the ridge penalty λ → doesn't require tuning

The endpoint returns both numbers, and the dashboard displays them side-by-side. **The client-facing narrative uses only the out-of-sample number.**

### 4.3 Also add: Benjamini-Hochberg correction for sub-industry-level significance

Currently the endpoint reports p-values per sub-industry without correction for the ~150 sub-industries tested. Add BH-FDR correction across all sub-industries in each framework. Report both the raw p and the BH-adjusted q. In the client narrative, use q < 0.10 as the significance threshold for sub-industry-level claims.

### 4.4 Also add: peer-inclusion correction to z-scores

Currently, when a company's culture score is standardised against its sub-industry peers, the company itself is included in the peer statistics. This is a self-including z-score, and inflates apparent deviations for companies near the extreme. Fix:

```python
def peer_zscore_excluding_self(company_score, all_peer_scores):
    peers_without_self = all_peer_scores.drop(company_score.name)
    return (company_score - peers_without_self.mean()) / peers_without_self.std()
```

Apply this everywhere z-scoring happens. Small but real effect on regressions.

---

## Phase 5 — Temporal separation for the performance regression

**Current problem.** Reviews span 2008–2026, but the performance metrics they're regressed against are 2019–2024. Reviews written after the performance window are literally in the "predictor" for those years. This is temporal leakage.

**Required change.** Score two review-window snapshots:
- `culture_score_2018`: computed only from reviews dated ≤ 2018-12-31
- `culture_score_current`: computed from all reviews

Use `culture_score_2018` for the correlation with 2019–2024 performance. Use `culture_score_current` only for descriptive current-state dashboards, not for performance-prediction claims.

Document both series in the dashboard so users can see the temporal design.

---

## Deliverables and reporting

After each phase, deliver:

1. **Phase 1:** Passing unit tests, plus the diff report showing % of reviews with score changes vs v1
2. **Phase 2:** Hand-label validation table (per dimension |r| between engine and humans), plus updated `schroders_keywords.py`
3. **Phase 3:** The 12×12 correlation matrix, PCA scree, VIF table, and LOO cross-validated R² per dimension per sub-industry
4. **Phase 4:** Rebuilt endpoint returning in-sample and out-of-sample R², plus BH-adjusted q-values
5. **Phase 5:** Two culture-score series (2018-cutoff and current) with the temporal-separation design documented in METHODOLOGY.md

### Meta-deliverable — updated METHODOLOGY.md

The current METHODOLOGY.md is a genuine strength of this repository. Update it to reflect v2 changes, and add a *Limitations* section covering:

- What review-selection biases exist (survivorship, English-language, employee-only)
- What temporal biases exist (retrospection, incident-driven review clusters)
- What the honest out-of-sample R² is for each framework and sub-industry
- What confidence claims we make vs what we do not claim

Client trust in this framework will depend far more on honest documentation of limitations than on any additional R² decimal point.

---

## Timeline

Rough budget for one developer at ~4 days/week on this:

- Phase 1 (scoring engine): 2 weeks
- Phase 2 (dictionary rebuild): 4 weeks (dictionary work is slow and iterative)
- Phase 3 (dimension redesign): 3 weeks (fast if Phase 2 dictionaries are reused)
- Phase 4 (endpoint rebuild): 1 week
- Phase 5 (temporal separation): 1 week

Total: ~11 weeks of focused work, with the 500-review hand-labelling as a parallel activity throughout.

**Do not attempt to compress this by skipping Phases 1–2.** Every issue in the current framework traces to scoring-engine limitations. Rebuilding dimensions on a broken engine reproduces the same problems.
