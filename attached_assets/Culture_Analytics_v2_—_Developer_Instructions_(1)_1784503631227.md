# Culture Analytics — Developer Instructions for v2 Methodology

**Context.** Independent methodology review of the current Schroders 18-dimension framework has identified a severe redundancy problem: on the 830 companies with 500+ reviews (i.e. those with the most reliable scores), a single principal component explains 94.8% of the variance across all 18 dimensions, and multiple dimension pairs correlate at r ≈ 1.00. This means the framework as currently scored is effectively unidimensional — it measures general review positivity, not 18 distinct cultural attributes.

This document sets out the changes required to rebuild the framework so it produces genuinely independent, investment-relevant signals. The scope of this rebuild is **development-phase only** — out-of-sample cross-validation and hand-labelled reference sets are deliberately deferred to a later phase. Success criteria in this document are internal-consistency and statistical-independence tests, not investment-signal validation.

**Reading order.** Work through the phases in sequence. Phases 1–2 are prerequisites for Phase 3 (bipolar dimension design). Do not skip ahead — designing new dimensions before fixing the scoring engine will reproduce the current problems on a new set of labels.

**Success criterion for v2 (development phase).** After the rebuild, on companies with ≥100 scored reviews:
- No pairwise dimension correlation should exceed |r| = 0.7
- PC1 should explain no more than 40% of variance
- At least 8 dimensions should have VIF < 5
- Ceiling saturation: no dimension has more than 15% of companies at |score| > 0.95

Investment-signal validation (out-of-sample R², sub-industry generalisation, statistical significance testing with FDR correction) is deferred to a later phase and is not required for this rebuild.

---

## Phase 1 — Fix the scoring engine (prerequisite work)

**Goal.** Ensure that when reviews contain language relevant to a dimension, the scorer captures it correctly and distinguishes positive from negative usage. This is a prerequisite for bipolar dimensions — without these fixes, negation and antonym-based scoring cannot work.

### 1.1 Word-boundary matching

**Current problem.** `culture_scoring.py:3788` uses `phrase in text_lower`. This means "ethical" matches inside "unethical", "not helpful" is not distinguished from "helpful", "no work-life balance" scores identically to "great work-life balance". Substring matching cannot support bipolar scoring — a bipolar rebuild without this fix will produce the same collisions we have today.

**Required change.** Replace substring matching with regex word-boundary matching:

```python
import re

def _compile_phrase(phrase: str) -> re.Pattern:
    """Compile a keyword phrase to a word-boundary regex.
    Multi-word phrases match with flexible whitespace between tokens."""
    tokens = phrase.lower().split()
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
```

**Sanity check (automated).** Run a diff on 1,000 random reviews scored under v1 vs v2. Expect noticeably different scores on reviews containing negated forms. If fewer than 5% of reviews show any score change, the regex is not firing correctly.

### 1.2 Negation-scope handling

**Current problem.** Even with word boundaries, "employees are not treated well" contains "treated well" which will fire a positive-pole term. English negation typically operates within a 3–5 word window; the scorer needs to detect this.

**Required change.** Two-pass approach — for every keyword match, check whether a negation marker appears within a 5-token window before the match:

```python
NEGATION_MARKERS = {
    'no', 'not', 'never', 'nothing', 'none', 'nobody', 'nowhere',
    'lacks', 'lacking', 'lack', 'without', 'absence',
    'poor', 'weak', 'insufficient', 'inadequate', 'minimal',
    "n't",  # captures "isn't", "don't", "doesn't", "won't"
    'hardly', 'barely', 'rarely', 'seldom',
    'anti', 'un', 'dis',  # prefixes worth flagging
}

def _find_matches_with_negation(text: str, pattern: re.Pattern, window: int = 5) -> tuple[int, int]:
    """Return (positive_matches, negated_matches) for a keyword pattern."""
    tokens = text.split()
    positive_count, negated_count = 0, 0
    for m in pattern.finditer(text):
        char_pos = m.start()
        token_idx = len(text[:char_pos].split())
        window_start = max(0, token_idx - window)
        preceding = tokens[window_start:token_idx]
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

Then in the scorer:
```python
# For each positive-pole term:
pos_matches, negated_pos = _find_matches_with_negation(text_lower, pattern, window=5)
pos += weight * pos_matches
neg += weight * negated_pos   # negated positive = evidence for negative pole

# For each negative-pole term:
neg_matches, negated_neg = _find_matches_with_negation(text_lower, pattern, window=5)
neg += weight * neg_matches
pos += weight * negated_neg   # negated negative = evidence for positive pole (rare but real)
```

**Sanity check (automated).** Run the following programmatic tests. All should pass:

```python
def test_negation_flips_polarity():
    assert score("great work-life balance")['d06'] > 0
    assert score("no work-life balance")['d06'] < 0
    assert score("terrible work-life balance")['d06'] < 0

def test_word_boundary_no_substring_collision():
    assert score("this is unethical behaviour")['d09'] < 0
    assert score("very ethical culture")['d09'] > 0
    # "ethical" inside "unethical" must not fire positive
    assert score("management is unethical")['d09'] < 0

def test_evidence_null_when_no_mentions():
    assert score("the office has good coffee")['d02'] is None

def test_multi_dimension_scoring():
    text = "innovative culture but poor work-life balance"
    r = score(text)
    assert r['d10'] > 0 and r['d06'] < 0
```

Add tests for at least 5 dimensions before proceeding.

### 1.3 Persist dictionary version alongside every scored review

**Current problem.** The `review_culture_scores` table stores scores without recording which dictionary version produced them. If dictionaries evolve, we can't tell which rows are stale.

**Required change.** Add `dictionary_version` (VARCHAR, e.g. '2026-08-01-v2') and `scoring_engine_version` (VARCHAR, e.g. 'v2.0-regex-negation') columns to `review_culture_scores`. Populate on every insert. When either changes, mark affected companies as needing re-scoring.

---

## Phase 2 — Generate bipolar dictionaries automatically

**Goal.** Bring the negative-pole dictionaries up to parity with the positive-pole dictionaries, using automated methods only. No hand-labelling. Target: each dimension has ~60–100 terms per pole, weight-matched, with automated cross-loading control.

The core insight: **you have 3.4 million reviews.** That's enough text to derive high-quality dimension-specific vocabularies purely from data, without any manual curation of individual keywords.

### 2.1 Seed each pole with ~10 anchor phrases

For each of the 12 candidate bipolar dimensions defined in Phase 3.1, the developer (or Andy) provides 8–12 anchor phrases per pole. These are not the final dictionary — they are the semantic centres from which the automated expansion generates the full vocabulary.

Example, dimension b09 Toxic ↔ Supportive:
- Negative pole seeds: "toxic culture", "toxic environment", "toxic workplace", "hostile management", "bullying", "high turnover", "burnout culture", "no support", "management doesn't care"
- Positive pole seeds: "supportive culture", "supportive team", "management cares", "psychologically safe", "healthy environment", "low turnover", "strong support system", "work-life balance respected", "compassionate leadership"

Seeds should be phrases actually observed in reviews (grep the corpus to verify) rather than abstract concepts. This is a 30-minute exercise per dimension, done once, with no ongoing manual work.

### 2.2 Expand each pole via embedding similarity + corpus co-occurrence

This is where the 3.4m reviews earn their keep. Three parallel expansion methods, all fully automated:

**Method A — Embedding similarity (primary source).**

Compute sentence embeddings for the seed phrases and for a large sample of noun/adjective phrases mined from the review corpus. Use a lightweight, permissive embedding model — `sentence-transformers/all-MiniLM-L6-v2` is a good default (small, fast, 384-dim, works well for short phrases). For each seed, retrieve the top-N most similar phrases by cosine similarity.

```python
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

# Mine candidate phrases from corpus (this is a one-time preprocessing step)
def mine_candidate_phrases(reviews_iterator, min_freq=20):
    """Extract 1-3 word noun/adjective phrases with sufficient frequency."""
    import spacy
    nlp = spacy.load('en_core_web_sm', disable=['ner'])  # fast tokenizer + POS
    from collections import Counter
    counter = Counter()
    for review in reviews_iterator:
        doc = nlp(review[:5000])  # truncate very long reviews
        for chunk in doc.noun_chunks:
            phrase = chunk.text.lower().strip()
            if 1 <= len(phrase.split()) <= 3 and phrase.isascii():
                counter[phrase] += 1
        # Also extract adjective phrases and verb phrases
        for i in range(len(doc) - 1):
            if doc[i].pos_ == 'ADV' and doc[i+1].pos_ == 'ADJ':
                phrase = f"{doc[i].text} {doc[i+1].text}".lower()
                counter[phrase] += 1
    return {p: c for p, c in counter.items() if c >= min_freq}

# One-time: build candidate pool from the full corpus
candidate_phrases = mine_candidate_phrases(all_reviews, min_freq=20)
# Expected size: ~50,000-200,000 unique phrases with freq >= 20

# For each dimension pole, expand seeds to a full vocabulary
def expand_pole(seeds: list[str], candidates: dict[str, int],
                model, top_n: int = 200, min_similarity: float = 0.45) -> dict[str, float]:
    """Return {phrase: weight} for expanded pole vocabulary.
    Weight = mean cosine similarity to seeds, discounted by cross-loading."""
    seed_embs = model.encode(seeds, normalize_embeddings=True)
    candidate_list = list(candidates.keys())
    cand_embs = model.encode(candidate_list, normalize_embeddings=True,
                              batch_size=256, show_progress_bar=False)

    # Similarity of each candidate to the mean seed embedding
    seed_centroid = seed_embs.mean(axis=0)
    similarities = cand_embs @ seed_centroid

    # Keep top N above threshold
    ranked = sorted(zip(candidate_list, similarities), key=lambda x: -x[1])
    selected = {p: float(s) for p, s in ranked if s >= min_similarity}
    # Cap at top_n
    top_selected = dict(sorted(selected.items(), key=lambda x: -x[1])[:top_n])
    return top_selected
```

Target: 150–300 candidate phrases per pole from this method alone. Similarity threshold 0.45 is a starting point; tune per dimension using the automated validation in 2.5 below.

**Method B — Co-occurrence with anchor phrases.**

For each seed phrase, find all sentences in the corpus containing it. Extract other noun/adjective phrases from those sentences. Phrases that co-occur frequently with the seed but rarely with the opposite pole's seeds are strong candidates.

```python
def cooccurrence_expansion(pole_seeds: list[str], opposite_seeds: list[str],
                            reviews_iterator, top_n: int = 100) -> dict[str, float]:
    """Find phrases that co-occur with pole_seeds but not opposite_seeds."""
    from collections import Counter, defaultdict
    pole_context = Counter()
    opposite_context = Counter()
    pole_seed_freq = Counter()
    opposite_seed_freq = Counter()

    pole_patterns = [re.compile(r'\b' + re.escape(s) + r'\b', re.IGNORECASE) for s in pole_seeds]
    opp_patterns = [re.compile(r'\b' + re.escape(s) + r'\b', re.IGNORECASE) for s in opposite_seeds]

    for review in reviews_iterator:
        # Split into sentences
        sentences = re.split(r'[.!?]+', review)
        for sent in sentences:
            has_pole = any(p.search(sent) for p in pole_patterns)
            has_opp = any(p.search(sent) for p in opp_patterns)
            if not (has_pole or has_opp):
                continue
            # Extract other noun/adj phrases from this sentence (reuse spacy pipeline)
            phrases = extract_noun_adj_phrases(sent)
            if has_pole:
                for p in phrases: pole_context[p] += 1
            if has_opp:
                for p in phrases: opposite_context[p] += 1

    # Pointwise mutual information: phrases enriched with pole vs opposite
    scored = {}
    for phrase, pole_ct in pole_context.items():
        opp_ct = opposite_context.get(phrase, 0)
        # PMI: log((pole_ct + 1) / (opp_ct + 1)) — favours pole-specific phrases
        pmi = np.log((pole_ct + 1) / (opp_ct + 1))
        if pole_ct >= 10:  # minimum evidence
            scored[phrase] = float(pmi)

    return dict(sorted(scored.items(), key=lambda x: -x[1])[:top_n])
```

Target: 50–100 additional candidates per pole from this method.

**Method C — Antonym generation (for negative-pole enrichment).**

For each positive-pole term with high embedding-similarity score, algorithmically generate negation variants and add them to the negative pole:

```python
def generate_negation_variants(positive_terms: dict[str, float]) -> dict[str, float]:
    """Generate 'no X', 'not X', 'lack of X', 'poor X', etc. for each positive term."""
    variants = {}
    prefixes = ['no ', 'not ', 'lack of ', 'lacks ', 'absence of ', 'poor ', 'weak ', 'insufficient ', 'little ', 'zero ']
    for term, weight in positive_terms.items():
        for prefix in prefixes:
            variant = prefix + term
            variants[variant] = weight * 0.7  # discount weight for generated variants
    # Also try WordNet antonyms for single-word terms
    from nltk.corpus import wordnet
    for term, weight in positive_terms.items():
        if len(term.split()) == 1:
            for syn in wordnet.synsets(term):
                for lemma in syn.lemmas():
                    for ant in lemma.antonyms():
                        variants[ant.name().replace('_', ' ')] = weight * 0.8
    return variants
```

Filter these variants back through Method A (embedding similarity to negative-pole seeds) to drop artifacts — many algorithmically generated variants won't be phrases people actually write.

### 2.3 Deduplicate, cross-load, and weight

Combine the three sources:

```python
def build_pole_dictionary(pole_seeds, opposite_seeds, all_reviews, model):
    method_a = expand_pole(pole_seeds, candidate_phrases, model, top_n=300, min_similarity=0.45)
    method_b = cooccurrence_expansion(pole_seeds, opposite_seeds, all_reviews, top_n=100)
    # For negative pole, add generated antonyms of positive pole
    # For positive pole, this step is skipped

    # Union with weight averaging
    combined = {}
    for phrase, score_a in method_a.items():
        combined[phrase] = combined.get(phrase, 0) + score_a
    for phrase, score_b in method_b.items():
        # Normalise method B scores (PMI) to similar range as method A (cosine)
        normalised_b = min(1.0, max(0.0, score_b / 3.0))
        combined[phrase] = combined.get(phrase, 0) + normalised_b

    # Cap dictionary at ~80 terms per pole (keeps computation manageable, avoids diminishing returns)
    top_terms = dict(sorted(combined.items(), key=lambda x: -x[1])[:80])
    return top_terms
```

**Cross-loading penalty.** After building all 24 dictionaries (12 dimensions × 2 poles), apply the cross-loading penalty across all of them:

```python
def apply_cross_load_penalty(all_dictionaries: dict) -> dict:
    """all_dictionaries: {(dim, pole): {phrase: weight}}
       Down-weights phrases appearing in multiple pole dictionaries."""
    from collections import Counter
    phrase_load_count = Counter()
    for (dim, pole), terms in all_dictionaries.items():
        for phrase in terms:
            phrase_load_count[phrase] += 1

    penalised = {}
    for (dim, pole), terms in all_dictionaries.items():
        penalised[(dim, pole)] = {}
        for phrase, weight in terms.items():
            load = phrase_load_count[phrase]
            effective_weight = weight / (1 + 0.5 * (load - 1))
            # Drop terms that load on 4+ different poles — they're too generic
            if load < 4:
                penalised[(dim, pole)][phrase] = round(effective_weight, 3)
    return penalised
```

### 2.4 Sensitivity to seed choice

**Important honest limitation.** The final dictionary depends on the seed choice. Different seeds → different vocabularies → different scores. To characterise this sensitivity:

```python
def seed_stability_test(pole_seeds: list[str], all_reviews, model, n_bootstrap: int = 5):
    """Randomly hold out 2 seeds each iteration, build dictionary from the remaining 6-10,
    measure overlap with the full-seed dictionary."""
    full_dict = build_pole_dictionary(pole_seeds, ...)
    overlaps = []
    for _ in range(n_bootstrap):
        held_out = random.sample(pole_seeds, 2)
        remaining = [s for s in pole_seeds if s not in held_out]
        bootstrap_dict = build_pole_dictionary(remaining, ...)
        overlap = len(set(full_dict) & set(bootstrap_dict)) / len(full_dict)
        overlaps.append(overlap)
    return np.mean(overlaps), np.std(overlaps)
```

Target: >80% mean overlap across bootstrap iterations. If a dimension's overlap is below 70%, the seeds are unstable — expand the seed set to 15+ phrases and retry.

### 2.5 Automated dictionary validation (no hand-labelling required)

Four automated checks that must pass before dictionaries are deployed:

**Check 1 — Balance.** For each dimension, positive-pole term count is within 20% of negative-pole term count. Current framework fails this (median 87 positive vs 15 negative — ratio 5.8×). Target: ratio between 0.8 and 1.25.

**Check 2 — Corpus firing rate.** For a random 10,000 reviews, each pole fires (at least one term matches) on a reasonable fraction of reviews. Target: 5–40% firing rate. Below 5% → pole too narrow; above 40% → pole too generic.

**Check 3 — Semantic separation.** The mean embedding of the positive pole terms should be reasonably far from the mean embedding of the negative pole terms of the same dimension (cosine similarity < 0.5 between pole centroids), but the mean embedding of pole A of dimension 1 should be well-separated from all poles of dimensions 2–12 (cosine similarity < 0.6 to any other pole centroid). Automate this as:

```python
def check_semantic_separation(all_dictionaries, model):
    centroids = {}
    for (dim, pole), terms in all_dictionaries.items():
        embs = model.encode(list(terms.keys()), normalize_embeddings=True)
        centroids[(dim, pole)] = embs.mean(axis=0)

    issues = []
    for (d1, p1), c1 in centroids.items():
        # Check own-dimension separation (positive vs negative)
        opposite = (d1, 'negative' if p1 == 'positive' else 'positive')
        if opposite in centroids:
            sim = float(np.dot(c1, centroids[opposite]))
            if sim > 0.5:
                issues.append(f"{d1} {p1}<->neg similarity too high: {sim:.2f}")
        # Check cross-dimension separation
        for (d2, p2), c2 in centroids.items():
            if d1 == d2: continue
            sim = float(np.dot(c1, c2))
            if sim > 0.6:
                issues.append(f"{d1} {p1} too similar to {d2} {p2}: {sim:.2f}")
    return issues
```

**Check 4 — Known-company sanity test.** Pick 6 companies with well-known cultural reputations (e.g. Netflix — high-performance/individual/risk-taking; Berkshire Hathaway — long-term/frugal; Costco — supportive/frugal/customer-focused; WeWork ca. 2018 — chaotic/growth-focused/toxic; JPMorgan — hierarchical/rules-driven; Patagonia — purpose-driven/values-oriented). Score them under the new dictionary and verify the dominant pole for each dimension matches the reputation. This is not manual keyword labelling — it's a sanity check on the aggregate output for known cases.

If any of the four checks fails, iterate on seeds and thresholds before proceeding to Phase 3.

---

## Phase 3 — Redesign the dimension set

**Goal.** Replace the current 18 dimensions with a smaller number of genuinely independent bipolar constructs, chosen for investment relevance and validated statistically.

### 3.1 Pre-specify the candidate dimensions (before any data analysis)

The 12 candidate bipoles. Redesigned to be pairwise-orthogonal by construction and investment-relevant.

| ID | Pole A (score → -1) | Pole B (score → +1) | Investment thesis |
|----|---------------------|---------------------|-------------------|
| b01 | Short-term / quarterly | Long-term / patient | Long-term orientation predicts capex quality and R&D productivity |
| b02 | Cost-focused / thrifty | Growth-focused / investing | Both legitimate; alignment with value vs growth strategy |
| b03 | Hierarchical / top-down | Egalitarian / distributed | Egalitarian predicts faster decisions and retention in knowledge industries |
| b04 | Rules-driven / process | Judgement-driven / adaptive | Rules-driven for regulated; judgement-driven for volatile |
| b05 | Individual performance | Team performance | Individual outperformance for sales; team outperformance for engineering |
| b06 | Insular / internally-driven | Externally-focused / market-driven | Externally-focused predicts better response to disruption |
| b07 | Risk-averse | Risk-taking | Risk-taking → growth optionality; risk-aversion → drawdown protection |
| b08 | Political / tenure-based | Meritocratic / performance-based | Meritocracy → talent retention and productivity |
| b09 | Toxic / high-turnover | Supportive / low-turnover | Retention cost and hiring quality |
| b10 | Chaotic / strategy churn | Stable / consistent | Execution quality |
| b11 | Compliance-minimising | Integrity-maximising | Reduced regulatory/reputational tail risk |
| b12 | Homogeneous | Diverse & inclusive | DEI correlates with decision quality where evidence supports it |

**Dimensions dropped from current 18:**
- d01 Purpose → bundled into b02, b06
- d05 Psychological Safety → partly in b09, b03
- d10 Innovation & Risk → split into b02, b07
- d11 Agility → into b04
- d12 Learning → facet of b09
- d14 Competitive Assertiveness → dropped (conflates internal drive with external posture; neither pole is clearly better for investment outcomes)
- d15 DEI → kept as b12 with narrower scope (inclusion)
- d16 Profession-focus → dropped (investment relevance unclear, high redundancy)
- d18 Tight/Loose Norms → partly in b04, b03

Do not add dimensions to this list without discussion. The core failure of the current framework is dimension inflation; adding more without validation reproduces it.

### 3.2 Build the bipolar dictionaries for the 12 candidates

Follow Phase 2 workflow — seeds → embedding expansion → co-occurrence → antonym generation → deduplicate → cross-load penalty → automated validation.

Store as `schroders_v2_keywords.py`, keeping v1 available for regression comparison.

### 3.3 Score the full universe under v2 dictionaries

Re-score all 1,957 companies (or however many reviews are available) using the v2 dictionaries and the v2 scoring engine (Phase 1 fixes). Persist with `dictionary_version = '2026-08-01-v2'`, `scoring_engine_version = 'v2.0-regex-negation'`.

### 3.4 Statistical validation of the 12 candidates (all automated)

Run these tests in order on companies with ≥100 scored reviews. Each is a stop-gate — failing means iterate before proceeding.

**Test A — Ceiling saturation.** No dimension should have more than 15% of companies at |score| > 0.95. If a dimension saturates, its opposite pole is under-populated — return to dictionary expansion (Phase 2), broadening seeds or lowering similarity thresholds.

**Test B — Pairwise correlation.** Compute the 12×12 correlation matrix. **No pairwise |r| should exceed 0.7.** If any pair does, the two dimensions are measuring the same thing — either merge or redefine one, and return to dictionary building for that pair.

**Test C — Principal components.** PCA on the standardised 12-dimension scores. **PC1 should explain no more than 40% of variance**; ideally 25–35%. If PC1 is higher, a general positivity factor remains. This is a diagnostic for whether Phase 1 (negation, word boundaries) and Phase 2 (symmetric bipolar dictionaries) succeeded in breaking the positivity factor.

**Test D — VIF.** Each dimension's VIF against the other 11 should be < 5. Dimensions with VIF > 5 are candidates for removal or redefinition.

**Test E — Sector universality.** Repeat Test B stratified by GICS sector (top 5 sectors by company count). Correlations may differ modestly across sectors; if they differ radically (some pairs at 0.8 in one sector and 0.2 in another), dimensions may need sector-specific scoring — flag but don't necessarily fix in v2.

**Test F — Bootstrap stability of the correlation matrix.** Random 80% subsample × 20 iterations. Correlation coefficients should be stable (standard deviation of any pair < 0.05 across iterations). Unstable correlations suggest small n effects that will invalidate downstream analysis.

### 3.5 Composite score construction

Two composite constructions to compute in parallel — this is cheap and lets you compare:

**Composite 1 — Equal-weighted.** For companies with all 12 dimensions scored: `composite = mean(all 12 z-scored dimensions)`. Simple, defensible, no free parameters, no in-sample-fitting concern.

**Composite 2 — Correlation-weighted (current method, kept for continuity).** For each industry group, per-dimension correlation with composite financial performance → weight × deviation → sum. This is the current construction from METHODOLOGY.md Section 8.3. Kept unchanged in this phase so the R² numbers remain comparable to v1.

Report both in the dashboard. Note in the METHODOLOGY.md that Composite 2 is in-sample by construction and will be replaced with an out-of-sample equivalent in a later phase.

---

## Phase 4 — Rebuild the R² correlation matrix endpoint (light-touch update)

**Goal.** Keep the current in-sample correlation-matrix methodology, but add complementary honesty metrics that are essentially free to compute and give an early signal on whether the R² will survive later out-of-sample validation.

**Out-of-sample cross-validation is deferred to a later phase.** This document only requires the diagnostic metrics below during the development phase.

### 4.1 Keep current endpoint behaviour

Retain the existing `/api/correlation-matrix` logic in `app.py:3891-4096`:
1. For each industry group, compute per-dimension Pearson correlations with performance
2. Use correlations as weights: `framework_score = Σ corr_d × (value_d − mean_d(G))`
3. Regress framework_score on performance in same group → in-sample R²

This continues to be reported as the headline number during development.

### 4.2 Add adjusted R² and confidence intervals (near-zero cost)

Alongside every reported R², report three additional metrics that give an early honesty signal:

```python
import numpy as np
from scipy import stats

def enriched_r2(y: np.ndarray, y_pred: np.ndarray, n_predictors: int) -> dict:
    """Compute in-sample R² plus adjustment metrics.
    n_predictors: number of dimensions used to build the composite (33 for combined,
                  18 for Schroders, 9 for MIT, 6 for Hofstede)."""
    n = len(y)
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot

    # Adjusted R² penalises for free parameters used
    adj_r2 = 1 - (1 - r2) * (n - 1) / (n - n_predictors - 1) if n > n_predictors + 1 else np.nan

    # Bootstrap 95% CI on the in-sample R²
    r2_boot = []
    for _ in range(500):
        idx = np.random.choice(n, n, replace=True)
        y_b, y_pred_b = y[idx], y_pred[idx]
        ss_res_b = np.sum((y_b - y_pred_b) ** 2)
        ss_tot_b = np.sum((y_b - y_b.mean()) ** 2)
        if ss_tot_b > 0:
            r2_boot.append(1 - ss_res_b / ss_tot_b)
    ci_low, ci_high = np.percentile(r2_boot, [2.5, 97.5]) if r2_boot else (np.nan, np.nan)

    return {
        'r2_in_sample': float(r2),
        'r2_adjusted': float(adj_r2),
        'r2_ci_low': float(ci_low),
        'r2_ci_high': float(ci_high),
        'n_companies': int(n),
        'n_predictors': int(n_predictors),
    }
```

**Interpretation guide for the dashboard.**
- If `r2_adjusted` is much lower than `r2_in_sample` (e.g. adjusted 0.10 vs raw 0.36), the raw R² is heavily inflated by degrees of freedom — out-of-sample validation will produce a much smaller number
- If `r2_ci_low` is close to zero (95% CI includes near-zero values), the R² is not statistically robust even in-sample
- If both look reasonable (adjusted close to raw, CI comfortably above zero), the R² is more likely to survive future out-of-sample validation

**These are diagnostic, not gate-keeping.** No decision is required based on them during development — they simply flag likely issues early. Continue to use `r2_in_sample` as the primary dashboard metric during this phase.

### 4.3 Peer-inclusion correction to z-scores (self-including z-scores)

**Current problem.** When a company's culture score is standardised against its sub-industry peers, the company itself is included in the peer statistics. This is a self-including z-score and inflates apparent deviations for companies at the extreme. The current correlation-matrix endpoint uses these self-including z-scores.

**Required change.**
```python
def peer_zscore_excluding_self(company_scores: pd.Series, peer_group_scores: pd.DataFrame) -> pd.Series:
    """For each company in peer_group_scores, standardise against peers excluding self."""
    result = {}
    for company in peer_group_scores.index:
        peers_without_self = peer_group_scores.drop(company)
        mean = peers_without_self.mean()
        std = peers_without_self.std()
        if std > 0:
            result[company] = (peer_group_scores.loc[company] - mean) / std
        else:
            result[company] = 0
    return pd.Series(result)
```

Apply everywhere z-scoring happens. Effect on regressions is small but real (each company's z-score is slightly larger in magnitude when self is excluded), and it removes a legitimate methodological criticism.

---

## Phase 5 — Temporal separation for the performance regression

**Current problem.** Reviews span 2008–2026, but the performance metrics they're regressed against are 2019–2024. Reviews written after the performance window are literally in the "predictor" for those years.

**Required change.** Score two review-window snapshots:
- `culture_score_2018_cutoff`: computed only from reviews with `review_date ≤ 2018-12-31`
- `culture_score_current`: computed from all reviews (current behaviour)

Use `culture_score_2018_cutoff` for the correlation with 2019–2024 performance. Use `culture_score_current` only for descriptive current-state dashboards.

Document both series in METHODOLOGY.md. The temporal-separation design is a separate methodological concern from the redundancy and bipolarity fixes — its priority relative to Phases 1–4 is up to the developer, but it should be included in v2.

---

## Deliverables and reporting

After each phase, produce:

1. **Phase 1:** Passing unit tests, plus the diff report showing % of reviews with score changes vs v1
2. **Phase 2:** The 12 × 2 dictionary files, seed-stability report per pole (bootstrap overlap), semantic-separation check output, corpus firing rates per pole, and known-company sanity test output
3. **Phase 3:** The 12×12 correlation matrix, PCA scree, VIF table, and per-sector correlation matrices for the top 5 sectors
4. **Phase 4:** Rebuilt endpoint returning enriched R² metrics (raw + adjusted + bootstrap CI) plus self-excluding z-scores
5. **Phase 5:** Two culture-score series (2018-cutoff and current)

### Meta-deliverable — updated METHODOLOGY.md

The current METHODOLOGY.md is a genuine strength of this repository. Update it to reflect v2 changes and add:

**Limitations section (required):**
- Dictionary methodology: fully automated from seed phrases; results depend on seed choice; no hand-labelling in v2
- The correlation-matrix R² is in-sample by construction; out-of-sample validation is a planned future phase; adjusted R² and bootstrap CI reported alongside for context
- Review-selection biases: survivorship, English-language, employee-only, US-tilt
- Temporal separation used for performance regressions (2018-cutoff series)
- Confidence tier: scores from companies with <20 reviews are shown greyed-out; scores from companies with <5 reviews are hidden

**Sensitivity section (required):**
- Seed-stability bootstrap results per dimension
- Ceiling saturation statistics per dimension
- PC1 variance explained (should be <40% after v2)
- Companies excluded due to insufficient reviews

---

## Timeline

Rough budget for one developer at ~4 days/week:

- Phase 1 (scoring engine + tests): 2 weeks
- Phase 2 (automated dictionary generation): 2–3 weeks (faster than hand-curation)
- Phase 3 (dimension redesign + validation): 2–3 weeks
- Phase 4 (endpoint diagnostics + z-score fix): 3–5 days
- Phase 5 (temporal separation): 1 week

Total: ~7–9 weeks of focused work. This is shorter than the hand-labelled variant (~11 weeks) because Phase 2 is fully automated.

**Do not compress by skipping Phases 1–2.** Every issue in the current framework traces to scoring-engine limitations. Rebuilding dimensions on a broken engine reproduces the problems.

**Out-of-sample validation is not in this phase.** After v2 dashboards are stable and reviewed internally, a follow-on phase should add: leave-one-out cross-validation on the R² endpoint, Benjamini-Hochberg FDR correction for sub-industry-level p-values, a hand-labelled reference set of 100–200 reviews for external validity checks, and comparison of v2 dictionaries against a small human-scored sample. That's the point at which client-facing use should be reassessed.
