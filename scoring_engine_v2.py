"""Culture Analytics v2 scoring engine.

Phase 1 of the v2 methodology:
- Word-boundary regex matching (replaces substring matching)
- Negation-scope handling within a 5-token window
- Dictionary/engine version tagging

The engine is dictionary-agnostic: it scores any framework expressed as
{dim: {"positive": {phrase: weight}, "negative": {phrase: weight}}}.
It is used both for the legacy Schroders 18-dim dictionaries (d01-d18)
and the v2 12-bipole dictionaries (b01-b12).
"""

import re
from typing import Dict, Optional, Tuple

SCORING_ENGINE_VERSION = "v2.0-regex-negation"

NEGATION_MARKERS = {
    'no', 'not', 'never', 'nothing', 'none', 'nobody', 'nowhere',
    'lacks', 'lacking', 'lack', 'without', 'absence',
    'poor', 'weak', 'insufficient', 'inadequate', 'minimal',
    "n't",
    'hardly', 'barely', 'rarely', 'seldom',
}
# Note: intra-word prefixes like 'un-', 'dis-', 'anti-' are deliberately NOT
# included. They are not separate tokens, so a whole-token membership check
# cannot detect them. Word-boundary matching already prevents e.g. "ethical"
# firing inside "unethical". Prefixed antonyms belong in negative-pole
# dictionaries directly ("unethical", "dishonest", ...).


def compile_phrase(phrase: str) -> re.Pattern:
    """Compile a keyword phrase to a word-boundary regex.
    Multi-word phrases match with flexible whitespace/hyphen between tokens."""
    tokens = phrase.lower().split()
    pattern = r'\b' + r'[\s\-]+'.join(re.escape(t) for t in tokens) + r'\b'
    return re.compile(pattern, re.IGNORECASE)


def compile_keywords(keywords: Dict) -> Dict:
    """Pre-compile a {dim: {pole: {phrase: weight}}} dictionary.
    Returns {dim: {pole: [(pattern, weight), ...]}}. Call once at import."""
    compiled = {}
    for dim, poles in keywords.items():
        compiled[dim] = {}
        for pole, terms in poles.items():
            compiled[dim][pole] = [
                (compile_phrase(phrase), weight) for phrase, weight in terms.items()
            ]
    return compiled


class _TokenIndex:
    """Precomputed token offsets for a text, so negation windows can be
    resolved without re-splitting the text for every keyword match."""

    __slots__ = ('tokens', 'starts')

    def __init__(self, text: str):
        self.tokens = []
        self.starts = []
        for m in re.finditer(r'\S+', text):
            self.tokens.append(m.group(0))
            self.starts.append(m.start())

    def token_index_at(self, char_pos: int) -> int:
        """Index of the token containing/starting at char_pos (binary search)."""
        lo, hi = 0, len(self.starts)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.starts[mid] <= char_pos:
                lo = mid + 1
            else:
                hi = mid
        return max(0, lo - 1)


def find_matches_with_negation(text: str, pattern: re.Pattern,
                               token_index: Optional[_TokenIndex] = None,
                               window: int = 5) -> Tuple[int, int]:
    """Return (plain_matches, negated_matches) for a keyword pattern.
    A match is negated when a negation marker appears within `window`
    tokens before it."""
    if token_index is None:
        token_index = _TokenIndex(text)
    plain, negated = 0, 0
    for m in pattern.finditer(text):
        tok_idx = token_index.token_index_at(m.start())
        window_start = max(0, tok_idx - window)
        preceding = token_index.tokens[window_start:tok_idx]
        has_negation = any(
            tok.lower().rstrip('.,;:!?') in NEGATION_MARKERS or "n't" in tok.lower()
            for tok in preceding
        )
        if has_negation:
            negated += 1
        else:
            plain += 1
    return plain, negated


def score_review_v2(review_text: str, compiled_keywords: Dict,
                    window: int = 5) -> Optional[Dict]:
    """Score a review against a compiled bipolar dictionary.

    For every dimension:
        score = (pos - neg) / (pos + neg)  in [-1, +1]
        None when the dimension is not mentioned.

    Negated positive-pole matches count toward the negative pole and
    vice versa.

    Returns {dim: {'score': float|None, 'evidence_count': float,
                   'confidence': str}}.
    """
    if not review_text or not isinstance(review_text, str):
        return None

    text_lower = review_text.lower()
    token_index = _TokenIndex(text_lower)
    results = {}

    for dim, poles in compiled_keywords.items():
        pos = 0.0
        neg = 0.0
        for pattern, weight in poles.get('positive', []):
            plain, negd = find_matches_with_negation(text_lower, pattern, token_index, window)
            pos += weight * plain
            neg += weight * negd     # negated positive = evidence for negative pole
        for pattern, weight in poles.get('negative', []):
            plain, negd = find_matches_with_negation(text_lower, pattern, token_index, window)
            neg += weight * plain
            pos += weight * negd     # negated negative = evidence for positive pole

        total = pos + neg
        if total == 0:
            score = None
        else:
            score = round((pos - neg) / total, 4)

        results[dim] = {
            'score': score,
            'confidence': 'medium' if score is not None else 'low',
            'evidence_count': round(total, 4),
        }

    return results
