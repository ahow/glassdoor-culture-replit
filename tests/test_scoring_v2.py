"""Phase 1 sanity tests for the v2 scoring engine (word boundaries + negation)."""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from scoring_engine_v2 import (
    compile_phrase, compile_keywords, find_matches_with_negation,
    score_review_v2,
)
from schroders_keywords import SCHRODERS_KEYWORDS

COMPILED = compile_keywords(SCHRODERS_KEYWORDS)


def score(text):
    return score_review_v2(text, COMPILED)


# ── Word-boundary behaviour ──────────────────────────────────────────────

def test_word_boundary_no_substring_collision():
    p = compile_phrase('ethical')
    assert p.search('very ethical culture')
    assert not p.search('this is unethical behaviour')


def test_multiword_phrase_flexible_whitespace():
    p = compile_phrase('work life balance')
    assert p.search('great work life balance')
    assert p.search('work  life   balance')
    assert p.search('work-life balance')


def test_phrase_not_matching_inside_words():
    p = compile_phrase('art')
    assert not p.search('department of parts')
    assert p.search('the art of management')


# ── Negation detection ───────────────────────────────────────────────────

def test_negation_within_window():
    p = compile_phrase('treated well')
    plain, neg = find_matches_with_negation('employees are not treated well', p)
    assert (plain, neg) == (0, 1)


def test_no_negation():
    p = compile_phrase('treated well')
    plain, neg = find_matches_with_negation('employees are treated well', p)
    assert (plain, neg) == (1, 0)


def test_contraction_negation():
    p = compile_phrase('care')
    plain, neg = find_matches_with_negation("management doesn't care", p)
    assert (plain, neg) == (0, 1)


def test_negation_outside_window():
    p = compile_phrase('balance')
    text = 'not that it matters at all much anyway good balance here'
    plain, neg = find_matches_with_negation(text, p, window=5)
    assert (plain, neg) == (1, 0)


# ── Polarity flips on real dimensions ────────────────────────────────────

def test_negation_flips_polarity_d06():
    assert score('great work-life balance and management cares')['d06']['score'] > 0
    assert score('no work-life balance here')['d06']['score'] < 0
    assert score('poor work-life balance')['d06']['score'] < 0


def test_word_boundary_integrity_d09():
    assert score('very ethical culture with strong integrity')['d09']['score'] > 0
    assert score('management is unethical and dishonest')['d09']['score'] < 0


def test_negated_positive_flips_d09():
    pos = score('strong integrity here')['d09']['score']
    neg = score('there is a lack of integrity here')['d09']['score']
    assert pos > 0
    assert neg < 0


def test_innovation_d10():
    assert score('innovative culture that encourages new ideas')['d10']['score'] > 0
    assert score('no innovation and risk averse management')['d10']['score'] < 0


def test_learning_d12():
    assert score('excellent training and development opportunities')['d12']['score'] > 0
    assert score('no training provided and zero development')['d12']['score'] < 0


def test_evidence_null_when_no_mentions():
    r = score('the office has good coffee')
    assert r['d02']['score'] is None
    assert r['d02']['evidence_count'] == 0


def test_multi_dimension_scoring():
    r = score('a culture of innovation but poor work-life balance')
    assert r['d10']['score'] > 0
    assert r['d06']['score'] < 0


def test_empty_and_invalid_input():
    assert score_review_v2('', COMPILED) is None
    assert score_review_v2(None, COMPILED) is None
    assert score_review_v2(123, COMPILED) is None


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))
