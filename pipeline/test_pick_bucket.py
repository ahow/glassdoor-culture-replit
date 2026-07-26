"""Deterministic unit test for pick_bucket() residual-count semantics.

Run: python3 pipeline/test_pick_bucket.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from factor_build import pick_bucket

SETTINGS = {
    'peer_hierarchy': ['gics_sub_industry', 'gics_industry', 'gics_sector'],
    'min_companies_per_bucket': 3,
}


def mk(sector, industry, sub):
    return {'gics_sector': sector, 'gics_industry': industry,
            'gics_sub_industry': sub}


def test_basic_finest_level():
    info = {f'c{i}': mk('S', 'I', 'SUB') for i in range(3)}
    buckets, levels = pick_bucket(info, SETTINGS)
    assert all(buckets[c] == 'SUB' for c in info), buckets
    assert levels['SUB'] == 'gics_sub_industry'


def test_residual_demotion_prefers_adequate_peer_set():
    # 3 companies in SUB-A (assigned at sub-industry); 2 leftover companies in
    # the same industry: full industry count is 5 >= 3, but residual is 2 < 3,
    # so they must fall to sector where the residual (2+2=4) is adequate.
    info = {}
    for i in range(3):
        info[f'a{i}'] = mk('S', 'I', 'SUB-A')
    for i in range(2):
        info[f'b{i}'] = mk('S', 'I', 'SUB-B')
    for i in range(2):
        info[f'x{i}'] = mk('S', 'I2', 'SUB-X')
    buckets, levels = pick_bucket(info, SETTINGS)
    assert all(buckets[f'a{i}'] == 'SUB-A' for i in range(3))
    # leftovers pooled at sector level with >= min_n peers
    leftover = [buckets['b0'], buckets['b1'], buckets['x0'], buckets['x1']]
    assert leftover == ['S'] * 4, buckets
    assert levels['S'] == 'gics_sector'


def test_global_fallback_for_tiny_remainder():
    info = {'lone1': mk('S1', 'I1', 'SUB1'), 'lone2': mk('S2', 'I2', 'SUB2')}
    buckets, levels = pick_bucket(info, SETTINGS)
    assert buckets == {'lone1': 'global', 'lone2': 'global'}, buckets
    assert levels['global'] == 'global'


def test_cross_level_name_collision_disambiguated():
    # 'Distributors' exists as both a sub-industry (big enough) and as an
    # industry name for a different residual group.
    info = {}
    for i in range(3):
        info[f'd{i}'] = mk('S', 'Other Ind', 'Distributors')
    for i in range(3):
        info[f'e{i}'] = mk('S', 'Distributors', f'Tiny-{i}')
    buckets, levels = pick_bucket(info, SETTINGS)
    assert all(buckets[f'd{i}'] == 'Distributors' for i in range(3))
    assert levels['Distributors'] == 'gics_sub_industry'
    e_bucket = buckets['e0']
    assert e_bucket != 'Distributors' and 'Distributors' in e_bucket, buckets
    assert levels[e_bucket] == 'gics_industry'


def test_every_nonglobal_bucket_meets_min_n():
    import random
    rng = random.Random(0)
    info = {}
    for i in range(60):
        s = f'S{rng.randint(1, 3)}'
        ind = f'{s}-I{rng.randint(1, 3)}'
        sub = f'{ind}-U{rng.randint(1, 3)}'
        info[f'c{i}'] = mk(s, ind, sub)
    buckets, levels = pick_bucket(info, SETTINGS)
    from collections import Counter
    counts = Counter(buckets.values())
    for b, n in counts.items():
        if b != 'global':
            assert n >= SETTINGS['min_companies_per_bucket'], (b, n)


def test_adequate_set_controls_qualification():
    # 4 companies in SUB but only 2 adequate -> SUB does not qualify;
    # at sector level 3 adequate (2 + 1 from another industry) -> everyone,
    # adequate or not, is assigned the sector bucket.
    info = {}
    for i in range(4):
        info[f'a{i}'] = mk('S', 'I', 'SUB')
    info['x0'] = mk('S', 'I2', 'SUB-X')
    adequate = {'a0', 'a1', 'x0'}
    buckets, levels = pick_bucket(info, SETTINGS, adequate=adequate)
    assert all(b == 'S' for b in buckets.values()), buckets
    assert levels['S'] == 'gics_sector'


def test_adequate_set_assigns_inadequate_members_too():
    # 3 adequate + 2 inadequate in same sub-industry: bucket qualifies at
    # sub-industry level and ALL 5 are assigned to it.
    info = {f'c{i}': mk('S', 'I', 'SUB') for i in range(5)}
    adequate = {'c0', 'c1', 'c2'}
    buckets, levels = pick_bucket(info, SETTINGS, adequate=adequate)
    assert all(buckets[c] == 'SUB' for c in info), buckets
    assert levels['SUB'] == 'gics_sub_industry'


def test_no_adequate_companies_all_global():
    info = {f'c{i}': mk('S', 'I', 'SUB') for i in range(5)}
    buckets, levels = pick_bucket(info, SETTINGS, adequate=set())
    assert all(b == 'global' for b in buckets.values()), buckets


if __name__ == '__main__':
    fns = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for fn in fns:
        fn()
        print(f'PASS {fn.__name__}')
    print(f'{len(fns)} tests passed')
