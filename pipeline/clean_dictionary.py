"""Hygiene pass over schroders_v2_keywords.py (uniform, rule-based).

Rules (applied identically to every dimension/pole):
 R1  Strip leading/trailing punctuation and whitespace from every term.
 R2  Drop terms that start with a negation word (no/not/never/n't/without/
     lack of/lacking). The scoring engine already handles negation via a
     token window, so embedded-negation phrases are (a) redundant and
     (b) frequently mis-poled by the miner (e.g. "no long-term vision"
     mined into the positive pole).
 R3  Drop terms whose cleaned core is shorter than 4 characters
     (generic tokens like "its", "bit").
 R4  Deduplicate within a pole after cleaning (keep the max weight);
     if the same cleaned term exists in BOTH poles of a dimension,
     drop it from both (polarity-ambiguous).

Writes the cleaned dictionary back to schroders_v2_keywords.py with a
bumped DICTIONARY_VERSION and a JSON audit of every removal to
pipeline_output/dictionary_hygiene_audit.json.
"""
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from schroders_v2_keywords import (  # noqa: E402
    SCHRODERS_V2_KEYWORDS, SCHRODERS_V2_DIMENSIONS, DICTIONARY_VERSION,
)

NEGATORS = ('no ', 'not ', 'never ', "n't ", 'without ', 'lack of ', 'lacking ')
NEW_VERSION = '2026-07-22-v3.1-generalised-clean'

audit = {'version_from': DICTIONARY_VERSION, 'version_to': NEW_VERSION,
         'removed': [], 'renamed': [], 'kept': 0}

cleaned = {}
for dim, poles in SCHRODERS_V2_KEYWORDS.items():
    cleaned[dim] = {}
    for pole, terms in poles.items():
        items = terms.items() if isinstance(terms, dict) else terms
        out = {}
        for term, weight in items:
            core = re.sub(r'^[^a-z0-9]+|[^a-z0-9)]+$', '', term.lower().strip())
            core = re.sub(r'\s+', ' ', core)
            if core != term:
                audit['renamed'].append({'dim': dim, 'pole': pole,
                                         'from': term, 'to': core})
            if len(core) < 4:
                audit['removed'].append({'dim': dim, 'pole': pole,
                                         'term': term, 'rule': 'R3-short'})
                continue
            if any(core.startswith(n) for n in NEGATORS):
                audit['removed'].append({'dim': dim, 'pole': pole,
                                         'term': term, 'rule': 'R2-negator'})
                continue
            if core in out:
                out[core] = max(out[core], float(weight))
                audit['removed'].append({'dim': dim, 'pole': pole,
                                         'term': term, 'rule': 'R4-dup-in-pole'})
            else:
                out[core] = float(weight)
        cleaned[dim][pole] = out

# R4b: cross-pole duplicates within a dimension -> drop from both
for dim, poles in cleaned.items():
    pos, neg = poles.get('positive', {}), poles.get('negative', {})
    both = set(pos) & set(neg)
    for t in both:
        audit['removed'].append({'dim': dim, 'pole': 'both', 'term': t,
                                 'rule': 'R4-cross-pole'})
        pos.pop(t, None)
        neg.pop(t, None)

audit['kept'] = sum(len(p) for d in cleaned.values() for p in d.values())

with open(os.path.join(ROOT, 'pipeline_output/dictionary_hygiene_audit.json'), 'w') as f:
    json.dump(audit, f, indent=1)

lines = ['"""Schroders v2 mined dictionary — generalised full-universe build,',
         'post-hygiene (see pipeline/clean_dictionary.py for the rules and',
         'pipeline_output/dictionary_hygiene_audit.json for the audit trail)."""',
         '',
         f'DICTIONARY_VERSION = {NEW_VERSION!r}',
         '',
         f'SCHRODERS_V2_DIMENSIONS = {json.dumps(SCHRODERS_V2_DIMENSIONS, indent=4)}',
         '',
         'SCHRODERS_V2_KEYWORDS = {']
for dim in sorted(cleaned):
    lines.append(f'    {dim!r}: {{')
    for pole in ('positive', 'negative'):
        lines.append(f'        {pole!r}: {{')
        for t, w in sorted(cleaned[dim].get(pole, {}).items()):
            lines.append(f'            {t!r}: {round(w, 4)},')
        lines.append('        },')
    lines.append('    },')
lines.append('}')
with open(os.path.join(ROOT, 'schroders_v2_keywords.py'), 'w') as f:
    f.write('\n'.join(lines) + '\n')

n_rem = len(audit['removed'])
print(f"Cleaned: kept {audit['kept']} terms, removed {n_rem}, "
      f"renamed {len(audit['renamed'])}. Version -> {NEW_VERSION}")
for rule in ('R2-negator', 'R3-short', 'R4-dup-in-pole', 'R4-cross-pole'):
    print(' ', rule, sum(1 for r in audit['removed'] if r['rule'] == rule))
