"""Reviewer diagnostic pack exporter.

Scans the full review corpus against BOTH v2 dictionaries (corpus-mined and
expert-seed) and accumulates term-level, dimension-level and review-level
diagnostics. Resumable: state + accumulators checkpointed after every batch.

Usage:
    python3 pipeline/diagnostics_export.py scan  [--batch 2000]
    python3 pipeline/diagnostics_export.py report
"""
import csv
import json
import os
import pickle
import re
import sys
from collections import defaultdict

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from scoring_engine_v2 import (  # noqa: E402
    compile_phrase, NEGATION_MARKERS, _TokenIndex, SCORING_ENGINE_VERSION)

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                       'diagnostics_pack')
STATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                         'pipeline_output')
STATE_FILE = os.path.join(STATE_DIR, 'diag_state.json')
ACC_FILE = os.path.join(STATE_DIR, 'diag_acc.pkl')
REVIEW_CSV = os.path.join(OUT_DIR, 'review_level_scores.csv')

os.makedirs(OUT_DIR, exist_ok=True)

# ---------------------------------------------------------------- dictionaries
def load_dicts():
    import importlib.util
    from schroders_v2_keywords import (
        SCHRODERS_V2_KEYWORDS as EXPERT, SCHRODERS_V2_DIMENSIONS as DIMS)
    mined_path = os.path.join(STATE_DIR, 'schroders_v2_keywords_mined_2026-08-01.py')
    spec = importlib.util.spec_from_file_location('mined_dict', mined_path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return {'mined': m.SCHRODERS_V2_KEYWORDS, 'expert': EXPERT}, DIMS


DICTS, DIMS = load_dicts()

# Flat term list: (dict_name, dim, pole, term, weight, compiled_pattern)
TERMS = []
for dname, d in DICTS.items():
    for dim in DIMS:
        for pole in ('positive', 'negative'):
            for term, w in d[dim][pole].items():
                TERMS.append((dname, dim, pole, term, w, compile_phrase(term)))
print(f"{len(TERMS)} term patterns compiled "
      f"(mined={sum(1 for t in TERMS if t[0]=='mined')}, "
      f"expert={sum(1 for t in TERMS if t[0]=='expert')})", flush=True)


def _int_dd():
    return defaultdict(int)


def new_acc():
    return {
        'n_reviews': 0,
        'n_by_sector': defaultdict(int),
        'companies': set(),
        # per term idx
        'match': defaultdict(int),        # raw matches (plain)
        'negated': defaultdict(int),      # negated matches
        'docfreq': defaultdict(int),      # reviews with >=1 hit
        'compset': defaultdict(set),      # companies with >=1 hit
        'sectorset': defaultdict(set),    # sectors with >=1 hit
        'cooc_dim': defaultdict(_int_dd),  # term -> {(dict,dim): n}
        'snippets': defaultdict(list),    # term -> up to 3 snippets
        # dim-level per dict: doc-frequency of dim hit, and dim x dim co-occurrence
        'dim_docfreq': defaultdict(int),          # (dict, dim) -> reviews hit
        'dim_cooc': defaultdict(int),             # (dict, dimA, dimB) -> reviews
        'dim_hits_dist': defaultdict(_int_dd),  # (dict,dim)->{n_matches: count}
    }


def has_negation(token_index, m_start, window=5):
    tok_idx = token_index.token_index_at(m_start)
    ws = max(0, tok_idx - window)
    for tok in token_index.tokens[ws:tok_idx]:
        tl = tok.lower().rstrip('.,;:!?')
        if tl in NEGATION_MARKERS or "n't" in tok.lower():
            return True
    return False


def scan():
    batch = 2000
    if '--batch' in sys.argv:
        batch = int(sys.argv[sys.argv.index('--batch') + 1])
    state = {'last_id': 0}
    if os.path.exists(STATE_FILE):
        state = json.load(open(STATE_FILE))
    if os.path.exists(ACC_FILE):
        with open(ACC_FILE, 'rb') as f:
            acc = pickle.load(f)
    else:
        acc = new_acc()
        # fresh review CSV with header
        with open(REVIEW_CSV, 'w', newline='') as f:
            wtr = csv.writer(f)
            hdr = ['review_id', 'company_name', 'gics_sector', 'review_datetime',
                   'employment_status', 'is_current_employee', 'location']
            for dn in ('mined', 'expert'):
                for dim in DIMS:
                    hdr += [f'{dn}_{dim}_score', f'{dn}_{dim}_evidence']
            wtr.writerow(hdr)

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    # sector map
    cur.execute("SELECT glassdoor_name, gics_sector FROM extraction_queue "
                "WHERE glassdoor_name IS NOT NULL")
    sector_map = dict(cur.fetchall())

    csvf = open(REVIEW_CSV, 'a', newline='')
    wtr = csv.writer(csvf)
    n_done = 0
    while True:
        cur.execute(
            "SELECT id, COALESCE(summary,'') || '. ' || COALESCE(pros,'') || '. ' || "
            "COALESCE(cons,'') || '. ' || COALESCE(advice_to_management,''), "
            "company_name, review_datetime, employment_status, is_current_employee, location "
            "FROM reviews WHERE id > %s ORDER BY id LIMIT %s",
            (state['last_id'], batch))
        rows = cur.fetchall()
        if not rows:
            break
        for rid, text, company, dt, emp, cur_emp, loc in rows:
            sector = sector_map.get(company, 'Asset Management/Other')
            acc['n_reviews'] += 1
            acc['n_by_sector'][sector] += 1
            acc['companies'].add(company)
            tl = (text or '').lower()
            tix = _TokenIndex(tl)
            dims_hit = set()          # (dict, dim)
            term_hits = []            # term indices hit this review
            pole_scores = defaultdict(lambda: [0.0, 0.0])  # (dict,dim)->[pos,neg]
            dim_nmatch = defaultdict(int)
            for i, (dn, dim, pole, term, w, pat) in enumerate(TERMS):
                plain = negd = 0
                for m in pat.finditer(tl):
                    if has_negation(tix, m.start()):
                        negd += 1
                    else:
                        plain += 1
                if plain or negd:
                    acc['match'][i] += plain
                    acc['negated'][i] += negd
                    acc['docfreq'][i] += 1
                    acc['compset'][i].add(company)
                    acc['sectorset'][i].add(sector)
                    dims_hit.add((dn, dim))
                    term_hits.append(i)
                    dim_nmatch[(dn, dim)] += plain + negd
                    if len(acc['snippets'][i]) < 3:
                        m0 = pat.search(tl)
                        s = max(0, m0.start() - 60)
                        acc['snippets'][i].append(
                            tl[s:m0.end() + 60].replace('\n', ' '))
                    key = pole if not negd or plain else pole
                    ps = pole_scores[(dn, dim)]
                    if pole == 'positive':
                        ps[0] += w * plain; ps[1] += w * negd
                    else:
                        ps[1] += w * plain; ps[0] += w * negd
            for i in term_hits:
                for (dn2, dim2) in dims_hit:
                    acc['cooc_dim'][i][(dn2, dim2)] += 1
            for k in dims_hit:
                acc['dim_docfreq'][k] += 1
                acc['dim_hits_dist'][k][dim_nmatch[k]] += 1
            hits_l = sorted(dims_hit)
            for a in range(len(hits_l)):
                for b in range(a, len(hits_l)):
                    if hits_l[a][0] == hits_l[b][0]:
                        acc['dim_cooc'][(hits_l[a][0], hits_l[a][1], hits_l[b][1])] += 1
            row = [rid, company, sector,
                   dt.isoformat() if dt else '', emp, cur_emp, loc]
            for dn in ('mined', 'expert'):
                for dim in DIMS:
                    pos, neg = pole_scores.get((dn, dim), (0.0, 0.0))
                    tot = pos + neg
                    row += [round((pos - neg) / tot, 4) if tot else '',
                            round(tot, 3)]
            wtr.writerow(row)
        state['last_id'] = rows[-1][0]
        n_done += len(rows)
        csvf.flush()
        with open(ACC_FILE + '.tmp', 'wb') as f:
            pickle.dump(acc, f)
        os.replace(ACC_FILE + '.tmp', ACC_FILE)
        json.dump(state, open(STATE_FILE, 'w'))
        print(f"  scanned {n_done} reviews (last id {state['last_id']})", flush=True)
    csvf.close()
    print(f"Scan done: {n_done} this run; total {acc['n_reviews']}.")


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'scan'
    if cmd == 'scan':
        scan()
    elif cmd == 'report':
        from diagnostics_report import build_reports
        build_reports(ACC_FILE, REVIEW_CSV, OUT_DIR, DICTS, DIMS)
