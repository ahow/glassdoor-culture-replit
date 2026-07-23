"""Phase 2 — automated bipolar dictionary generation for the 12 v2 dimensions.

Stages (run in order; each stage persists output so runs are resumable):
  mine     — one pass over the review corpus with spaCy: candidate phrase
             frequencies (Method A pool) + per-pole sentence co-occurrence
             counts (Method B), saved to pipeline_output/
  embed    — MiniLM embeddings for all candidate phrases
  build    — Methods A+B+C per pole, combine, cap, cross-load penalty,
             write schroders_v2_keywords.py
  stability— seed bootstrap stability test (report only)

Usage:
    python pipeline/build_dictionaries.py mine [--limit N] [--min-freq K]
    python pipeline/build_dictionaries.py embed
    python pipeline/build_dictionaries.py build
    python pipeline/build_dictionaries.py stability

Must run offline (developer machine / one-off worker), never in the web app.
"""

import json
import os
import pickle
import re
import sys
from collections import Counter

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline.schroders_v2_seeds import (  # noqa: E402
    SCHRODERS_V2_DIMENSIONS, SCHRODERS_V2_DIM_INFO, SCHRODERS_V2_SEEDS,
)

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pipeline_output')
os.makedirs(OUT, exist_ok=True)

DICTIONARY_VERSION = '2026-07-22-v3-generalised'
MODEL_NAME = 'sentence-transformers/all-MiniLM-L6-v2'

GENERIC_STOP_PHRASES = {
    'the company', 'this company', 'the people', 'the work', 'the job',
    'the management', 'the pay', 'the culture', 'a lot', 'lots', 'the team',
    'the environment', 'the place', 'the office', 'it', 'they', 'you', 'i',
    'we', 'them', 'us', 'the best', 'the worst', 'everything', 'nothing',
    'the staff', 'employees', 'the employees', 'people', 'work', 'company',
    'management', 'the hours', 'the benefits',
}

# Function words / generic workplace nouns that slip in via co-occurrence PMI.
STOP_TERMS = {
    'who', 'what', 'when', 'where', 'why', 'how', 'which', 'that', 'this',
    'some', 'all', 'none', 'any', 'many', 'much', 'more', 'most', 'other',
    'others', 'lot', 'lots', 'times', 'time', 'things', 'thing', 'stuff',
    'way', 'ways', 'day', 'days', 'year', 'years', 'place', 'job', 'jobs',
    'culture', 'office', 'managers', 'manager', 'staff', 'team', 'teams',
    'bonus', 'pay', 'salary', 'benefits', 'hours', 'long hours', 'no',
    'nothing', 'everything', 'everyone', 'someone', 'anyone', 'one',
    'senior management', 'upper management', 'middle management', 'firm',
    'organization', 'organisation', 'department', 'departments', 'role',
    'roles', 'colleagues', 'coworkers', 'boss', 'bosses', 'leadership',
}


def _arg(name, default=None, cast=str):
    for i, a in enumerate(sys.argv):
        if a == name and i + 1 < len(sys.argv):
            return cast(sys.argv[i + 1])
    return default


def iter_reviews(after_id=0, limit=None):
    import psycopg2
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor('review_stream')
    cur.itersize = 2000
    q = ("SELECT id, COALESCE(summary,'') || '. ' || COALESCE(pros,'') || '. ' || "
         "COALESCE(cons,'') || '. ' || COALESCE(advice_to_management,'') "
         f"FROM reviews WHERE id > {int(after_id)} ORDER BY id")
    if limit:
        q += f" LIMIT {int(limit)}"
    cur.execute(q)
    for rid, text in cur:
        if text and len(text.strip()) > 10:
            yield rid, text.strip()
    conn.close()


def compile_seed_patterns():
    pats = {}
    for dim, poles in SCHRODERS_V2_SEEDS.items():
        for pole, seeds in poles.items():
            pats[(dim, pole)] = [
                re.compile(r'\b' + r'[\s\-]+'.join(re.escape(t) for t in s.lower().split()) + r'\b')
                for s in seeds
            ]
    return pats


MINE_STATE = os.path.join(OUT, 'mine_state.pkl')


def stage_mine():
    """Resumable: processes --chunk reviews per invocation, checkpointing
    counters + last review id to pipeline_output/mine_state.pkl.
    Run repeatedly until 'MINING COMPLETE' is printed, then run with
    --finalize to write candidates.json / cooccurrence.pkl."""
    import spacy
    chunk = _arg('--chunk', 25000, int)
    min_freq = _arg('--min-freq', 10, int)
    finalize = '--finalize' in sys.argv
    seed_pats = compile_seed_patterns()

    if os.path.exists(MINE_STATE):
        with open(MINE_STATE, 'rb') as f:
            st = pickle.load(f)
        candidates, context, last_id, total = (
            st['candidates'], st['context'], st['last_id'], st['total'])
    else:
        candidates = Counter()
        context = {key: Counter() for key in seed_pats}
        last_id, total = 0, 0

    if finalize:
        out = {p: c for p, c in candidates.items()
               if c >= min_freq and p not in GENERIC_STOP_PHRASES}
        with open(os.path.join(OUT, 'candidates.json'), 'w') as f:
            json.dump(out, f)
        with open(os.path.join(OUT, 'cooccurrence.pkl'), 'wb') as f:
            pickle.dump({k: dict(v) for k, v in context.items()}, f)
        print(f"Finalized after {total} reviews -> {len(out)} candidates "
              f"(min_freq={min_freq})")
        return

    nlp = spacy.load('en_core_web_sm', disable=['ner', 'lemmatizer'])

    def extract_phrases(doc):
        out = []
        for chunk in doc.noun_chunks:
            phrase = chunk.text.lower().strip()
            if 1 <= len(phrase.split()) <= 3 and phrase.isascii() and len(phrase) > 2:
                out.append((phrase, chunk.start_char))
        for i in range(len(doc) - 1):
            if doc[i].pos_ == 'ADV' and doc[i + 1].pos_ == 'ADJ':
                out.append((f"{doc[i].text} {doc[i+1].text}".lower(), doc[i].idx))
            if doc[i].pos_ == 'ADJ' and doc[i + 1].pos_ == 'NOUN':
                out.append((f"{doc[i].text} {doc[i+1].text}".lower(), doc[i].idx))
        return out

    n = 0
    rows = list(iter_reviews(after_id=last_id, limit=chunk))
    if not rows:
        print(f"MINING COMPLETE ({total} reviews total). "
              "Run with --finalize to write outputs.")
        return
    ids = [rid for rid, _ in rows]
    for doc in nlp.pipe((t[:5000] for _, t in rows), batch_size=64):
        n += 1
        phrases = extract_phrases(doc)
        for p, _ in phrases:
            candidates[p] += 1
        # Method B: sentence-level co-occurrence with seeds
        for sent in doc.sents:
            stext = sent.text.lower()
            hit_keys = [k for k, pats in seed_pats.items() if any(p.search(stext) for p in pats)]
            if not hit_keys:
                continue
            sent_phrases = {p for p, start in phrases
                            if sent.start_char <= start < sent.end_char}
            for k in hit_keys:
                for p in sent_phrases:
                    context[k][p] += 1
        if n % 5000 == 0:
            print(f"  mined {total + n} reviews, {len(candidates)} unique phrases",
                  flush=True)

    total += n
    last_id = ids[-1]
    # prune singletons to bound memory (they can never reach min_freq soon anyway
    # only prune those far below threshold once counter grows large)
    if len(candidates) > 800000:
        candidates = Counter({p: c for p, c in candidates.items() if c > 1})
    with open(MINE_STATE, 'wb') as f:
        pickle.dump({'candidates': candidates, 'context': context,
                     'last_id': last_id, 'total': total}, f)
    print(f"Checkpoint: {total} reviews mined, {len(candidates)} unique phrases, "
          f"last_id={last_id}")


def _load_model():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer(MODEL_NAME)


def stage_embed():
    with open(os.path.join(OUT, 'candidates.json')) as f:
        candidates = json.load(f)
    phrases = sorted(candidates.keys())
    model = _load_model()
    embs = model.encode(phrases, normalize_embeddings=True, batch_size=256,
                        show_progress_bar=True)
    np.save(os.path.join(OUT, 'candidate_embs.npy'), embs)
    with open(os.path.join(OUT, 'candidate_list.json'), 'w') as f:
        json.dump(phrases, f)
    print(f"Embedded {len(phrases)} candidate phrases")


def expand_pole_a(seeds, phrases, embs, model, top_n=300, min_similarity=0.45):
    seed_embs = model.encode(seeds, normalize_embeddings=True)
    centroid = seed_embs.mean(axis=0)
    centroid /= np.linalg.norm(centroid)
    sims = embs @ centroid
    order = np.argsort(-sims)
    out = {}
    for idx in order[:top_n * 3]:
        if sims[idx] < min_similarity:
            break
        out[phrases[idx]] = float(sims[idx])
        if len(out) >= top_n:
            break
    return out


def method_b_scores(dim, pole, cooc, top_n=100, min_ct=10):
    pole_ctx = cooc.get((dim, pole), {})
    opp = 'negative' if pole == 'positive' else 'positive'
    opp_ctx = cooc.get((dim, opp), {})
    scored = {}
    for phrase, ct in pole_ctx.items():
        if ct < min_ct or phrase in GENERIC_STOP_PHRASES:
            continue
        pmi = np.log((ct + 1) / (opp_ctx.get(phrase, 0) + 1))
        if pmi > 0:
            scored[phrase] = float(pmi)
    return dict(sorted(scored.items(), key=lambda x: -x[1])[:top_n])


def method_c_antonyms(positive_terms):
    """WordNet antonyms only (negation phrases are handled by the Phase 1.2
    negation logic — adding them here would double-count)."""
    from nltk.corpus import wordnet
    variants = {}
    for term, weight in positive_terms.items():
        if len(term.split()) == 1:
            for syn in wordnet.synsets(term):
                for lemma in syn.lemmas():
                    for ant in lemma.antonyms():
                        variants[ant.name().replace('_', ' ')] = weight * 0.8
    return variants


def apply_cross_load_penalty(all_dicts):
    load = Counter()
    for terms in all_dicts.values():
        for phrase in terms:
            load[phrase] += 1
    out = {}
    for key, terms in all_dicts.items():
        out[key] = {}
        for phrase, weight in terms.items():
            ld = load[phrase]
            if ld < 4:
                out[key][phrase] = round(weight / (1 + 0.5 * (ld - 1)), 3)
    return out


def _pole_centroid(model, seeds):
    e = model.encode(seeds, normalize_embeddings=True)
    c = e.mean(axis=0)
    return c / np.linalg.norm(c)


def build_all_dicts(model, phrases, embs, cooc, seeds_map=None, cap=80,
                    min_similarity=0.40, margin=0.06, min_abs_sim=0.25,
                    max_corpus_freq=25000,
                    candidate_freqs=None, verbose=True):
    """Methods A+B+C with two extra constraints found necessary in iteration:
    - pole exclusivity: a phrase is kept only for the pole whose seed centroid
      it is closer to, by at least `margin` (fixes pos<->neg centroid overlap)
    - generic-phrase cap: phrases appearing in more than `max_corpus_freq`
      reviews are excluded unless they are seeds (fixes over-firing poles)
    """
    seeds_map = seeds_map or SCHRODERS_V2_SEEDS
    if candidate_freqs is None:
        try:
            with open(os.path.join(OUT, 'candidates.json')) as f:
                candidate_freqs = json.load(f)
        except FileNotFoundError:
            candidate_freqs = {}
    phrase_sim = {}  # phrase -> {(dim,pole): sim} for exclusivity checks
    centroids = {(dim, pole): _pole_centroid(model, seeds_map[dim][pole])
                 for dim in SCHRODERS_V2_DIMENSIONS
                 for pole in ('positive', 'negative')}
    all_dicts = {}
    for dim in SCHRODERS_V2_DIMENSIONS:
        pole_cands = {}
        for pole in ('positive', 'negative'):
            seeds = seeds_map[dim][pole]
            a = expand_pole_a(seeds, phrases, embs, model, top_n=300,
                              min_similarity=min_similarity)
            b = method_b_scores(dim, pole, cooc, top_n=100)
            combined = {}
            for p, s in a.items():
                combined[p] = combined.get(p, 0) + s
            for p, s in b.items():
                combined[p] = combined.get(p, 0) + min(1.0, max(0.0, s / 3.0))
            if pole == 'negative':
                ants = method_c_antonyms(
                    {p: w for p, w in
                     expand_pole_a(seeds_map[dim]['positive'], phrases, embs, model,
                                   top_n=100, min_similarity=0.5).items()})
                if ants:
                    ant_list = list(ants.keys())
                    ant_embs = model.encode(ant_list, normalize_embeddings=True)
                    sims_own = ant_embs @ centroids[(dim, 'negative')]
                    sims_opp = ant_embs @ centroids[(dim, 'positive')]
                    for p, so, sx in zip(ant_list, sims_own, sims_opp):
                        # antonyms must be clearly closer to the negative pole
                        if so >= 0.35 and so - sx >= margin and p not in combined:
                            combined[p] = float(ants[p]) * float(so)
            pole_cands[pole] = (combined, a, b, seeds)

        # pole-exclusivity + generic-frequency filter (with relaxed-margin
        # backfill so the two poles stay balanced — Check 1)
        pole_out = {}
        for pole in ('positive', 'negative'):
            combined, a, b, seeds = pole_cands[pole]
            opp = 'negative' if pole == 'positive' else 'positive'
            c_own, c_opp = centroids[(dim, pole)], centroids[(dim, opp)]
            kept, borderline = {}, []
            plist = [p for p in combined if p not in seeds]
            if plist:
                p_embs = model.encode(plist, normalize_embeddings=True)
                sims_own = p_embs @ c_own
                sims_opp = p_embs @ c_opp
                for p, so, sx in zip(plist, sims_own, sims_opp):
                    if p in STOP_TERMS or all(w in STOP_TERMS for w in p.split()):
                        continue  # function-word junk from PMI co-occurrence
                    if so < min_abs_sim:
                        continue  # not actually related to this pole
                    if candidate_freqs.get(p, 0) > max_corpus_freq:
                        continue  # too generic — fires on everything
                    if so - sx >= margin:
                        kept[p] = combined[p]
                    elif so - sx >= margin / 4:
                        borderline.append((p, combined[p], so - sx))
            for s in seeds:
                kept[s] = max(combined.get(s, 0), 1.0)
            pole_out[pole] = (kept, borderline, a, b)

        # backfill the thinner pole from its borderline list to restore balance
        for pole in ('positive', 'negative'):
            opp = 'negative' if pole == 'positive' else 'positive'
            kept, borderline, a, b = pole_out[pole]
            target = min(cap, len(pole_out[opp][0]))
            if len(kept) < 0.8 * target and borderline:
                for p, w, _ in sorted(borderline, key=lambda x: -x[2]):
                    kept[p] = w
                    if len(kept) >= 0.9 * target:
                        break
            all_dicts[(dim, pole)] = dict(
                sorted(kept.items(), key=lambda x: -x[1])[:cap])
            if verbose:
                print(f"  {dim} {pole}: {len(all_dicts[(dim, pole)])} terms "
                      f"(A={len(a)}, B={len(b)})", flush=True)

    # Global exclusivity: a non-seed phrase may live in only ONE (dim, pole) —
    # the one whose seed centroid it is closest to. Shared phrases were the
    # main driver of cross-dimension centroid overlap (Check 3).
    seed_sets = {k: set(seeds_map[k[0]][k[1]]) for k in all_dicts}
    phrase_locs = {}
    for key, terms in all_dicts.items():
        for p in terms:
            if p not in seed_sets[key]:
                phrase_locs.setdefault(p, []).append(key)
    shared = [p for p, locs in phrase_locs.items() if len(locs) > 1]
    if shared:
        sh_embs = model.encode(shared, normalize_embeddings=True)
        for p, e in zip(shared, sh_embs):
            best = max(phrase_locs[p], key=lambda k: float(e @ centroids[k]))
            for k in phrase_locs[p]:
                if k != best:
                    del all_dicts[k][p]

    # Rebalance after exclusivity deletions: trim the larger pole's
    # lowest-weight non-seed terms until the ratio is within [0.83, 1.2].
    for dim in SCHRODERS_V2_DIMENSIONS:
        for _ in range(200):
            np_, nn = len(all_dicts[(dim, 'positive')]), len(all_dicts[(dim, 'negative')])
            if nn and 0.83 <= np_ / nn <= 1.2:
                break
            big = (dim, 'positive') if np_ > nn else (dim, 'negative')
            trimmable = [p for p in all_dicts[big] if p not in seed_sets[big]]
            if not trimmable:
                break
            del all_dicts[big][min(trimmable, key=lambda p: all_dicts[big][p])]
    return apply_cross_load_penalty(all_dicts)


def _load_artifacts():
    with open(os.path.join(OUT, 'candidate_list.json')) as f:
        phrases = json.load(f)
    embs = np.load(os.path.join(OUT, 'candidate_embs.npy'))
    with open(os.path.join(OUT, 'cooccurrence.pkl'), 'rb') as f:
        cooc = pickle.load(f)
    return phrases, embs, cooc


def stage_build():
    phrases, embs, cooc = _load_artifacts()
    model = _load_model()
    all_dicts = build_all_dicts(model, phrases, embs, cooc)

    # Write schroders_v2_keywords.py
    path = os.path.join(os.path.dirname(OUT), 'schroders_v2_keywords.py')
    with open(path, 'w') as f:
        f.write('"""Schroders v2 12-bipole keyword dictionaries.\n'
                f'Auto-generated by pipeline/build_dictionaries.py '
                f'(version {DICTIONARY_VERSION}).\nDo not edit by hand — '
                'regenerate via the pipeline.\n"""\n\n')
        f.write(f'DICTIONARY_VERSION = {DICTIONARY_VERSION!r}\n\n')
        f.write(f'SCHRODERS_V2_DIMENSIONS = {SCHRODERS_V2_DIMENSIONS!r}\n\n')
        f.write('SCHRODERS_V2_DIM_INFO = ')
        f.write(json.dumps(SCHRODERS_V2_DIM_INFO, indent=4, ensure_ascii=False))
        f.write('\n\nSCHRODERS_V2_KEYWORDS = {\n')
        for dim in SCHRODERS_V2_DIMENSIONS:
            f.write(f'    "{dim}": {{\n')
            for pole in ('positive', 'negative'):
                f.write(f'        "{pole}": {{\n')
                for phrase, w in sorted(all_dicts[(dim, pole)].items(),
                                        key=lambda x: -x[1]):
                    f.write(f'            {phrase!r}: {w},\n')
                f.write('        },\n')
            f.write('    },\n')
        f.write('}\n')
    counts = {d: (len(all_dicts[(d, "positive")]), len(all_dicts[(d, "negative")]))
              for d in SCHRODERS_V2_DIMENSIONS}
    print(f"Wrote {path}")
    for d, (p, n) in counts.items():
        print(f"  {d}: {p} pos / {n} neg (ratio {p/max(1,n):.2f})")


def stage_stability(n_bootstrap=5):
    import random
    phrases, embs, cooc = _load_artifacts()
    model = _load_model()
    print("Seed-stability bootstrap (hold out 2 seeds per iteration):")
    report = {}
    for dim in SCHRODERS_V2_DIMENSIONS:
        for pole in ('positive', 'negative'):
            seeds = SCHRODERS_V2_SEEDS[dim][pole]
            full = set(expand_pole_a(seeds, phrases, embs, model, top_n=80))
            overlaps = []
            for _ in range(n_bootstrap):
                remaining = [s for s in seeds
                             if s not in random.sample(seeds, 2)]
                boot = set(expand_pole_a(remaining, phrases, embs, model, top_n=80))
                if full:
                    overlaps.append(len(full & boot) / len(full))
            m, s = float(np.mean(overlaps)), float(np.std(overlaps))
            report[f'{dim}_{pole}'] = {'mean_overlap': round(m, 3), 'std': round(s, 3)}
            flag = '' if m > 0.8 else (' <-- UNSTABLE (<0.7)' if m < 0.7 else ' (borderline)')
            print(f"  {dim} {pole}: {m:.2f} +/- {s:.2f}{flag}")
    with open(os.path.join(OUT, 'seed_stability.json'), 'w') as f:
        json.dump(report, f, indent=2)


if __name__ == '__main__':
    stage = sys.argv[1] if len(sys.argv) > 1 else 'all'
    if stage in ('mine', 'all'):
        stage_mine()
    if stage in ('embed', 'all'):
        stage_embed()
    if stage in ('build', 'all'):
        stage_build()
    if stage in ('stability', 'all'):
        stage_stability()
