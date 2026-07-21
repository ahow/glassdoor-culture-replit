"""Build the reviewer diagnostic pack from the scan accumulators."""
import csv
import math
import os
import pickle
import re
from collections import defaultdict

import numpy as np


def _norm(term):
    return re.sub(r'[\s\-]+', ' ', term.lower()).strip()


def build_reports(acc_file, review_csv, out_dir, dicts, dims):
    with open(acc_file, 'rb') as f:
        acc = pickle.load(f)
    n_reviews = acc['n_reviews']
    n_companies = len(acc['companies'])

    # Rebuild TERMS in identical order to the scan
    from diagnostics_export import TERMS

    # ------------------------------------------------ 1. term-level table
    path = os.path.join(out_dir, 'term_level_diagnostics.csv')
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dictionary', 'dimension', 'pole', 'term', 'normalized_term',
                    'ngram_length', 'weight', 'match_type',
                    'corpus_match_count', 'negated_match_count',
                    'document_frequency', 'document_frequency_pct',
                    'company_frequency', 'company_frequency_pct',
                    'sector_frequency', 'sectors_hit',
                    'top_cooccurring_dimensions',
                    'pmi_own_dimension', 'pmi_best_other_dimension',
                    'best_other_dimension', 'distinctiveness_pmi_margin',
                    'example_snippets'])
        for i, (dn, dim, pole, term, wt, pat) in enumerate(TERMS):
            df = acc['docfreq'].get(i, 0)
            comps = acc['compset'].get(i, set())
            sects = acc['sectorset'].get(i, set())
            cooc = acc['cooc_dim'].get(i, {})
            # PMI of term with each dimension of the same dictionary
            pmis = {}
            for (dn2, dim2), n_joint in cooc.items():
                if dn2 != dn:
                    continue
                dim_df = acc['dim_docfreq'].get((dn2, dim2), 0)
                if df and dim_df and n_joint:
                    pmis[dim2] = math.log2(
                        (n_joint / n_reviews) / ((df / n_reviews) * (dim_df / n_reviews)))
            own_pmi = pmis.get(dim)
            others = {d: p for d, p in pmis.items() if d != dim}
            best_other = max(others, key=others.get) if others else ''
            best_other_pmi = others.get(best_other) if best_other else None
            top_cooc = sorted(
                ((f"{d2}", n) for (dn2, d2), n in cooc.items()
                 if dn2 == dn and d2 != dim), key=lambda x: -x[1])[:3]
            margin = (own_pmi - best_other_pmi
                      if own_pmi is not None and best_other_pmi is not None else None)
            w.writerow([
                dn, dim, pole, term, _norm(term), len(_norm(term).split()), wt,
                'word-boundary regex, hyphen/space flexible, case-insensitive',
                acc['match'].get(i, 0), acc['negated'].get(i, 0),
                df, round(df / n_reviews * 100, 4),
                len(comps), round(len(comps) / n_companies * 100, 2),
                len(sects), '; '.join(sorted(sects)),
                '; '.join(f"{d}:{n}" for d, n in top_cooc),
                round(own_pmi, 3) if own_pmi is not None else '',
                round(best_other_pmi, 3) if best_other_pmi is not None else '',
                best_other,
                round(margin, 3) if margin is not None else '',
                ' | '.join(acc['snippets'].get(i, []))[:1000]])
    print('term_level_diagnostics.csv written')

    # ------------------------------------------- load review-level matrix
    data = {dn: {d: [] for d in dims} for dn in ('mined', 'expert')}
    ev = {dn: {d: [] for d in dims} for dn in ('mined', 'expert')}
    company_rows = defaultdict(list)  # company -> row indices
    sectors_of = {}
    dts = []
    companies_col = []
    with open(review_csv) as f:
        r = csv.reader(f)
        hdr = next(r)
        idx = {h: k for k, h in enumerate(hdr)}
        for row in r:
            companies_col.append(row[idx['company_name']])
            sectors_of[row[idx['company_name']]] = row[idx['gics_sector']]
            dts.append(row[idx['review_datetime']][:4])
            for dn in ('mined', 'expert'):
                for d in dims:
                    v = row[idx[f'{dn}_{d}_score']]
                    data[dn][d].append(float(v) if v else np.nan)
                    e = row[idx[f'{dn}_{d}_evidence']]
                    ev[dn][d].append(float(e) if e else 0.0)
    for k, comp in enumerate(companies_col):
        company_rows[comp].append(k)
    print('review CSV loaded:', len(companies_col), 'rows')

    # ------------------------------------------- 2. dimension-level summary
    def company_panel(dn, min_reviews=0, row_filter=None):
        panel = {}
        for comp, idxs in company_rows.items():
            if row_filter is not None:
                idxs = [k for k in idxs if row_filter(k)]
            if len(idxs) < min_reviews:
                continue
            means = []
            for d in dims:
                xs = [data[dn][d][k] for k in idxs
                      if not np.isnan(data[dn][d][k])]
                means.append(np.mean(xs) if xs else np.nan)
            panel[comp] = means
        return panel

    def corr_vif_pca(panel):
        M = np.array([v for v in panel.values() if not any(np.isnan(x) for x in v)])
        if len(M) < 5:
            return None, None, None, len(M)
        C = np.corrcoef(M.T)
        vifs = []
        for j in range(M.shape[1]):
            y = M[:, j]
            X = np.delete(M, j, axis=1)
            X1 = np.column_stack([np.ones(len(X)), X])
            beta, *_ = np.linalg.lstsq(X1, y, rcond=None)
            resid = y - X1 @ beta
            ss_res = (resid ** 2).sum()
            ss_tot = ((y - y.mean()) ** 2).sum()
            r2 = 1 - ss_res / ss_tot if ss_tot else 0
            vifs.append(1 / (1 - r2) if r2 < 1 else float('inf'))
        Z = (M - M.mean(0)) / np.where(M.std(0) == 0, 1, M.std(0))
        eig = np.linalg.eigvalsh(np.cov(Z.T))[::-1]
        expl = eig / eig.sum()
        return C, vifs, expl, len(M)

    with open(os.path.join(out_dir, 'dimension_level_summary.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dictionary', 'dimension', 'n_terms',
                    'reviews_hit', 'reviews_hit_pct',
                    'mean_matches_per_hit_review', 'median_matches', 'p90_matches',
                    'max_matches', 'companies_nonzero', 'companies_nonzero_pct',
                    'score_mean', 'score_std', 'score_p10', 'score_p50', 'score_p90',
                    'vif', 'pc1_loading_sign_note'])
        vif_store = {}
        for dn in ('mined', 'expert'):
            panel = company_panel(dn)
            C, vifs, expl, n_used = corr_vif_pca(panel)
            vif_store[dn] = (C, vifs, expl, n_used, panel)
            for j, d in enumerate(dims):
                hits_dist = acc['dim_hits_dist'].get((dn, d), {})
                counts = []
                for nm, cnt in hits_dist.items():
                    counts += [nm] * cnt
                counts.sort()
                dfd = acc['dim_docfreq'].get((dn, d), 0)
                scores = np.array([x for x in data[dn][d] if not np.isnan(x)])
                nz_comp = sum(
                    1 for comp, idxs in company_rows.items()
                    if any(not np.isnan(data[dn][d][k]) for k in idxs))
                nt = len(dicts[dn][d]['positive']) + len(dicts[dn][d]['negative'])
                w.writerow([
                    dn, d, nt, dfd, round(dfd / n_reviews * 100, 3),
                    round(np.mean(counts), 2) if counts else 0,
                    counts[len(counts) // 2] if counts else 0,
                    counts[int(len(counts) * 0.9)] if counts else 0,
                    counts[-1] if counts else 0,
                    nz_comp, round(nz_comp / n_companies * 100, 1),
                    round(scores.mean(), 4) if len(scores) else '',
                    round(scores.std(), 4) if len(scores) else '',
                    round(np.percentile(scores, 10), 4) if len(scores) else '',
                    round(np.percentile(scores, 50), 4) if len(scores) else '',
                    round(np.percentile(scores, 90), 4) if len(scores) else '',
                    round(vifs[j], 2) if vifs else '', ''])
    print('dimension_level_summary.csv written')

    # correlation matrices + PCA
    for dn in ('mined', 'expert'):
        C, vifs, expl, n_used, panel = vif_store[dn]
        if C is not None:
            with open(os.path.join(out_dir, f'pairwise_correlations_{dn}.csv'), 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow([''] + dims)
                for j, d in enumerate(dims):
                    w.writerow([d] + [round(x, 3) for x in C[j]])
            with open(os.path.join(out_dir, f'pca_explained_variance_{dn}.csv'), 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['component', 'explained_variance_pct', 'n_companies_used', n_used])
                for j, e in enumerate(expl):
                    w.writerow([f'PC{j+1}', round(e * 100, 2)])
    print('correlation + PCA files written')

    # bootstrap stability (company-level, 200 reps)
    rng = np.random.default_rng(42)
    with open(os.path.join(out_dir, 'bootstrap_stability.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dictionary', 'metric', 'value', 'note'])
        for dn in ('mined', 'expert'):
            panel = vif_store[dn][4]
            M = np.array([v for v in panel.values() if not any(np.isnan(x) for x in v)])
            if len(M) < 10:
                w.writerow([dn, 'skipped', '', 'too few complete companies'])
                continue
            corrs = []
            for _ in range(200):
                idx = rng.integers(0, len(M), len(M))
                Cb = np.corrcoef(M[idx].T)
                corrs.append(Cb[np.triu_indices(len(dims), 1)])
            corrs = np.array(corrs)
            w.writerow([dn, 'max_pairwise_r_std', round(float(corrs.std(0).max()), 4),
                        f'{len(M)} complete companies, 200 reps'])
            w.writerow([dn, 'mean_pairwise_r_std', round(float(corrs.std(0).mean()), 4), ''])
    print('bootstrap_stability.csv written')

    # --------------------------------------- 3. company panel side by side
    with open(os.path.join(out_dir, 'company_panel_side_by_side.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        hdr = ['company_name', 'gics_sector', 'n_reviews']
        for dn in ('mined', 'expert'):
            for d in dims:
                hdr += [f'{dn}_{d}_mean_score', f'{dn}_{d}_n_scored']
        w.writerow(hdr)
        for comp in sorted(company_rows):
            idxs = company_rows[comp]
            row = [comp, sectors_of.get(comp, ''), len(idxs)]
            for dn in ('mined', 'expert'):
                for d in dims:
                    xs = [data[dn][d][k] for k in idxs if not np.isnan(data[dn][d][k])]
                    row += [round(float(np.mean(xs)), 4) if xs else '', len(xs)]
            w.writerow(row)
    print('company_panel_side_by_side.csv written')

    # --------------------------------------- 5. duplicates / near-duplicates
    with open(os.path.join(out_dir, 'duplicate_terms.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dictionary', 'normalized_term', 'appears_in'])
        for dn in ('mined', 'expert'):
            seen = defaultdict(list)
            for d in dims:
                for pole in ('positive', 'negative'):
                    for t in dicts[dn][d][pole]:
                        seen[_norm(t)].append(f'{d}/{pole}')
            for t, places in sorted(seen.items()):
                if len(places) > 1:
                    w.writerow([dn, t, '; '.join(places)])
    print('duplicate_terms.csv written')

    # --------------------------------------- 6. top terms by contribution
    with open(os.path.join(out_dir, 'top_terms_by_contribution.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dictionary', 'dimension', 'pole', 'rank', 'term', 'weight',
                    'corpus_matches', 'weighted_contribution', 'document_frequency_pct'])
        by_pole = defaultdict(list)
        for i, (dn, dim, pole, term, wt, pat) in enumerate(TERMS):
            contrib = wt * (acc['match'].get(i, 0) + acc['negated'].get(i, 0))
            by_pole[(dn, dim, pole)].append((contrib, term, wt, acc['match'].get(i, 0),
                                             acc['docfreq'].get(i, 0)))
        for key, lst in sorted(by_pole.items()):
            for rank, (contrib, term, wt, mc, df) in enumerate(
                    sorted(lst, reverse=True)[:100], 1):
                w.writerow([key[0], key[1], key[2], rank, term, wt, mc,
                            round(contrib, 1), round(df / n_reviews * 100, 4)])
    print('top_terms_by_contribution.csv written')

    # cross-dim co-occurrence at dimension level
    with open(os.path.join(out_dir, 'dimension_cooccurrence.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dictionary', 'dim_a', 'dim_b', 'reviews_with_both',
                    'jaccard_pct'])
        for (dn, a, b), n in sorted(acc['dim_cooc'].items()):
            if a == b:
                continue
            da = acc['dim_docfreq'].get((dn, a), 0)
            db = acc['dim_docfreq'].get((dn, b), 0)
            uni = da + db - n
            w.writerow([dn, a, b, n, round(n / uni * 100, 2) if uni else 0])
    print('dimension_cooccurrence.csv written')

    # --------------------------------------- 7. sector and time slices
    years = np.array(dts)
    with open(os.path.join(out_dir, 'sector_time_slice_correlations.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dictionary', 'slice_type', 'slice_value', 'n_companies',
                    'max_abs_pairwise_r', 'mean_abs_pairwise_r'])
        sector_of_row = [sectors_of.get(c, '') for c in companies_col]
        all_sectors = sorted(set(sector_of_row))
        for dn in ('mined', 'expert'):
            for sec in all_sectors:
                flt = [k for k in range(len(companies_col)) if sector_of_row[k] == sec]
                fset = set(flt)
                panel = company_panel(dn, min_reviews=20,
                                      row_filter=lambda k: k in fset)
                C, _, _, n_used = corr_vif_pca(panel)
                if C is None:
                    w.writerow([dn, 'sector', sec, n_used, 'insufficient', ''])
                    continue
                off = np.abs(C[np.triu_indices(len(dims), 1)])
                w.writerow([dn, 'sector', sec, n_used,
                            round(float(off.max()), 3), round(float(off.mean()), 3)])
            for period, lo, hi in (('pre-2019', '0000', '2018'),
                                   ('2019-2022', '2019', '2022'),
                                   ('2023+', '2023', '9999')):
                fset = {k for k in range(len(years)) if lo <= years[k] <= hi}
                panel = company_panel(dn, min_reviews=20,
                                      row_filter=lambda k: k in fset)
                C, _, _, n_used = corr_vif_pca(panel)
                if C is None:
                    w.writerow([dn, 'period', period, n_used, 'insufficient', ''])
                    continue
                off = np.abs(C[np.triu_indices(len(dims), 1)])
                w.writerow([dn, 'period', period, n_used,
                            round(float(off.max()), 3), round(float(off.mean()), 3)])
    print('sector_time_slice_correlations.csv written')
