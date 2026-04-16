"""
Embedding-Based Keyword Expansion Pipeline — Stages 1–3
========================================================
Standalone script. Does NOT modify any existing tables, scoring modules,
or dashboard behaviour. All outputs go to keyword_expansion_output/.

Usage:
    python embedding_pipeline.py                  # run all stages
    python embedding_pipeline.py --stage 1        # centroids only
    python embedding_pipeline.py --stage 2        # vocabulary + FAISS index
    python embedding_pipeline.py --stage 3        # expansion + candidate CSVs
    python embedding_pipeline.py --model fast     # use MiniLM (5× faster, slightly lower quality)
    python embedding_pipeline.py --model best     # use mpnet (default, highest quality)
    python embedding_pipeline.py --topk 100       # candidates per pole (default 200)
    python embedding_pipeline.py --min-freq 30    # min corpus frequency (default 50)

Outputs:
    keyword_expansion_output/centroids/         — numpy centroid vectors per pole
    keyword_expansion_output/faiss_index/       — FAISS index + vocab list
    keyword_expansion_output/candidates/        — one CSV per pole, ready for human review
"""

import os
import sys
import re
import time
import pickle
import logging
import argparse
import numpy as np
import pandas as pd
from collections import Counter
from pathlib import Path

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Output directories ────────────────────────────────────────────────────────
BASE_DIR        = Path("keyword_expansion_output")
CENTROID_DIR    = BASE_DIR / "centroids"
FAISS_DIR       = BASE_DIR / "faiss_index"
CANDIDATE_DIR   = BASE_DIR / "candidates"
for d in (CENTROID_DIR, FAISS_DIR, CANDIDATE_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── Model options ─────────────────────────────────────────────────────────────
MODELS = {
    "best": "sentence-transformers/all-mpnet-base-v2",   # 768-dim, highest quality
    "fast": "sentence-transformers/all-MiniLM-L6-v2",    # 384-dim, 5× faster
}

# ── Weight tier thresholds ────────────────────────────────────────────────────
WEIGHT_TIERS = [
    (0.85, 1.00),
    (0.70, 0.75),
    (0.55, 0.50),
    (0.40, 0.25),
]

# ── Minimum evidence for low-match dimensions ─────────────────────────────────
# Execution and Innovation currently have <2% match rate, so use lower freq
SPARSE_DIMENSIONS = {"execution", "innovation"}


# ═════════════════════════════════════════════════════════════════════════════
# Seed keyword definitions
# Imported directly from culture_scoring.py so there is a single source of truth
# ═════════════════════════════════════════════════════════════════════════════

def load_seed_keywords():
    """
    Load seed keywords from culture_scoring.py and return a flat dict:
        pole_id  →  list of seed phrases
    Hofstede poles: "{dimension}__{pole_name}"   (e.g. "tight_loose__tight_control")
    MIT poles:      "mit__{dimension}"            (e.g. "mit__execution")
    """
    from culture_scoring import HOFSTEDE_DIMENSIONS, MIT_BIG_9_KEYWORDS

    poles = {}

    for dim, dim_poles in HOFSTEDE_DIMENSIONS.items():
        for pole_name, keywords in dim_poles.items():
            pole_id = f"{dim}__{pole_name}"
            poles[pole_id] = list(keywords)

    for dim, keywords in MIT_BIG_9_KEYWORDS.items():
        pole_id = f"mit__{dim}"
        poles[pole_id] = list(keywords)

    return poles


# ═════════════════════════════════════════════════════════════════════════════
# Stage 1 — Build centroid vectors
# ═════════════════════════════════════════════════════════════════════════════

def stage1_build_centroids(model_name: str):
    """
    Encode each pole's seed keywords and compute a normalised centroid vector.
    Saves one .npy file per pole to keyword_expansion_output/centroids/.
    """
    log.info("=" * 60)
    log.info("STAGE 1: Building pole centroid vectors")
    log.info(f"  Model : {model_name}")
    log.info("=" * 60)

    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(model_name)

    seeds = load_seed_keywords()
    log.info(f"  Poles to encode: {len(seeds)}")

    centroids = {}
    for pole_id, keywords in seeds.items():
        log.info(f"  Encoding {pole_id!r}  ({len(keywords)} seeds)")
        embeddings = model.encode(
            keywords,
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=64,
        )
        centroid = embeddings.mean(axis=0)
        # Re-normalise the centroid so cosine similarity = dot product
        norm = np.linalg.norm(centroid)
        if norm > 0:
            centroid = centroid / norm

        centroids[pole_id] = centroid
        np.save(CENTROID_DIR / f"{pole_id}.npy", centroid)

    log.info(f"  Saved {len(centroids)} centroid vectors to {CENTROID_DIR}/")
    return centroids


# ═════════════════════════════════════════════════════════════════════════════
# Stage 2 — Build review vocabulary and FAISS index
# ═════════════════════════════════════════════════════════════════════════════

def _get_db_connection():
    import psycopg2
    db_url = os.environ.get("DATABASE_URL", "")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(db_url)


def _extract_vocabulary(min_freq: int, batch_size: int = 10_000):
    """
    Stream review text from the database in batches, tokenise into
    unigrams and bigrams, and return a frequency Counter.
    """
    log.info("  Extracting vocabulary from review corpus (this may take several minutes)…")
    conn = _get_db_connection()
    cur  = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM reviews WHERE review_text IS NOT NULL")
    total = cur.fetchone()[0]
    log.info(f"  Total reviews to scan: {total:,}")

    counts: Counter = Counter()
    processed = 0
    offset = 0

    while True:
        cur.execute(
            "SELECT review_text FROM reviews "
            "WHERE review_text IS NOT NULL "
            "ORDER BY id LIMIT %s OFFSET %s",
            (batch_size, offset),
        )
        rows = cur.fetchall()
        if not rows:
            break

        for (text,) in rows:
            text_lower = text.lower()
            tokens = re.findall(r"\b[a-z]{3,}\b", text_lower)
            # Unigrams
            counts.update(tokens)
            # Bigrams
            bigrams = [f"{tokens[i]} {tokens[i+1]}" for i in range(len(tokens) - 1)]
            counts.update(bigrams)

        processed += len(rows)
        offset    += batch_size
        if processed % 100_000 == 0 or processed >= total:
            log.info(f"  Processed {processed:,} / {total:,} reviews …")

    conn.close()

    vocab = {term: freq for term, freq in counts.items() if freq >= min_freq}
    log.info(f"  Vocabulary size (freq ≥ {min_freq}): {len(vocab):,} terms")
    return vocab


def stage2_build_faiss_index(model_name: str, min_freq: int):
    """
    Extract the review vocabulary, encode every term, and build a
    FAISS inner-product index (equivalent to cosine similarity on
    normalised vectors).

    Saves:
        faiss_index/vocab.pkl       — ordered list of terms
        faiss_index/freq.pkl        — corresponding frequency list
        faiss_index/index.faiss     — FAISS IndexFlatIP
        faiss_index/embeddings.npy  — (N, dim) normalised embeddings
    """
    log.info("=" * 60)
    log.info("STAGE 2: Building vocabulary FAISS index")
    log.info(f"  Model     : {model_name}")
    log.info(f"  Min freq  : {min_freq}")
    log.info("=" * 60)

    import faiss
    from sentence_transformers import SentenceTransformer

    vocab_freq = _extract_vocabulary(min_freq)
    vocab_terms = list(vocab_freq.keys())
    vocab_freqs = [vocab_freq[t] for t in vocab_terms]

    log.info(f"  Encoding {len(vocab_terms):,} vocabulary terms…")
    model = SentenceTransformer(model_name)

    # Encode in chunks and show progress every 10 000 terms
    all_embeddings = []
    chunk = 2_000
    for start in range(0, len(vocab_terms), chunk):
        batch = vocab_terms[start : start + chunk]
        emb = model.encode(
            batch,
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=256,
        )
        all_embeddings.append(emb)
        done = min(start + chunk, len(vocab_terms))
        if done % 20_000 == 0 or done == len(vocab_terms):
            log.info(f"  Encoded {done:,} / {len(vocab_terms):,} terms …")

    embeddings = np.vstack(all_embeddings).astype("float32")

    dim = embeddings.shape[1]
    log.info(f"  Building FAISS index (dim={dim}, vectors={len(vocab_terms):,})…")
    index = faiss.IndexFlatIP(dim)   # inner product on normalised = cosine sim
    index.add(embeddings)

    faiss.write_index(index, str(FAISS_DIR / "index.faiss"))
    with open(FAISS_DIR / "vocab.pkl",  "wb") as f: pickle.dump(vocab_terms, f)
    with open(FAISS_DIR / "freq.pkl",   "wb") as f: pickle.dump(vocab_freqs, f)
    np.save(FAISS_DIR / "embeddings.npy", embeddings)

    log.info(f"  Saved FAISS index to {FAISS_DIR}/")
    return vocab_terms, vocab_freqs, index, embeddings


# ═════════════════════════════════════════════════════════════════════════════
# Stage 3 — Nearest-neighbour expansion + weight assignment → CSVs
# ═════════════════════════════════════════════════════════════════════════════

def _cosine_to_weight(sim: float):
    """Map cosine similarity score to weight tier (or None if below threshold)."""
    for threshold, weight in WEIGHT_TIERS:
        if sim >= threshold:
            return weight
    return None


def _load_stage2_outputs():
    """Load saved Stage 2 artefacts."""
    import faiss
    index = faiss.read_index(str(FAISS_DIR / "index.faiss"))
    with open(FAISS_DIR / "vocab.pkl", "rb") as f: vocab_terms = pickle.load(f)
    with open(FAISS_DIR / "freq.pkl",  "rb") as f: vocab_freqs = pickle.load(f)
    return vocab_terms, vocab_freqs, index


def _load_stage1_outputs():
    """Load all saved centroid .npy files."""
    centroids = {}
    for p in CENTROID_DIR.glob("*.npy"):
        pole_id = p.stem
        centroids[pole_id] = np.load(p)
    return centroids


def stage3_expand_and_weight(top_k: int = 200):
    """
    For each pole centroid, query the FAISS index for the top_k nearest
    vocabulary terms, assign cosine-derived weights, and write a CSV file
    to keyword_expansion_output/candidates/ for human review.

    Columns in each CSV:
        term                 — candidate word or phrase
        cosine_similarity    — raw similarity to the pole centroid (0–1)
        proposed_weight      — 1.0 / 0.75 / 0.50 / 0.25 (data-driven)
        corpus_frequency     — how often the term appears in reviews
        is_seed              — TRUE if already in the current keyword list
        cross_loads_to       — other poles where this term also appears (filled later)
        expert_approved      — (blank — for human to fill)
        expert_weight_override — (blank — for human to fill)
        notes                — (blank — for human to fill)
    """
    log.info("=" * 60)
    log.info("STAGE 3: Nearest-neighbour expansion + weight assignment")
    log.info(f"  Top-K candidates per pole: {top_k}")
    log.info("=" * 60)

    centroids   = _load_stage1_outputs()
    vocab_terms, vocab_freqs, index = _load_stage2_outputs()
    seeds       = load_seed_keywords()

    seed_lookup = {
        pole_id: set(k.lower() for k in keywords)
        for pole_id, keywords in seeds.items()
    }

    # Collect all candidates across poles first (for cross-loading detection)
    all_candidate_dfs = {}

    for pole_id, centroid in centroids.items():
        log.info(f"  Searching: {pole_id}")

        query = centroid.reshape(1, -1).astype("float32")
        similarities, indices = index.search(query, top_k + 50)  # slight over-fetch

        rows = []
        seen = set()
        for sim, idx in zip(similarities[0], indices[0]):
            if idx < 0 or idx >= len(vocab_terms):
                continue
            term = vocab_terms[idx]
            freq = vocab_freqs[idx]
            if term in seen:
                continue
            seen.add(term)

            weight = _cosine_to_weight(float(sim))
            if weight is None:
                continue  # below minimum threshold

            is_seed = term.lower() in seed_lookup.get(pole_id, set())

            rows.append({
                "term":                   term,
                "cosine_similarity":      round(float(sim), 4),
                "proposed_weight":        1.00 if is_seed else weight,
                "corpus_frequency":       freq,
                "is_seed":                is_seed,
                "cross_loads_to":         "",
                "expert_approved":        "",
                "expert_weight_override": "",
                "notes":                  "",
            })

            if len(rows) >= top_k:
                break

        df = pd.DataFrame(rows).sort_values("cosine_similarity", ascending=False)
        all_candidate_dfs[pole_id] = df

    # Cross-loading detection — flag terms appearing in multiple poles
    log.info("  Detecting cross-loading terms …")
    term_poles: dict[str, list[str]] = {}
    for pole_id, df in all_candidate_dfs.items():
        for term in df["term"]:
            term_poles.setdefault(term, []).append(pole_id)

    for pole_id, df in all_candidate_dfs.items():
        cross = df["term"].map(
            lambda t: "; ".join(p for p in term_poles.get(t, []) if p != pole_id)
        )
        df = df.copy()
        df["cross_loads_to"] = cross

        # Write CSV
        dim_label = pole_id.replace("__", "_").replace("mit_", "mit__")
        out_path = CANDIDATE_DIR / f"{pole_id}.csv"
        df.to_csv(out_path, index=False)
        log.info(f"  Saved {len(df):>3} candidates → {out_path.name}")
        all_candidate_dfs[pole_id] = df

    # Summary statistics
    log.info("")
    log.info("─" * 60)
    log.info("STAGE 3 SUMMARY")
    log.info("─" * 60)
    log.info(f"{'Pole':<45}  {'Candidates':>10}  {'Seeds found':>11}")
    for pole_id, df in sorted(all_candidate_dfs.items()):
        seeds_found = df["is_seed"].sum()
        log.info(f"  {pole_id:<43}  {len(df):>10}  {seeds_found:>11}")

    log.info("")
    log.info(f"  CSV files written to: {CANDIDATE_DIR}/")
    log.info("  Next step: open the CSVs, review candidates, fill in")
    log.info("  expert_approved (TRUE/FALSE) and expert_weight_override.")


# ═════════════════════════════════════════════════════════════════════════════
# Entry point
# ═════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Embedding-based keyword expansion pipeline (Stages 1–3)"
    )
    parser.add_argument(
        "--stage", type=int, choices=[1, 2, 3],
        help="Run a single stage only (default: run all)"
    )
    parser.add_argument(
        "--model", choices=["fast", "best"], default="best",
        help="Embedding model: 'fast' (MiniLM) or 'best' (mpnet, default)"
    )
    parser.add_argument(
        "--topk", type=int, default=200,
        help="Candidates to retrieve per pole (default: 200)"
    )
    parser.add_argument(
        "--min-freq", type=int, default=50,
        help="Minimum corpus frequency for vocabulary inclusion (default: 50)"
    )
    return parser.parse_args()


def main():
    args   = parse_args()
    model  = MODELS[args.model]
    stage  = args.stage
    top_k  = args.topk
    min_freq = args.min_freq

    t0 = time.time()
    log.info("Embedding keyword expansion pipeline — starting")
    log.info(f"  Model    : {model}")
    log.info(f"  Top-K    : {top_k}")
    log.info(f"  Min freq : {min_freq}")

    if stage in (None, 1):
        stage1_build_centroids(model)

    if stage in (None, 2):
        stage2_build_faiss_index(model, min_freq)

    if stage in (None, 3):
        stage3_expand_and_weight(top_k)

    elapsed = time.time() - t0
    log.info(f"Done — total time: {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
