"""Dense, full text and hybrid retrieval over the current chunks.

Full text uses Postgres ts_rank_cd over an OR of the question's lexemes. That's in the
spirit of BM25 (term frequency, rarer terms matter, length normalised) but it is not BM25.
"""

from dataclasses import dataclass

RRF_K = 60
CANDIDATES = 50


@dataclass(frozen=True)
class Hit:
    chunk_id: str
    set_id: str
    section_code: str
    generic_name: str
    heading: str
    body: str
    score: float


SELECT = """c.chunk_id, c.set_id::text, c.section_code, l.generic_name, c.heading, c.body"""


def dense(conn, embedder, question: str, k: int = 10) -> list[Hit]:
    q = embedder.encode([question])[0]
    # exact scan. About 60k vectors fit in a few milliseconds, an HNSW index would be next at scale
    rows = conn.execute(
        f"""select {SELECT}, 1 - (v.embedding <=> %s) as score
            from chunks c join chunk_vectors v using (content_hash) join labels l using (set_id)
            order by v.embedding <=> %s, c.chunk_id limit %s""",
        (q, q, k),
    ).fetchall()
    return [Hit(*r) for r in rows]


def full_text(conn, question: str, k: int = 10) -> list[Hit]:
    rows = conn.execute(
        f"""with q as (select nullif(replace(plainto_tsquery('english', %s)::text, '&', '|'), '')::tsquery as q)
            select {SELECT}, ts_rank_cd(c.tsv, q.q, 1) as score
            from chunks c join labels l using (set_id), q
            where q.q is not null and c.tsv @@ q.q
            order by score desc, c.chunk_id limit %s""",
        (question, k),
    ).fetchall()
    return [Hit(*r) for r in rows]


def hybrid(conn, embedder, question: str, k: int = 10) -> list[Hit]:
    """Reciprocal rank fusion of the two lists. Only ranks are used, so the scores don't
    need to be on the same scale."""
    lists = [dense(conn, embedder, question, CANDIDATES), full_text(conn, question, CANDIDATES)]
    fused: dict[str, float] = {}
    by_id: dict[str, Hit] = {}
    for hits in lists:
        for rank, hit in enumerate(hits, 1):
            fused[hit.chunk_id] = fused.get(hit.chunk_id, 0.0) + 1.0 / (RRF_K + rank)
            by_id.setdefault(hit.chunk_id, hit)
    order = sorted(fused, key=lambda cid: (-fused[cid], cid))[:k]
    return [Hit(**{**by_id[cid].__dict__, "score": fused[cid]}) for cid in order]


METHODS = {
    "dense": lambda conn, emb, q, k: dense(conn, emb, q, k),
    "full_text": lambda conn, emb, q, k: full_text(conn, q, k),
    "hybrid": hybrid,
}
