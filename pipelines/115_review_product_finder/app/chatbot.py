"""Menu and button product finder. No free text on purpose: every answer the
bot gives maps to a published score, so it can't be asked something it has no
data for.

    streamlit run app/chatbot.py --server.port 8215
"""
import json
import os
from pathlib import Path

import duckdb
import streamlit as st

SERVING = Path(os.getenv("FINDER_SERVING", Path(__file__).resolve().parents[1] / "data/serving"))
TOP_N = 5

st.set_page_config(page_title="Product finder", layout="centered")


@st.cache_data(ttl=60)
def load(published_at: str):
    con = duckdb.connect()
    q = lambda name: con.execute(f"select * from read_parquet('{SERVING / name}.parquet')").df()  # noqa: E731
    return q("scores"), q("evidence"), q("concerns"), q("types")


def published() -> dict | None:
    p = SERVING / "_published.json"
    return json.loads(p.read_text()) if p.exists() else None


def say(text: str):
    with st.chat_message("assistant"):
        st.markdown(text)


def you(text: str):
    with st.chat_message("user"):
        st.markdown(text)


def reset(from_step: str):
    order = ["domain", "concern", "type"]
    for k in order[order.index(from_step):]:
        st.session_state.pop(k, None)


meta = published()
if meta is None:
    st.error("Nothing published yet. Run the pipeline first (make backfill).")
    st.stop()

scores, evidence, concerns, types = load(meta["published_at"])
st.caption(f"Scores from model {meta['model_version']}, reviews through {meta['through_batch']}")

say("Hi! Is this for your skin or your hair?")
if "domain" not in st.session_state:
    c1, c2 = st.columns(2)
    if c1.button("Skin and body", use_container_width=True):
        st.session_state.domain = "skin"
        st.rerun()
    if c2.button("Hair and scalp", use_container_width=True):
        st.session_state.domain = "hair"
        st.rerun()
    st.stop()

domain = st.session_state.domain
you("Skin and body" if domain == "skin" else "Hair and scalp")

available = scores[scores.domain == domain].concern.unique()
options = concerns[concerns.concern.isin(available)].sort_values("sort_order")
say("What's bothering you most?")
if "concern" not in st.session_state:
    cols = st.columns(2)
    for i, row in enumerate(options.itertuples()):
        if cols[i % 2].button(row.label, key=f"c_{row.concern}", use_container_width=True):
            st.session_state.concern = row.concern
            st.rerun()
    if st.button("Start over"):
        reset("domain")
        st.rerun()
    st.stop()

concern = st.session_state.concern
you(concerns.set_index("concern").label[concern])

pool = scores[(scores.domain == domain) & (scores.concern == concern)]
type_rows = types[(types.type_group == domain) & types.type_key.isin(pool.type_key.unique())].sort_values("sort_order")
say(f"And how would you describe your {'skin' if domain == 'skin' else 'hair'}?")
if "type" not in st.session_state:
    cols = st.columns(2)
    for i, row in enumerate(type_rows.itertuples()):
        if cols[i % 2].button(row.label, key=f"t_{row.type_key}", use_container_width=True):
            st.session_state.type = row.type_key
            st.rerun()
    if cols[len(type_rows) % 2].button("Not sure / any", key="t_any", use_container_width=True):
        st.session_state.type = "any"
        st.rerun()
    if st.button("Start over"):
        reset("domain")
        st.rerun()
    st.stop()

type_key = st.session_state.type
you("Not sure / any" if type_key == "any" else types.set_index("type_key").label[type_key])

top = pool[pool.type_key == type_key].sort_values("rank_in_concern").head(TOP_N)
if top.empty:
    say("I don't have enough reviews from people with that type yet. Here's what works for everyone.")
    top = pool[pool.type_key == "any"].sort_values("rank_in_concern").head(TOP_N)
    type_key = "any"
else:
    say(f"Here are the top {len(top)} by what reviewers said about this concern.")

for row in top.itertuples():
    with st.container(border=True):
        st.markdown(f"**{row.title}**")
        st.caption(f"{row.store or 'unknown brand'} · score {row.score:.0f}/100 from {row.n_reviews} reviews "
                   f"({row.positive_reviews} positive, {row.negative_reviews} negative)")
        ev = evidence[(evidence.parent_asin == row.parent_asin) & (evidence.concern == concern)
                      & (evidence.type_key == type_key)].sort_values("evidence_rank")
        for e in ev.itertuples():
            st.markdown(f"> {e.sentence}  \n<small>{int(e.rating)} stars, {e.helpful_vote} found it helpful</small>",
                        unsafe_allow_html=True)

c1, c2 = st.columns(2)
if c1.button("Try another type", use_container_width=True):
    reset("type")
    st.rerun()
if c2.button("Start over", use_container_width=True):
    reset("domain")
    st.rerun()
