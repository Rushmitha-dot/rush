"""DuckDB file that stands in for the Snowflake account locally.

raw and nlp are written by Python, ops holds run history and the model version
pointer, and dbt builds staging, intermediate and marts on top.
"""
from contextlib import contextmanager
from pathlib import Path

import duckdb

DDL = """
create schema if not exists raw;
create schema if not exists nlp;
create schema if not exists ops;

create table if not exists raw.reviews (
    review_id varchar not null,
    batch_date date not null,
    parent_asin varchar not null,
    asin varchar not null,
    user_id varchar not null,
    rating double,
    title varchar,
    text varchar,
    review_ts timestamp not null,
    helpful_vote integer,
    verified_purchase boolean,
    loaded_at timestamp not null
);

create table if not exists raw.reviews_quarantine (
    batch_date date not null,
    review_id varchar,
    reason varchar not null,
    payload json,
    loaded_at timestamp not null
);

create table if not exists raw.products (
    parent_asin varchar primary key,
    title varchar,
    store varchar,
    main_category varchar,
    average_rating double,
    rating_number integer,
    price double,
    loaded_at timestamp not null
);

create table if not exists nlp.scored_reviews (
    review_id varchar not null,
    model_version varchar not null,
    n_mentions integer not null,
    scored_at timestamp not null
);

create table if not exists nlp.mentions (
    review_id varchar not null,
    model_version varchar not null,
    mention_no integer not null,
    label varchar not null,
    key varchar not null,
    matched varchar,
    sentence varchar,
    polarity double,
    scored_at timestamp not null
);

create sequence if not exists ops.run_id_seq;

create table if not exists ops.pipeline_runs (
    run_id bigint primary key default nextval('ops.run_id_seq'),
    batch_date date,
    command varchar not null,
    trigger varchar not null,
    status varchar not null,
    started_at timestamp not null default current_timestamp,
    finished_at timestamp,
    rows_in integer,
    rows_loaded integer,
    rows_quarantined integer,
    reviews_scored integer,
    seconds double,
    error varchar
);

create table if not exists ops.dq_results (
    run_id bigint not null,
    batch_date date,
    check_name varchar not null,
    severity varchar not null,
    passed boolean not null,
    observed double,
    threshold varchar,
    detail varchar
);

create table if not exists ops.load_ledger (
    batch_date date primary key,
    kind varchar not null,
    file_name varchar not null,
    md5 varchar not null,
    rows integer not null,
    loaded_at timestamp not null
);

create table if not exists ops.model_versions (
    model_version varchar primary key,
    status varchar not null,  -- candidate, active, retired
    registered_at timestamp not null default current_timestamp,
    activated_at timestamp,
    note varchar
);
"""


@contextmanager
def connect(db_path: Path, read_only: bool = False):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def init(con) -> None:
    con.execute(DDL)


def load_catalog(con, path: Path) -> int:
    """Product metadata is a static file here. A real feed would land daily
    like the reviews, this table is replaced as a whole each time it's loaded."""
    con.execute("begin")
    con.execute("delete from raw.products")
    con.execute(f"""
        insert into raw.products
        select parent_asin, any_value(title), any_value(store), any_value(main_category),
               any_value(average_rating), any_value(rating_number), any_value(try_cast(price as double)),
               current_timestamp
        from read_json('{path}', format='newline_delimited',
                       columns={{parent_asin: 'varchar', title: 'varchar', store: 'varchar',
                                 main_category: 'varchar', average_rating: 'double', rating_number: 'bigint',
                                 price: 'varchar'}})
        where parent_asin is not null
        group by parent_asin
    """)
    con.execute("commit")
    return con.execute("select count(*) from raw.products").fetchone()[0]


def active_versions(con) -> list[str]:
    """Versions that get scored on every daily run: the active one, plus a
    candidate while its backfill is in progress, so it doesn't fall behind."""
    rows = con.execute("select model_version from ops.model_versions where status in ('active', 'candidate') "
                       "order by registered_at").fetchall()
    return [r[0] for r in rows]
