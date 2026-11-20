-- one row per set id, the version the assistant is allowed to cite
create table if not exists labels (
    set_id          uuid primary key,
    version         int not null,
    document_id     uuid not null,
    effective_date  date not null,
    doc_type        text not null,
    drug_name       text not null,
    generic_name    text not null,
    labeler         text,
    status          text not null check (status in ('active', 'removed')),
    update_file     text not null,
    updated_at      timestamptz not null default now()
);

-- every version the feed has seen, so "what changed" questions have an answer
create table if not exists label_versions (
    set_id          uuid not null,
    version         int not null,
    document_id     uuid not null,
    effective_date  date not null,
    update_file     text not null,
    n_chunks        int not null,
    loaded_at       timestamptz not null default now(),
    superseded_at   timestamptz,
    primary key (set_id, version)
);

-- vectors are keyed by content hash, so identical text in two labels (common for generics)
-- or an unchanged paragraph in a new version is embedded once
create table if not exists chunk_vectors (
    content_hash    text primary key,
    model           text not null,
    embedding       vector(384) not null,
    embedded_at     timestamptz not null default now()
);

create table if not exists chunks (
    chunk_id        text primary key,
    set_id          uuid not null references labels(set_id),
    version         int not null,
    section_code    text not null,
    section_name    text not null,
    heading         text not null,
    ordinal         int not null,
    effective_date  date not null,
    content_hash    text not null references chunk_vectors(content_hash),
    body            text not null,
    tsv             tsvector generated always as (to_tsvector('english', heading || ' ' || body)) stored
);
create index if not exists chunks_set_id on chunks(set_id);
create index if not exists chunks_hash on chunks(content_hash);
create index if not exists chunks_tsv on chunks using gin(tsv);

create table if not exists tombstones (
    set_id          uuid not null,
    last_version    int,
    removed_in_file text not null,
    chunks_deleted  int not null,
    vectors_deleted int not null,
    removed_at      timestamptz not null default now(),
    primary key (set_id, removed_in_file)
);

create table if not exists quarantine (
    id              bigserial primary key,
    update_file     text not null,
    member          text not null,
    set_id          text,
    version         int,
    reason          text not null,
    detail          text,
    at              timestamptz not null default now()
);

create table if not exists load_ledger (
    update_file     text not null,
    source_md5      text not null,
    mode            text not null,
    status          text not null check (status in ('loaded', 'blocked', 'failed')),
    file_date       date,
    labels_in       int,
    labels_applied  int,
    labels_skipped  int,
    quarantined     int,
    tombstoned      int,
    chunks_added    int,
    chunks_removed  int,
    chunks_kept     int,
    embedded        int,
    seconds         numeric(10, 2),
    detail          text,
    finished_at     timestamptz not null default now(),
    primary key (update_file, source_md5)
);
