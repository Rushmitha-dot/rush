-- Ops mirror of the HPD violations feed. The poller writes here, Debezium reads the WAL.
CREATE SCHEMA IF NOT EXISTS mirror;

CREATE TABLE IF NOT EXISTS mirror.violations (
    violationid            bigint PRIMARY KEY,
    buildingid             bigint,
    registrationid         bigint,
    boroid                 smallint,
    boro                   text,
    housenumber            text,
    streetname             text,
    zip                    text,
    apartment              text,
    story                  text,
    block                  integer,
    lot                    integer,
    class                  text NOT NULL,
    inspectiondate         date NOT NULL,
    approveddate           date,
    originalcertifybydate  date,
    originalcorrectbydate  date,
    newcertifybydate       date,
    newcorrectbydate       date,
    certifieddate          date,
    ordernumber            text,
    novid                  bigint,
    novdescription         text,
    novissueddate          date,
    currentstatusid        integer,
    currentstatus          text,
    currentstatusdate      date,
    novtype                text,
    violationstatus        text NOT NULL,
    latitude               double precision,
    longitude              double precision,
    communityboard         integer,
    councildistrict        integer,
    censustract            text,
    bin                    bigint,
    bbl                    text,
    nta                    text,
    source_updated_at      timestamptz NOT NULL,
    mirrored_at            timestamptz NOT NULL DEFAULT now(),
    row_hash               text NOT NULL
);

-- Hash of every business column, whatever columns the table has today. Built from jsonb so a column
-- added later (the drift drill) is part of the hash without touching this function.
CREATE OR REPLACE FUNCTION mirror.set_row_hash() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.row_hash := md5((to_jsonb(NEW) - 'row_hash' - 'source_updated_at' - 'mirrored_at')::text);
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS violations_row_hash ON mirror.violations;
CREATE TRIGGER violations_row_hash BEFORE INSERT OR UPDATE ON mirror.violations
    FOR EACH ROW EXECUTE FUNCTION mirror.set_row_hash();

CREATE TABLE IF NOT EXISTS mirror.quarantine (
    id                bigserial PRIMARY KEY,
    violationid       text,
    reason            text NOT NULL,
    payload           jsonb NOT NULL,
    source_updated_at text,
    seen_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mirror.poll_state (
    source        text PRIMARY KEY,
    watermark     timestamptz NOT NULL,
    last_id       bigint NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mirror.poll_runs (
    run_id        bigserial PRIMARY KEY,
    mode          text NOT NULL,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    status        text NOT NULL DEFAULT 'running',
    watermark_in  timestamptz,
    watermark_out timestamptz,
    fetched       integer DEFAULT 0,
    inserted      integer DEFAULT 0,
    updated       integer DEFAULT 0,
    unchanged     integer DEFAULT 0,
    quarantined   integer DEFAULT 0,
    pages         integer DEFAULT 0,
    note          text
);

CREATE TABLE IF NOT EXISTS mirror.heartbeat (
    id      integer PRIMARY KEY,
    beat_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS mirror.debezium_signal (
    id   varchar(64) PRIMARY KEY,
    type varchar(32) NOT NULL,
    data text
);
