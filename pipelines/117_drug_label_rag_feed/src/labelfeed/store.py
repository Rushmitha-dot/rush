from importlib.resources import files

import psycopg
from pgvector.psycopg import register_vector

from .settings import Settings


def connect(settings: Settings, autocommit: bool = False) -> psycopg.Connection:
    conn = psycopg.connect(settings.dsn, autocommit=True)
    conn.execute("create extension if not exists vector")
    conn.execute(f'create schema if not exists "{settings.schema}"')
    conn.execute(f'set search_path to "{settings.schema}", public')
    register_vector(conn)
    conn.autocommit = autocommit
    return conn


def init_schema(conn: psycopg.Connection) -> None:
    conn.execute(files("labelfeed").joinpath("sql/schema.sql").read_text())
    conn.commit()


def drop_schema(settings: Settings) -> None:
    with psycopg.connect(settings.dsn, autocommit=True) as conn:
        conn.execute(f'drop schema if exists "{settings.schema}" cascade')
