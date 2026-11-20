"""Register and check the Debezium connector through the Kafka Connect REST API."""

import json
import os
from pathlib import Path

import requests

CONNECTOR = Path(__file__).resolve().parents[2] / "docker" / "connector.json"


def connector_config() -> dict:
    spec = json.loads(CONNECTOR.read_text())
    # the docker-connect profile reaches Postgres by service name, the host JVM by the mapped port
    spec["config"]["database.hostname"] = os.environ.get("HPD_CONNECT_PG_HOST", spec["config"]["database.hostname"])
    spec["config"]["database.port"] = os.environ.get("HPD_CONNECT_PG_PORT", spec["config"]["database.port"])
    return spec


def register(connect_url: str) -> dict:
    spec = connector_config()
    r = requests.put(f"{connect_url}/connectors/{spec['name']}/config", json=spec["config"], timeout=30)
    r.raise_for_status()
    return r.json()


def status(connect_url: str, name: str = "hpd-mirror") -> dict:
    r = requests.get(f"{connect_url}/connectors/{name}/status", timeout=10)
    r.raise_for_status()
    return r.json()


def is_running(st: dict) -> bool:
    return st["connector"]["state"] == "RUNNING" and all(t["state"] == "RUNNING" for t in st.get("tasks", []))
