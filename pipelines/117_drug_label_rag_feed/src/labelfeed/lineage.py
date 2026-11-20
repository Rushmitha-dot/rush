"""OpenLineage run events appended to a JSON lines file. No backend, just the event shape,
so a Marquez or DataHub collector could read the same events later."""

import json
import uuid
from datetime import UTC, datetime

from .settings import Settings

PRODUCER = "https://github.com/Rushmitha-dot/rush/tree/main/pipelines/117_drug_label_rag_feed"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
OUTPUTS = ["labels", "label_versions", "chunks", "chunk_vectors", "tombstones", "quarantine"]


class Run:
    def __init__(self, settings: Settings, job: str, inputs: list[str]):
        self.settings = settings
        self.job = job
        self.inputs = inputs
        self.run_id = str(uuid.uuid4())

    def emit(self, event_type: str, facets: dict | None = None) -> None:
        event = {
            "eventType": event_type,
            "eventTime": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
            "run": {"runId": self.run_id, "facets": facets or {}},
            "job": {"namespace": "labelfeed", "name": self.job},
            "inputs": [{"namespace": "dailymed", "name": name} for name in self.inputs],
            "outputs": [{"namespace": "postgres://labelfeed", "name": f"{self.settings.schema}.{t}"}
                        for t in OUTPUTS],
        }
        path = self.settings.lineage_log
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as f:
            f.write(json.dumps(event) + "\n")
