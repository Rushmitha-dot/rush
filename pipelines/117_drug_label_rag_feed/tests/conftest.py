import io
import os
import re
import uuid
import zipfile
from pathlib import Path

import psycopg
import pytest

from labelfeed import dailymed, store
from labelfeed.settings import load_settings

RX = "34391-3"
OTC = "34390-5"

ROSU = "02797697-300a-43db-92cf-9b78f08626be"
METF = "1b5e1f0c-6c2e-4b0a-9b1e-2a6a1d6e0c11"
LISI = "7f3c2a10-1d4e-4c55-8a3b-0e2f9f6b1a22"

DEFAULT_SECTIONS = {
    "34067-9": ("1 INDICATIONS AND USAGE", ["{drug} is indicated to lower LDL cholesterol in adults."]),
    "34070-3": ("4 CONTRAINDICATIONS", ["Acute liver failure or decompensated cirrhosis.",
                                        "Hypersensitivity to {drug} or any excipients."]),
    "34084-4": ("6 ADVERSE REACTIONS", ["The most common adverse reactions were headache, nausea and myalgia."]),
}


def spl_xml(set_id: str, version: int, drug: str = "Rosuvastatin", sections: dict | None = None,
            doc_type: str = RX, effective: str = "20261101", subsections: dict | None = None) -> bytes:
    """A small but structurally real SPL document. Text is written for the tests."""
    sections = DEFAULT_SECTIONS if sections is None else sections
    parts = []
    for code, (title, paragraphs) in sections.items():
        body = "".join(f"<paragraph>{p.format(drug=drug)}</paragraph>" for p in paragraphs)
        subs = ""
        for sub_title, sub_paras in (subsections or {}).get(code, []):
            sub_body = "".join(f"<paragraph>{p}</paragraph>" for p in sub_paras)
            subs += (f'<component><section><code code="42229-5" codeSystem="2.16.840.1.113883.6.1"/>'
                     f"<title>{sub_title}</title><text>{sub_body}</text></section></component>")
        parts.append(f'<component><section><id root="{uuid.uuid4()}"/>'
                     f'<code code="{code}" codeSystem="2.16.840.1.113883.6.1"/>'
                     f"<title>{title}</title><text>{body}</text>"
                     f"<excerpt><highlight><text><paragraph>highlight text is skipped</paragraph></text>"
                     f"</highlight></excerpt>{subs}</section></component>")
    doc_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{set_id}/{version}/{drug}")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="{doc_id}"/>
  <code code="{doc_type}" codeSystem="2.16.840.1.113883.6.1"/>
  <title>{drug} tablets</title>
  <effectiveTime value="{effective}"/>
  <setId root="{set_id}"/>
  <versionNumber value="{version}"/>
  <author><assignedEntity><representedOrganization><name>Test Labeler</name></representedOrganization>
  </assignedEntity></author>
  <component><structuredBody>
    <component><section><code code="48780-1" codeSystem="2.16.840.1.113883.6.1"/>
      <subject><manufacturedProduct><manufacturedProduct><name>{drug}</name>
        <asEntityWithGeneric><genericMedicine><name>{drug.upper()}</name></genericMedicine></asEntityWithGeneric>
      </manufacturedProduct></manufacturedProduct></subject></section></component>
    {"".join(parts)}
  </structuredBody></component>
</document>""".encode()


def make_update(updates_dir: Path, name: str, added: dict[str, bytes], deleted: list[str] = (),
                extra_members: dict[str, bytes] | None = None) -> Path:
    """Write an update zip shaped like DailyMed's (one inner zip per label, with an image),
    then slim it the way the pipeline does. `added` maps set id to XML."""
    raw_dir = updates_dir.parent / "incoming"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw = raw_dir / name
    stamp = "20261116"
    with zipfile.ZipFile(raw, "w") as z:
        lines = []
        for set_id, xml in added.items():
            inner = io.BytesIO()
            with zipfile.ZipFile(inner, "w") as iz:
                iz.writestr(f"{uuid.uuid4()}.xml", xml)
                iz.writestr("carton.jpg", b"\xff\xd8 not really a jpeg")
            member = f"prescription/{stamp}_{set_id}.zip"
            z.writestr(member, inner.getvalue())
            lines.append(member)
        otc = io.BytesIO()
        with zipfile.ZipFile(otc, "w") as iz:
            iz.writestr("x.xml", spl_xml(str(uuid.uuid4()), 1, doc_type=OTC))
        z.writestr(f"otc/{stamp}_{uuid.uuid4()}.zip", otc.getvalue())
        for member, data in (extra_members or {}).items():
            z.writestr(member, data)
            if member.startswith("prescription/"):
                lines.append(member)
        z.writestr("files_added.txt", "\n".join(lines + [f"otc/{stamp}_x.zip"]) + "\n")
        z.writestr("files_deleted.txt", "\n".join(f"prescription/20250101_{s}.zip" for s in deleted) + "\n")
    return dailymed.slim(raw, updates_dir)


def _db_available(dsn: str) -> bool:
    try:
        psycopg.connect(dsn, connect_timeout=2).close()
        return True
    except psycopg.OperationalError:
        return False


@pytest.fixture
def settings(tmp_path, request):
    schema = "test_" + re.sub(r"\W", "_", request.node.name[:40].lower()) + f"_{os.getpid()}"
    s = load_settings(schema=schema, data_dir=tmp_path)
    if not _db_available(s.dsn):
        pytest.skip("Postgres is not running (make up)")
    store.drop_schema(s)
    yield s
    store.drop_schema(s)


@pytest.fixture
def conn(settings):
    c = store.connect(settings)
    store.init_schema(c)
    yield c
    c.close()
