"""Parse an FDA Structured Product Label into the sections the assistant answers from."""

import hashlib
import re
import uuid
from dataclasses import dataclass, field
from datetime import date

from lxml import etree

NS = {"v": "urn:hl7-org:v3"}
LOINC = "2.16.840.1.113883.6.1"
HUMAN_RX = "34391-3"

# LOINC section codes kept for retrieval. Older labels (before the 2006 PLR format) have
# separate Warnings and Precautions sections instead of the combined one.
SECTIONS = {
    "34066-1": "Boxed warning",
    "34067-9": "Indications and usage",
    "34068-7": "Dosage and administration",
    "34070-3": "Contraindications",
    "43685-7": "Warnings and precautions",
    "34071-1": "Warnings",
    "42232-9": "Precautions",
    "34084-4": "Adverse reactions",
}

# MiniLM truncates at 256 word pieces, and label text runs about 1.3 pieces per word
MAX_WORDS = 160
MIN_WORDS = 25


class SplError(ValueError):
    pass


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    section_code: str
    section_name: str
    heading: str
    ordinal: int
    body: str
    embed_text: str
    content_hash: str


@dataclass
class Label:
    set_id: str
    document_id: str
    version: int
    effective_date: date
    doc_type: str
    drug_name: str
    generic_name: str
    labeler: str | None
    chunks: list[Chunk] = field(default_factory=list)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace(" ", " ")).strip()


BLOCK_TAGS = {"paragraph", "item", "list", "br", "td", "th", "tr", "table", "caption", "title"}


def _text(el) -> str:
    """itertext() glues "Myalgia<list><item>severe" into "Myalgiasevere". Block level tags
    get a space around them, inline ones (content, sup, linkHtml) don't."""
    out = []

    def walk(node):
        if not isinstance(node.tag, str):  # comments and processing instructions
            if node.tail:
                out.append(node.tail)
            return
        block = etree.QName(node).localname in BLOCK_TAGS
        if block:
            out.append(" ")
        if node.text:
            out.append(node.text)
        for child in node:
            walk(child)
        if block:
            out.append(" ")
        if node.tail and node is not el:
            out.append(node.tail)

    walk(el)
    return _clean("".join(out))


def _attr(root, path: str, name: str) -> str | None:
    found = root.find(path, NS)
    return None if found is None else found.get(name)


def _blocks(text_el) -> list[str]:
    """Paragraphs, list items and table rows of a section's <text>, in reading order."""
    out = []
    for el in text_el.iter():
        tag = etree.QName(el).localname if isinstance(el.tag, str) else ""
        if tag == "paragraph" and not _inside(el, ("item", "table"), text_el):
            out.append(_text(el))
        elif tag == "item" and not _inside(el, ("item", "table"), text_el):
            out.append("- " + _text(el))
        elif tag == "tr":
            cells = [_text(c) for c in el if etree.QName(c).localname in ("td", "th")]
            cells = [c for c in cells if c]
            if cells:
                out.append(" | ".join(cells))
        elif tag == "caption" and not _inside(el, ("item",), text_el):
            out.append(_text(el))
    return [b for b in out if b and b != "-"]


def _inside(el, tags: tuple[str, ...], stop) -> bool:
    parent = el.getparent()
    while parent is not None and parent is not stop:
        if etree.QName(parent).localname in tags:
            return True
        parent = parent.getparent()
    return False


def _walk(section, path: list[str]):
    """Yield (heading path, blocks) for a section and each nested subsection."""
    title = section.find("v:title", NS)
    here = path + ([_text(title)] if title is not None and _text(title) else [])
    text_el = section.find("v:text", NS)
    if text_el is not None:
        blocks = _blocks(text_el)
        if blocks:
            yield here, blocks
    for sub in section.findall("v:component/v:section", NS):
        yield from _walk(sub, here)


def _pack(blocks: list[str]) -> list[str]:
    """Greedy pack blocks into pieces of at most MAX_WORDS, splitting a long block on sentences."""
    pieces, current, words = [], [], 0
    for block in blocks:
        parts = [block]
        if len(block.split()) > MAX_WORDS:
            parts = re.split(r"(?<=[.;])\s+", block)
        for part in parts:
            n = len(part.split())
            while n > MAX_WORDS:  # one run-on sentence or a giant table cell
                head = " ".join(part.split()[:MAX_WORDS])
                if current:
                    pieces.append(" ".join(current))
                    current, words = [], 0
                pieces.append(head)
                part = " ".join(part.split()[MAX_WORDS:])
                n = len(part.split())
            if words + n > MAX_WORDS and current:
                pieces.append(" ".join(current))
                current, words = [], 0
            current.append(part)
            words += n
    if current:
        # a short tail reads better glued to the previous piece than embedded alone
        if pieces and words < MIN_WORDS and len(pieces[-1].split()) + words <= MAX_WORDS + MIN_WORDS:
            pieces[-1] = pieces[-1] + " " + " ".join(current)
        else:
            pieces.append(" ".join(current))
    return pieces


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _target_sections(root):
    """Outermost sections with a kept code. A Warnings section nested inside another kept
    section belongs to its parent's chunks, not to a second copy."""
    for section in root.iterfind(".//v:structuredBody//v:section", NS):
        code = _attr(section, "v:code", "code")
        if code not in SECTIONS:
            continue
        parent = section.getparent()
        nested = False
        while parent is not None:
            if etree.QName(parent).localname == "section" and _attr(parent, "v:code", "code") in SECTIONS:
                nested = True
                break
            parent = parent.getparent()
        if not nested:
            yield code, section


def _names(root) -> tuple[str, str]:
    product = root.find(".//v:manufacturedProduct/v:manufacturedProduct", NS)
    if product is None:
        product = root.find(".//v:manufacturedProduct/v:manufacturedMedicine", NS)
    if product is None:
        return "", ""
    name = product.find("v:name", NS)
    generic = product.find("v:asEntityWithGeneric/v:genericMedicine/v:name", NS)
    drug = _text(name) if name is not None else ""
    gen = _text(generic) if generic is not None else drug
    return drug, gen


def parse(xml: bytes) -> Label:
    try:
        root = etree.fromstring(xml, parser=etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True))
    except etree.XMLSyntaxError as e:
        raise SplError(f"xml_parse_error: {e}") from e

    set_id = _attr(root, "v:setId", "root")
    document_id = _attr(root, "v:id", "root")
    version = _attr(root, "v:versionNumber", "value")
    effective = _attr(root, "v:effectiveTime", "value")
    try:
        set_id = str(uuid.UUID(set_id))
        document_id = str(uuid.UUID(document_id))
    except (TypeError, ValueError) as e:
        raise SplError(f"bad_id: set id {set_id!r}, document id {document_id!r}") from e
    if not version or not version.isdigit():
        raise SplError(f"bad_version: {version!r}")
    try:
        eff = date(int(effective[:4]), int(effective[4:6]), int(effective[6:8]))
    except (TypeError, ValueError) as e:
        raise SplError(f"bad_effective_date: {effective!r}") from e

    drug, generic = _names(root)
    labeler = root.find("v:author/v:assignedEntity/v:representedOrganization/v:name", NS)
    label = Label(
        set_id=set_id,
        document_id=document_id,
        version=int(version),
        effective_date=eff,
        doc_type=_attr(root, "v:code", "code") or "",
        drug_name=drug,
        generic_name=generic,
        labeler=_text(labeler) if labeler is not None else None,
    )
    label.chunks = chunk(label, root)
    return label


def chunk(label: Label, root) -> list[Chunk]:
    """Chunks are packed inside each subsection, so an edit in 5.3 only changes 5.3's chunks."""
    out = []
    # some labels carry the same section twice (a titled and an untitled boxed warning), so
    # the subsection counter runs per section code, not per section element
    n_by_code: dict[str, int] = {}
    for code, section in _target_sections(root):
        name = SECTIONS[code]
        for path, blocks in _walk(section, []):
            n_sub = n_by_code.get(code, 0)
            n_by_code[code] = n_sub + 1
            heading = " > ".join(path) or name
            for i, body in enumerate(_pack(blocks)):
                embed_text = f"{label.drug_name} ({label.generic_name}). {name}. {heading}. {body}"
                out.append(Chunk(
                    chunk_id=f"{label.set_id}/{code}/{n_sub}/{i}",
                    section_code=code,
                    section_name=name,
                    heading=heading,
                    ordinal=len(out),
                    body=body,
                    embed_text=embed_text,
                    content_hash=content_hash(embed_text),
                ))
    return out
