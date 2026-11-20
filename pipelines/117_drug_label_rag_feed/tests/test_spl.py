import pytest

from labelfeed import spl

from .conftest import ROSU, spl_xml


def test_parse_reads_identity_and_names():
    label = spl.parse(spl_xml(ROSU, 6))
    assert label.set_id == ROSU
    assert label.version == 6
    assert str(label.effective_date) == "2026-11-01"
    assert label.doc_type == spl.HUMAN_RX
    assert (label.drug_name, label.generic_name, label.labeler) == ("Rosuvastatin", "ROSUVASTATIN", "Test Labeler")


def test_only_kept_sections_and_no_highlights():
    xml = spl_xml(ROSU, 1, sections={
        "34067-9": ("1 INDICATIONS", ["Lowers LDL."]),
        "34089-3": ("11 DESCRIPTION", ["White tablets."]),
    })
    label = spl.parse(xml)
    assert {c.section_code for c in label.chunks} == {"34067-9"}
    assert all("highlight" not in c.body for c in label.chunks)


def test_chunk_carries_drug_and_section_in_embedded_text():
    c = spl.parse(spl_xml(ROSU, 1)).chunks[0]
    assert c.embed_text.startswith("Rosuvastatin (ROSUVASTATIN). Indications and usage. 1 INDICATIONS AND USAGE.")
    assert c.content_hash == spl.content_hash(c.embed_text)
    assert c.chunk_id == f"{ROSU}/34067-9/0/0"


def test_long_section_is_split_under_the_word_limit():
    sentence = "Monitor creatine kinase in patients with muscle pain or weakness during treatment. "
    xml = spl_xml(ROSU, 1, sections={"43685-7": ("5 WARNINGS AND PRECAUTIONS", [sentence * 60])})
    chunks = spl.parse(xml).chunks
    assert len(chunks) > 3
    assert max(len(c.body.split()) for c in chunks) <= spl.MAX_WORDS
    assert " ".join(c.body for c in chunks).split() == (sentence * 60).split()


def test_edit_in_one_subsection_leaves_other_chunks_alone():
    subs = {"43685-7": [("5.1 Myopathy", ["Risk of myopathy rises with dose."]),
                        ("5.2 Liver enzymes", ["Check liver enzymes before starting."])]}
    edited = {"43685-7": [("5.1 Myopathy", ["Risk of myopathy and rhabdomyolysis rises with dose."]),
                          ("5.2 Liver enzymes", ["Check liver enzymes before starting."])]}
    sections = {"43685-7": ("5 WARNINGS AND PRECAUTIONS", ["See subsections."])}
    before = {c.chunk_id: c.content_hash for c in spl.parse(spl_xml(ROSU, 1, sections=sections,
                                                                     subsections=subs)).chunks}
    after = {c.chunk_id: c.content_hash for c in spl.parse(spl_xml(ROSU, 2, sections=sections,
                                                                    subsections=edited)).chunks}
    changed = {k for k in before if before[k] != after[k]}
    assert changed == {f"{ROSU}/43685-7/1/0"}


def test_tables_lists_and_nested_paragraphs_are_read_once():
    xml = spl_xml(ROSU, 1, sections={"34084-4": ("6 ADVERSE REACTIONS", [
        "Table 1 lists reactions.</paragraph><table><tbody><tr><td>Headache</td><td>5.5%</td></tr>"
        "<tr><td><paragraph>Nausea</paragraph></td><td>3.4%</td></tr></tbody></table>"
        "<list><item>Myalgia<list><item>severe</item></list></item></list><paragraph>End."])})
    body = spl.parse(xml).chunks[0].body
    assert body == "Table 1 lists reactions. Headache | 5.5% Nausea | 3.4% - Myalgia severe End."


def test_warnings_nested_inside_boxed_warning_are_not_duplicated():
    inner = ('<component><section><code code="34071-1" codeSystem="2.16.840.1.113883.6.1"/>'
             "<title>WARNING</title><text><paragraph>Fatal liver injury.</paragraph></text></section></component>")
    xml = spl_xml(ROSU, 1, sections={"34066-1": ("BOXED WARNING", ["Read this first." + "</paragraph>"
                                                                   "</text>" + inner + "<text><paragraph>x"])})
    codes = [c.section_code for c in spl.parse(xml).chunks]
    assert codes and set(codes) == {"34066-1"}


@pytest.mark.parametrize("xml, reason", [
    (b"<document><unclosed>", "xml_parse_error"),
    (spl_xml(ROSU, 1).replace(b'<versionNumber value="1"/>', b'<versionNumber value="one"/>'), "bad_version"),
    (spl_xml("not-a-uuid", 1), "bad_id"),
    (spl_xml(ROSU, 1, effective="2026-11"), "bad_effective_date"),
])
def test_broken_documents_raise_with_a_reason(xml, reason):
    with pytest.raises(spl.SplError, match=reason):
        spl.parse(xml)


def test_a_section_code_used_twice_still_gives_unique_chunk_ids():
    # seen on a real fluoxetine label, a titled boxed warning and a second untitled one
    xml = spl_xml(ROSU, 1, sections={"34066-1": ("WARNING: SUICIDAL THOUGHTS", ["Monitor for worsening."])})
    second = (b'<component><section><code code="34066-1" codeSystem="2.16.840.1.113883.6.1"/>'
              b"<text><paragraph>Not approved for pediatric use.</paragraph></text></section></component>")
    xml = xml.replace(b"</structuredBody>", second + b"</structuredBody>")
    ids = [c.chunk_id for c in spl.parse(xml).chunks]
    assert len(ids) == len(set(ids)) == 2
