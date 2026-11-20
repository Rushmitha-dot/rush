"""HCPCS code family, three ways. The Python function is how it was first written. The native
column expression is what the job uses. The pandas version is there to measure the middle option.

Ranges are the AMA CPT section ranges. Level II codes (a letter then four digits) are grouped by
their first letter. CPT Category II codes end in F, Category III in T, lab PLA codes in U.
"""
import numpy as np
import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import StringType

CPT_RANGES = [  # (low, high, family), inclusive
    (100, 1999, "anesthesia"),
    (10004, 69990, "surgery"),
    (70010, 79999, "radiology"),
    (80047, 89398, "pathology_lab"),
    (90281, 99199, "medicine"),
    (99202, 99499, "evaluation_management"),
    (99500, 99607, "medicine"),
]

LEVEL2 = {
    "A": "transport_supplies", "B": "enteral_parenteral", "C": "outpatient_pps", "E": "dme", "G": "procedures_g",
    "J": "drugs", "K": "dme", "L": "orthotics_prosthetics", "M": "medical_services", "P": "pathology_lab",
    "Q": "temporary_q", "R": "radiology", "V": "vision_hearing",
}

SUFFIX = {"F": "cpt_category_2", "T": "cpt_category_3", "U": "pathology_lab", "M": "pathology_lab"}


def code_family(code: str | None) -> str | None:
    if code is None:
        return None
    c = code.strip().upper()
    if len(c) != 5:
        return "other"
    if c[0].isalpha():
        return LEVEL2.get(c[0], "level2_other") if c[1:].isdigit() else "other"
    if c[:4].isdigit() and c[4] in SUFFIX:
        return SUFFIX[c[4]]
    if not c.isdigit():
        return "other"
    n = int(c)
    for low, high, family in CPT_RANGES:
        if low <= n <= high:
            return family
    return "other"


code_family_udf = udf(code_family, StringType())


def code_family_col(code: Column) -> Column:
    c = F.upper(F.trim(code))
    first, rest, last = F.substring(c, 1, 1), F.substring(c, 2, 4), F.substring(c, 5, 1)
    n = c.cast("int")
    level2 = F.lit("level2_other")
    for letter, family in reversed(LEVEL2.items()):
        level2 = F.when(first == letter, F.lit(family)).otherwise(level2)
    suffix = F.lit("other")
    for letter, family in SUFFIX.items():
        suffix = F.when(last == letter, F.lit(family)).otherwise(suffix)
    cpt = F.lit("other")
    for low, high, family in reversed(CPT_RANGES):
        cpt = F.when(n.between(low, high), F.lit(family)).otherwise(cpt)
    return (
        F.when(code.isNull(), F.lit(None).cast("string"))
        .when(F.length(c) != 5, F.lit("other"))
        .when(first.rlike("^[A-Z]$"), F.when(rest.rlike("^[0-9]{4}$"), level2).otherwise(F.lit("other")))
        .when(F.substring(c, 1, 4).rlike("^[0-9]{4}$") & last.isin(*SUFFIX), suffix)
        .when(c.rlike("^[0-9]{5}$"), cpt)
        .otherwise(F.lit("other"))
    )


@pandas_udf(StringType())
def code_family_pandas(codes: pd.Series) -> pd.Series:
    c = codes.str.strip().str.upper()
    ok = c.str.len() == 5
    first, rest, last = c.str[0], c.str[1:], c.str[4]
    n = pd.to_numeric(c.where(c.str.fullmatch(r"[0-9]{5}", na=False)), errors="coerce")
    out = pd.Series("other", index=c.index, dtype=object)
    for low, high, family in CPT_RANGES:
        out = out.mask(n.between(low, high), family)
    is_suffix = c.str[:4].str.fullmatch(r"[0-9]{4}", na=False) & last.isin(list(SUFFIX))
    out = out.mask(is_suffix, last.map(SUFFIX))
    is_level2 = first.str.fullmatch(r"[A-Z]", na=False)
    level2 = np.where(rest.str.fullmatch(r"[0-9]{4}", na=False), first.map(LEVEL2).fillna("level2_other"), "other")
    out = out.mask(is_level2, pd.Series(level2, index=c.index))
    out = out.mask(~ok.fillna(False), "other")
    return out.mask(codes.isna(), None)
