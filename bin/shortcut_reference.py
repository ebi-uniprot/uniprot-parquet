#!/usr/bin/env python3
"""
shortcut_reference.py — every shortcut column's value, computed from the
JSON entry in plain Python.

The transform derives shortcut columns in SQL (bin/parquet_transform.py).
Reconstruction (validator check 16) reads only source columns, so it cannot
see a wrong shortcut.  This module is an independent second implementation of
each shortcut: written from the column's documented meaning, deliberately not
by reusing the SQL, so one mistake does not hide another.  The validator's
check 21 and tests/test_roundtrip.py compare the lake with it.

``expected_shortcuts(entry)`` gives ``{table: [row, ...]}``.  Each row is a
dict holding that table's shortcut columns, with the rows in the lake's order
for one accession:
- child rows by ``*_index``;
- accession_map rows by accession.

``compare_rows`` compares those rows with the lake's rows.
"""

import re

# Swiss-Prot / TrEMBL as UniProt writes entryType.  Any other value has no
# expected `reviewed`: the transform refuses it, and here it is a mismatch.
ENTRY_TYPES = {
    "UniProtKB reviewed (Swiss-Prot)": True,
    "UniProtKB unreviewed (TrEMBL)": False,
}

# Taxonomic divisions, most specific first: (lineage taxon, division).
_DIVISION_RULES = (
    ("Rodentia", "rodents"), ("Mammalia", "mammals"), ("Vertebrata", "vertebrates"),
    ("Fungi", "fungi"), ("Viridiplantae", "plants"), ("Eukaryota", "invertebrates"),
    ("Bacteria", "bacteria"), ("Archaea", "archaea"), ("Viruses", "viruses"),
)

# Lists built with DuckDB's list_distinct, whose element order is not
# defined: compared as sorted lists.  Every other list compares in order.
UNORDERED = {("entries", "ec"), ("entries", "go_id"), ("entries", "xref_dbs")}

# Child table → (source array, *_index column), as the lake builds them.
_CHILDREN = {
    "features": ("features", "feature_index"),
    "xrefs": ("uniProtKBCrossReferences", "xref_index"),
    "comments": ("comments", "comment_index"),
    "publications": ("references", "reference_index"),
}

_INT = re.compile(r"[+-]?[0-9]+")


def _get(obj, *path):
    """obj[path[0]][path[1]]..., None where any step is missing."""
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _distinct(values):
    """Non-null values, first occurrence kept."""
    return list(dict.fromkeys(v for v in values if v is not None))


def _ec_numbers(names: dict) -> list:
    """EC numbers of one proteinDescription-shaped dict's naming blocks."""
    blocks = [names.get("recommendedName") or {}]
    for key in ("alternativeNames", "submissionNames", "submittedNames"):
        blocks += names.get(key) or []
    return [x.get("value") for b in blocks for x in b.get("ecNumbers") or []]


def _protein_name_primary(desc: dict):
    """Recommended name, else the first submitted name, else the first
    alternative name."""
    submitted = desc.get("submissionNames") or desc.get("submittedNames") or []
    alternative = desc.get("alternativeNames") or []
    for name in (_get(desc, "recommendedName", "fullName", "value"),
                 _get(submitted[0], "fullName", "value") if submitted else None,
                 _get(alternative[0], "fullName", "value") if alternative else None):
        if name is not None:
            return name
    return None


def _division(entry: dict) -> str:
    if _get(entry, "organism", "taxonId") == 9606:
        return "human"
    lineage = _get(entry, "organism", "lineage") or []
    return next((division for taxon, division in _DIVISION_RULES if taxon in lineage),
                "unclassified")


def _property(xref: dict, key: str):
    """The value of a cross-reference's first property named ``key``."""
    return next((p.get("value") for p in xref.get("properties") or [] if p.get("key") == key), None)


def _go_term(xref: dict) -> dict:
    """{id, aspect, term, evidence_type} of a GO cross-reference: GoTerm
    'F:ATP binding' gives aspect 'F' and term 'ATP binding'."""
    go_term = _property(xref, "GoTerm")
    return {"id": xref.get("id"),
            "aspect": None if go_term is None else go_term[:1],
            "term": None if go_term is None else go_term[2:],
            "evidence_type": _property(xref, "GoEvidenceType")}


def _pubmed_order(pmid: str):
    """Numeric ids in numeric order, then any other id in text order."""
    numeric = bool(_INT.fullmatch(pmid.strip())) and -2**63 <= int(pmid) < 2**63
    return (not numeric, int(pmid) if numeric else 0, pmid)


def _text_value(comment: dict):
    """A comment's texts[].value joined by a blank line; None when empty."""
    texts = comment.get("texts")
    if texts is None:
        return None
    return "\n\n".join(t.get("value") for t in texts if t.get("value") is not None) or None


def expected_shortcuts(entry: dict) -> dict[str, list[dict]]:
    """Every table's shortcut columns for one JSON entry, as plain Python."""
    accession = entry.get("primaryAccession")
    reviewed = ENTRY_TYPES.get(entry.get("entryType"))
    organism_id = _get(entry, "organism", "taxonId")
    organism_name = _get(entry, "organism", "scientificName")
    length = _get(entry, "sequence", "length")
    keys = {"accession": accession, "reviewed": reviewed, "organism_id": organism_id}

    desc = entry.get("proteinDescription") or {}
    genes = entry.get("genes") or []
    keywords = entry.get("keywords") or []
    xrefs = entry.get("uniProtKBCrossReferences") or []
    audit = entry.get("entryAudit") or {}
    references = entry.get("references") or []
    go_xrefs = [x for x in xrefs if x.get("database") == "GO"]
    pubmed = _distinct(c.get("id") for r in references
                       for c in _get(r, "citation", "citationCrossReferences") or []
                       if c.get("database") == "PubMed")

    ec = _ec_numbers(desc)
    for nested in ("includes", "contains"):
        for part in desc.get(nested) or []:
            ec += _ec_numbers(part)

    entries_row = {
        **keys,
        "id": entry.get("uniProtkbId"),
        "organism_name": organism_name,
        "gene_primary": [_get(g, "geneName", "value") for g in genes],
        "protein_name_primary": _protein_name_primary(desc),
        "length": length,
        "gene_synonym": [s.get("value") for g in genes for s in g.get("synonyms") or []],
        "alt_protein_names": [_get(a, "fullName", "value") for a in desc.get("alternativeNames") or []],
        "ec": _distinct(ec),
        "division": _division(entry),
        "go_id": _distinct(x.get("id") for x in go_xrefs),
        "go_terms": [_go_term(x) for x in go_xrefs],
        "xref_dbs": _distinct(x.get("database") for x in xrefs),
        "xref_proteomes": sorted(_distinct(x.get("id") for x in xrefs
                                           if x.get("database") == "Proteomes")),
        "keywordid": [k.get("id") for k in keywords],
        "keyword": [k.get("name") for k in keywords],
        "keyword_categories": [k.get("category") for k in keywords],
        "date_created": audit.get("firstPublicDate"),
        "date_modified": audit.get("lastAnnotationUpdateDate"),
        "date_sequence_modified": audit.get("lastSequenceUpdateDate"),
        "feature_count": len(entry.get("features") or []),
        "xref_count": len(xrefs),
        "comment_count": len(entry.get("comments") or []),
        "reference_count": len(references),
        "lit_pubmed_id": sorted(pubmed, key=_pubmed_order),
    }

    def children(table, extra):
        array, index = _CHILDREN[table]
        return [{**keys, index: i, **extra(element)}
                for i, element in enumerate(entry.get(array) or [])]

    out = {
        "entries": [entries_row],
        "features": children("features", lambda f: {
            "organism_name": organism_name,
            "length": length,
            "start_pos": _get(f, "location", "start", "value"),
            "end_pos": _get(f, "location", "end", "value"),
            "ligand_id": _get(f, "ligand", "id"),
            "evidence_codes": [e.get("evidenceCode") for e in f.get("evidences") or []],
        }),
        "xrefs": children("xrefs", lambda x: {}),
        "comments": children("comments", lambda c: {"text_value": _text_value(c)}),
        "publications": children("publications", lambda r: {}),
    }
    secondaries = entry.get("secondaryAccessions") or []
    out["accession_map"] = sorted(
        ({"accession": a, "primary_accession": accession, "is_primary": a == accession,
          "reviewed": reviewed, "organism_id": organism_id}
         for a in [accession, *secondaries]),
        key=lambda row: (row["accession"], row["primary_accession"]))
    return out


def _comparable(table: str, column: str, value):
    """A lake or reference value in a form both sides share: dates as ISO
    strings, NULL lists as empty (a list shortcut whose source is absent from
    the whole input is a typed NULL rather than []), unordered lists sorted."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value is None and column in _LIST_COLUMNS.get(table, ()):
        return []
    if isinstance(value, list) and (table, column) in UNORDERED:
        return sorted(value, key=lambda v: (v is None, str(v)))
    return value


_LIST_COLUMNS = {
    "entries": {"gene_primary", "gene_synonym", "alt_protein_names", "ec", "go_id", "go_terms",
                "xref_dbs", "xref_proteomes", "keywordid", "keyword", "keyword_categories",
                "lit_pubmed_id"},
    "features": {"evidence_codes"},
}


def compare_rows(table: str, accession: str, expected: list[dict],
                 actual: list[dict]) -> list[str]:
    """Mismatches between the expected shortcut rows of one accession and
    the lake's rows (each a dict holding at least the shortcut columns), as
    "table.column accession[row]: expected …, got …" strings."""
    if len(expected) != len(actual):
        return [f"{table} {accession}: expected {len(expected)} rows, got {len(actual)}"]
    problems = []
    for i, (want, got) in enumerate(zip(expected, actual)):
        where = accession if table == "entries" else f"{accession}[{i}]"
        for column, value in want.items():
            if column not in got:
                problems.append(f"{table}.{column} {where}: column missing")
                continue
            a, b = _comparable(table, column, value), _comparable(table, column, got[column])
            if a != b:
                problems.append(f"{table}.{column} {where}: expected {a!r}, got {b!r}")
    return problems


def lake_order(table: str, rows: list[dict]) -> list[dict]:
    """One accession's lake rows in the order expected_shortcuts lists them."""
    if table in _CHILDREN:
        return sorted(rows, key=lambda r: r[_CHILDREN[table][1]])
    if table == "accession_map":
        return sorted(rows, key=lambda r: (r["accession"], r["primary_accession"]))
    return list(rows)
