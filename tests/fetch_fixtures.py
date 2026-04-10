#!/usr/bin/env python3
"""fetch_fixtures.py — Download diverse test entries from UniProtKB REST API.

Fetches multiple targeted queries to exercise every code path in the pipeline:
viruses (organismHosts), fragments, multi-gene entries, rare comment types,
submissions-only publications, ligand features, isoform xrefs, etc.

Additionally discovers the top 100 most heavily annotated entries from both
Swiss-Prot and TrEMBL by sampling candidate pools from well-studied organisms
and ranking by total annotation count (features + xrefs + comments + references).

Usage:
    python tests/fetch_fixtures.py                  # ~4K entries (default)
    python tests/fetch_fixtures.py --scale stress   # ~15K entries
    python tests/fetch_fixtures.py --force           # re-fetch even if present
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

SEARCH_URL = "https://rest.uniprot.org/uniprotkb/search"
ENTRY_URL = "https://rest.uniprot.org/uniprotkb"
MAX_PER_QUERY = 500  # UniProtKB search endpoint hard limit

SCRIPT_DIR = Path(__file__).resolve().parent
FIXTURE_DIR = SCRIPT_DIR / "fixtures"


# ── HTTP helpers ─────────────────────────────────────────────────────


def _fetch_json(url: str, retries: int = 3) -> dict | None:
    """Fetch JSON from a URL with retries on transient failures."""
    import http.client

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read())
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
            ConnectionError,
            TimeoutError,
        ) as exc:
            if attempt < retries:
                wait = attempt * 2
                print(f"    Attempt {attempt} failed ({type(exc).__name__}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    WARNING: Failed after {retries} attempts ({type(exc).__name__}: {exc}), skipping")
                return None


def fetch_search(name: str, query: str, limit: int = MAX_PER_QUERY) -> list[dict]:
    """Run a UniProtKB search query and return the results list."""
    limit = min(limit, MAX_PER_QUERY)
    print(f"  {name} (limit {limit})...", end="", flush=True)
    # UniProtKB requires brackets percent-encoded and spaces as +
    encoded_query = query.replace(" ", "+").replace("[", "%5B").replace("]", "%5D")
    url = f"{SEARCH_URL}?query={encoded_query}&format=json&size={limit}"
    data = _fetch_json(url)
    if data is None:
        print()
        return []
    results = data.get("results", [])
    print(f" {len(results)} entries")
    return results


def fetch_entry(accession: str) -> dict | None:
    """Fetch a single entry by accession."""
    url = f"{ENTRY_URL}/{accession}.json"
    return _fetch_json(url)


# ── Annotation scoring ───────────────────────────────────────────────


def annotation_count(entry: dict) -> int:
    """Total annotation volume: features + xrefs + comments + references."""
    return (
        len(entry.get("features", []))
        + len(entry.get("uniProtKBCrossReferences", []))
        + len(entry.get("comments", []))
        + len(entry.get("references", []))
    )


def annotation_summary(entry: dict) -> str:
    ft = len(entry.get("features", []))
    xr = len(entry.get("uniProtKBCrossReferences", []))
    cc = len(entry.get("comments", []))
    rf = len(entry.get("references", []))
    return f"ft={ft} xr={xr} cc={cc} rf={rf} total={ft+xr+cc+rf}"


# ── Champion discovery ───────────────────────────────────────────────


def _discover_champions(reviewed: bool, n: int = 100) -> list[dict]:
    """Sample candidate pools and return the top-N most annotated entries.

    For Swiss-Prot (reviewed=True), candidates come from:
      - Known heavy hitters by accession                  (p53, EGFR, APP, HBB, etc.)
      - Human, mouse, rat, E. coli annotation_score=5    (well-studied proteomes)
      - Viruses annotation_score=5                        (SARS-CoV-2 polyproteins)
      - Long proteins (≥5000 aa)                          (titins, polyproteins)
      - Entries with PDB structures + annotation_score=5  (structural biology)

    For TrEMBL (reviewed=False), candidates come from:
      - Human, mouse, rat, chicken, zebrafish, pig, bovine (model organisms)
      - Long TrEMBL (≥10000 aa)                          (titin orthologs)
      - TrEMBL annotation_score=5                         (best auto-annotated)
      - Viral TrEMBL                                      (polyproteins)
    """
    rev = "true" if reviewed else "false"
    label = "Swiss-Prot" if reviewed else "TrEMBL"
    print(f"\n  Discovering top {n} {label} champions...")

    pool: dict[str, dict] = {}  # acc → entry

    # ── Seed with known heavy hitters (by accession) ─────────────
    if reviewed:
        # Famously the most annotated Swiss-Prot entries (most cited,
        # most features, most xrefs). These may not appear in the first
        # 500 results from organism-based search queries.
        known_accessions = [
            "P0DTD1",  # R1AB_SARS2   — SARS-CoV-2 replicase, #1 by total annotations
            "P0DTC2",  # SPIKE_SARS2  — SARS-CoV-2 spike, massive xref count
            "P04637",  # P53_HUMAN    — most cited human protein
            "P00533",  # EGFR_HUMAN   — heavily studied oncology target
            "P05067",  # A4_HUMAN     — Alzheimer's APP, many comments/refs
            "Q8WZ42",  # TITIN_HUMAN  — largest human protein, most features
            "P68871",  # HBB_HUMAN    — hemoglobin beta, most variants
            "P69905",  # HBA_HUMAN    — hemoglobin alpha
            "P38398",  # BRCA1_HUMAN  — heavily annotated cancer gene
            "P42212",  # GFP_AEQVI    — most xref'd non-human protein
            "P01308",  # INS_HUMAN    — insulin, pharma/structural champion
            "P0C6X7",  # R1AB_SARS    — SARS-CoV-1 replicase
            "P10636",  # TAU_HUMAN    — Alzheimer's tau, many variants
            "P35555",  # FBN1_HUMAN   — Marfan syndrome, many features
            "P21359",  # NF1_HUMAN    — neurofibromatosis, many features
            "P01116",  # RASK2_HUMAN  — KRAS, heavily cited oncology
            "P04062",  # GLCM_HUMAN   — Gaucher disease, many variants
            "P00734",  # THRB_HUMAN   — thrombin, many features/xrefs
        ]
        print(f"    Seeding {len(known_accessions)} known heavy hitters...")
        for acc in known_accessions:
            entry = fetch_entry(acc)
            if entry and acc not in pool:
                pool[acc] = entry

    if reviewed:
        candidate_queries = [
            ("human_sp5",     f"(organism_id:9606) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("mouse_sp5",     f"(organism_id:10090) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("rat_sp5",       f"(organism_id:10116) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("ecoli_sp5",     f"(organism_id:83333) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("virus_sp5",     f"(taxonomy_id:10239) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("long_sp",       f"(length:[5000 TO *]) AND (reviewed:{rev})"),
            ("pdb_sp5",       f"(database:pdb) AND (reviewed:{rev}) AND (annotation_score:5) AND (length:[1000 TO *])"),
            ("drosophila_sp5", f"(organism_id:7227) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("yeast_sp5",     f"(organism_id:559292) AND (reviewed:{rev}) AND (annotation_score:5)"),
        ]
    else:
        candidate_queries = [
            ("human_tr",      f"(organism_id:9606) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("mouse_tr",      f"(organism_id:10090) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("rat_tr",        f"(organism_id:10116) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("chicken_tr",    f"(organism_id:9031) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("zebrafish_tr",  f"(organism_id:7955) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("pig_tr",        f"(organism_id:9823) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("bovine_tr",     f"(organism_id:9913) AND (reviewed:{rev}) AND (annotation_score:5)"),
            ("long_tr",       f"(length:[10000 TO *]) AND (reviewed:{rev})"),
            ("virus_tr",      f"(taxonomy_id:10239) AND (reviewed:{rev})"),
        ]

    for name, query in candidate_queries:
        entries = fetch_search(f"  [champion pool] {name}", query)
        for e in entries:
            acc = e.get("primaryAccession", "")
            if acc and acc not in pool:
                pool[acc] = e

    print(f"    Candidate pool: {len(pool)} unique entries")

    # Rank by total annotation count and keep top N
    ranked = sorted(pool.values(), key=annotation_count, reverse=True)
    champions = ranked[:n]

    if champions:
        top = champions[0]
        bot = champions[-1]
        print(f"    #{1}: {top['primaryAccession']} ({top.get('uniProtkbId','?')}) — {annotation_summary(top)}")
        print(f"    #{n}: {bot['primaryAccession']} ({bot.get('uniProtkbId','?')}) — {annotation_summary(bot)}")

    return champions


# ── Query definitions ────────────────────────────────────────────────

# Each query: (name, query_string, optional_limit)
# query_string uses natural syntax — URL-encoded at fetch time.

CORE_QUERIES: list[tuple[str, str] | tuple[str, str, int]] = [
    # Viruses (have organismHosts, unusual taxonomy)
    ("viruses",        "(taxonomy_id:10239) AND (reviewed:true)"),
    # Human Swiss-Prot (well-annotated, all comment types, GO xrefs)
    ("human_swissprot", "(organism_id:9606) AND (reviewed:true) AND (annotation_score:5)"),
    # TrEMBL Human (submissionNames, minimal annotations)
    ("trembl_human",   "(organism_id:9606) AND (reviewed:false)"),
    # TrEMBL Mouse
    ("trembl_mouse",   "(organism_id:10090) AND (reviewed:false)"),
    # TrEMBL Arabidopsis (plant kingdom)
    ("trembl_plant",   "(organism_id:3702) AND (reviewed:false)", 300),
    # Entries with isoforms (ALTERNATIVE PRODUCTS comments, isoform xrefs)
    ("isoforms",       "(cc_ap:*) AND (reviewed:true) AND (organism_id:9606)"),
    # Fragments (incomplete sequences)
    ("fragments",      "(fragment:true) AND (reviewed:true)"),
    # Bacteria (different lineage structure)
    ("bacteria",       "(taxonomy_id:2) AND (reviewed:true) AND (annotation_score:5)"),
    # Fungi (diverse kingdom)
    ("fungi",          "(taxonomy_id:4751) AND (reviewed:true)"),
    # Rare comment types (COFACTOR, INTERACTION, CATALYTIC_ACTIVITY)
    ("rare_comments",  "(cc_cofactor:*) AND (cc_interaction:*) AND (reviewed:true)"),
]

STRESS_QUERIES: list[tuple[str, str] | tuple[str, str, int]] = [
    # More TrEMBL organisms (volume + kingdom diversity)
    ("trembl_rat",        "(organism_id:10116) AND (reviewed:false)"),
    ("trembl_zebrafish",  "(organism_id:7955) AND (reviewed:false)"),
    ("trembl_drosophila", "(organism_id:7227) AND (reviewed:false)"),
    ("trembl_ecoli",      "(organism_id:83333) AND (reviewed:false)"),
    ("trembl_yeast",      "(organism_id:559292) AND (reviewed:false)"),
    ("trembl_rice",       "(organism_id:39947) AND (reviewed:false)"),
    ("trembl_chicken",    "(organism_id:9031) AND (reviewed:false)"),
    ("trembl_pig",        "(organism_id:9823) AND (reviewed:false)"),
    ("trembl_bovine",     "(organism_id:9913) AND (reviewed:false)"),
    ("trembl_rabbit",     "(organism_id:9986) AND (reviewed:false)"),
    # More Swiss-Prot from different organisms
    ("swissprot_mouse",       "(organism_id:10090) AND (reviewed:true)"),
    ("swissprot_ecoli",       "(organism_id:83333) AND (reviewed:true)"),
    ("swissprot_yeast",       "(organism_id:559292) AND (reviewed:true)"),
    ("swissprot_drosophila",  "(organism_id:7227) AND (reviewed:true)"),
    ("swissprot_arabidopsis", "(organism_id:3702) AND (reviewed:true)"),
    ("swissprot_zebrafish",   "(organism_id:7955) AND (reviewed:true)"),
    # Archaea (rare kingdom)
    ("archaea",          "(taxonomy_id:2157) AND (reviewed:true)"),
    # Very long sequences
    ("long_sequences",   "(length:[10000 TO *]) AND (reviewed:true)"),
    # Very short sequences
    ("short_sequences",  "(length:[1 TO 10]) AND (reviewed:true)"),
    # Feature-rich: very long Swiss-Prot proteins (many domains → many features)
    ("feature_rich",     "(length:[5000 TO *]) AND (reviewed:true) AND (annotation_score:5)"),
    # Xref-rich: well-studied Swiss-Prot with PDB structures (many database links)
    ("xref_rich",        "(database:pdb) AND (reviewed:true) AND (annotation_score:5) AND (length:[1000 TO *])"),
    # More virus families for organismHosts coverage
    ("viruses_trembl",   "(taxonomy_id:10239) AND (reviewed:false)"),
    # Toxins (TOXIC DOSE comment type)
    ("toxins",           "(cc_toxic_dose:*) AND (reviewed:true)"),
    # Allergens (ALLERGEN comment type)
    ("allergens",        "(cc_allergen:*) AND (reviewed:true)"),
    # Pharmaceutical entries
    ("pharmaceutical",   "(cc_pharmaceutical:*) AND (reviewed:true)"),
    # Entries with gene location (geneLocations field)
    ("gene_location",    "(organelle:*) AND (reviewed:true)"),
]


# ── Merge & deduplicate ──────────────────────────────────────────────


def merge_entries(entry_lists: list[list[dict]]) -> list[dict]:
    """Merge multiple lists of entries, deduplicating by primaryAccession."""
    seen: set[str] = set()
    merged: list[dict] = []
    for entries in entry_lists:
        for entry in entries:
            acc = entry.get("primaryAccession", "")
            if acc and acc not in seen:
                seen.add(acc)
                merged.append(entry)
    return merged


def write_fixture(entries: list[dict], output: Path) -> None:
    """Write entries to gzipped JSON in {"results": [...]} format."""
    output.parent.mkdir(parents=True, exist_ok=True)
    wrapped = {"results": entries}
    with gzip.open(output, "wt", encoding="utf-8") as f:
        json.dump(wrapped, f)
    size_mb = output.stat().st_size / 1024 / 1024
    print(f"  Written to {output} ({size_mb:.1f} MB)")


# ── Main ─────────────────────────────────────────────────────────────


def run_queries(queries: list) -> list[list[dict]]:
    """Execute a list of (name, query[, limit]) tuples, return results."""
    results = []
    for item in queries:
        name, query = item[0], item[1]
        limit = item[2] if len(item) > 2 else MAX_PER_QUERY
        entries = fetch_search(name, query, limit)
        results.append(entries)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch UniProtKB test fixtures")
    parser.add_argument(
        "--scale", choices=["default", "stress"], default="default",
        help="default (~4K entries) or stress (~15K entries)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-fetch even if fixture already exists",
    )
    args = parser.parse_args()

    if args.scale == "stress":
        output = FIXTURE_DIR / "diverse_stress.json.gz"
    else:
        output = FIXTURE_DIR / "diverse.json.gz"

    if output.exists() and not args.force:
        size = output.stat().st_size / 1024 / 1024
        print(f"Fixture already present: {output} ({size:.1f} MB)")
        print("  Use --force to re-fetch.")
        return

    print(f"Fetching diverse test fixtures from UniProtKB REST API (scale: {args.scale})...\n")

    all_results: list[list[dict]] = []

    # ── Champion entries (top 100 from each) ──────────────────────
    print("═" * 68)
    print(" Champion discovery — top 100 most annotated entries")
    print("═" * 68)

    swissprot_champions = _discover_champions(reviewed=True, n=100)
    trembl_champions = _discover_champions(reviewed=False, n=100)

    all_results.append(swissprot_champions)
    all_results.append(trembl_champions)

    # ── Core queries (always fetched) ─────────────────────────────
    print()
    print("═" * 68)
    print(" Core queries — exercise every schema-driven guard")
    print("═" * 68)
    print()

    all_results.extend(run_queries(CORE_QUERIES))

    # ── Stress-only queries ───────────────────────────────────────
    if args.scale == "stress":
        print()
        print("═" * 68)
        print(" Stress-scale additional queries")
        print("═" * 68)
        print()

        all_results.extend(run_queries(STRESS_QUERIES))

    # ── Merge, deduplicate, write ─────────────────────────────────
    print()
    print("Merging and deduplicating...")
    merged = merge_entries(all_results)
    print(f"  {len(merged)} unique entries")

    write_fixture(merged, output)

    # Summary stats
    sp = sum(1 for e in merged if "Swiss-Prot" in e.get("entryType", ""))
    tr = len(merged) - sp
    print(f"\nDone. {len(merged)} entries ({sp} Swiss-Prot, {tr} TrEMBL)")


if __name__ == "__main__":
    main()
