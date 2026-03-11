#!/bin/env python3
#Mon  9 Mar 13:46:17 GMT 2026 0.1 coded first draft
#Tue 10 Mar 11:04:33 GMT 2026 0.3 added split by taxonomy and creation of json files
#Tue 10 Mar 15:33:43 GMT 2026 1.0 added simple build_tables
#Tue 10 Mar 16:05:01 GMT 2026 1.1 added -e to choose whether writing as tsv or parquet
#Wed 11 Mar 11:24:06 GMT 2026 1.2 added strict=False due to some mixed data (float and int, e.g. "velocity" field)

#This script processes a chunk of entries in jsonl format to
#1) identify the taxonomy division of each entry
# 1.1) a custom name for specific taxid, governed by yaml configuration file, can be used as override
#2) write the lines, untouched, in separate jsonl files grouped by taxonomy division (or custom name)
#3) build a series of dataframes with specific exposed data from the jsonl
#4) write the dataframes as taxdiv.tablename files, in either parquet or tsv format

#Example call:
# upjson_processor.py entries.jsonl -o outdir [-m taxonmap]
#
#Example outputs:
# fungi.jsonl fungi.core.parquet fungi.sequence.parquet human.jsonl human.core.parquet human.sequence.parquet

#Format of taxonmap (yaml):
#taxid: custom_name
#e.g.
#559292: bakersyeast
#9615: dog

#Possible extension:
# modify write_tables to create a pool of n=threads workers and independently process the entries corresponding to each taxdivision in parallel

import re
import os
import sys
import time
import json
import yaml
import argparse
import polars as pl
from pathlib import Path
from collections import defaultdict


def eprint(*myargs, **kwargs):
    """
    Prints the provided arguments to stderr, useful for logging errors or status without cluttering stdout.

    Args:
        *myargs: Variable length argument list, elements to be printed.
        **kwargs: Arbitrary keyword arguments (e.g., end='\n').

    Returns:
        None
    """
    print(*myargs, file=sys.stderr, **kwargs)


def is_valid_file(path):
    """Check if the given path is a valid, readable file."""
    if not path:
        raise argparse.ArgumentTypeError(f"File path cannot be empty or None.")

    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"The file '{path}' does not exist.")

    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"The path '{path}' is not a valid file.")

    if not os.access(path, os.R_OK):
        raise argparse.ArgumentTypeError(f"The file '{path}' is not readable.")

    return path


def check_args():
    p = argparse.ArgumentParser()
    p.add_argument("input_jsonl", type=is_valid_file)
    p.add_argument("-m", "--taxonmap", type=is_valid_file, help="YAML custom mapping file: taxonId -> group name")
    p.add_argument("-o", "--outdir", default="datalake")
    p.add_argument("-e", "--extension", default="tsv", choices=["tsv", "parquet"])

    args = p.parse_args()

    return args


def load_taxon_map(path):
    """
    load a yaml taxon custom list
    """
    if not path:
        return {}
    with open(path) as f:
        data = yaml.safe_load(f)
    taxon_map = {int(k): v for k, v in data.items()}
    #eprint(taxon_map) #debug
    return taxon_map


def detect_group(entry, taxon_map):
    """
    detect taxon group based on taxid or lineage
    """
    taxid = entry["organism"]["taxonId"]

    if taxid in taxon_map:
        #eprint(f"{taxid} -> {taxon_map[taxid]}") #debug
        return taxon_map[taxid]

    lineage = entry["organism"]["lineage"]

    if not lineage:
        return "error"

    first = lineage[0]

    if first == "Bacteria":
        return "bacteria"
    elif first == "Archaea":
        return "archaea"
    elif first == "Viruses":
        return "viruses"
    elif "Homo sapiens" in lineage:
        return "human"
    elif first == "Eukaryota":
        if lineage[1] == "Fungi":
            return "fungi"
        elif "Rodentia" in lineage:
            return "rodents"
        elif "Mammalia" in lineage:
            return "mammals"
        elif lineage[2] == "Chordata" and lineage[3] == "Craniata":
            return "vertebrates"
        elif any(x in lineage for x in ["Viridiplantae", "Rhodophyta", "Stramenopiles", "Euglyphida", "Euglenida"]) or \
           any(re.search(r"phyceae$", x) for x in lineage):
            return "plants"
        else:
            return "invertebrates"
    return "unclassified"



def build_tables(df):
    """
    Placeholder for table construction.

    Must return:
        {tablename: polars_dataframe}
    """

    tables = {}

    #eprint(df.schema) #debug

    #retrieve values which will appear in multiple tables
    seq_length = pl.col("sequence").struct.field("length")

    #create the tables
    tables["core"] = df.select(
        [
            pl.col("primaryAccession"),
            pl.col("entryType").replace({"UniProtKB reviewed (Swiss-Prot)": "sp"}).replace({"UniProtKB unreviewed (TrEMBL)": "tr"}),
            seq_length.alias("seqLen"),
            pl.col("sequence").struct.field("molWeight")
        ]
    )
    tables["sequence"] = df.select(
        [
            pl.col("primaryAccession"),
            pl.col("sequence").struct.field("value").alias("sequence"),
            seq_length.alias("seqLen"),
            pl.col("sequence").struct.field("md5")
        ]
    )

    return tables


def write_group_jsonl(groups, outdir):
    """
    write taxonomy grouped jsonl
    """
    for group, jsonl in groups.items():
        path = outdir / f"{group}.jsonl"
        with open(path, "w") as f:
            f.write(jsonl)


def write_tables(groups, outdir, filetype="parquet"):
    """
    write parquet (or tsv) table
    """
    for group, entries in groups.items():
        df = pl.DataFrame(entries, strict=False)
        tables = build_tables(df)
        for table_name, table_df in tables.items():
            if table_df.height == 0:
                continue

            outfile = outdir / f"{group}.{table_name}.{filetype}"
            if filetype == "parquet":
                table_df.write_parquet(
                    outfile,
                    compression="zstd",
                    use_pyarrow=True
                )
            elif filetype == "tsv":
                table_df.write_csv(
                    outfile,
                    separator="\t"
                )
            else:
                raise ValueError(f"Unsupported filetype: {filetype}")


def process_chunk(args):
    """
    main process
    """

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    taxon_map = {}
    if args.taxonmap is not None:
        taxon_map = load_taxon_map(args.taxonmap)

    json_by_taxon = defaultdict(str)
    entries_by_taxon = defaultdict(list)

    with open(args.input_jsonl) as f:
        for line in f:
            entry = json.loads(line)
            accession = entry["primaryAccession"]
            group = detect_group(entry, taxon_map)
            #eprint(f"{accession} -> {group}") #debug
            json_by_taxon[group] += line
            entries_by_taxon[group].append(entry)

    write_group_jsonl(json_by_taxon, outdir) #taxdiv-grouped jsonl

    write_tables(entries_by_taxon, outdir, args.extension) #dataframes


if __name__ == "__main__":
    #initial_secs = time.time()  # for total time count
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    eprint(f" .-- BEGUN {timestamp} --.")

    args = check_args()

    process_chunk(args)

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    eprint(f" '-- ENDED {timestamp} --'")
