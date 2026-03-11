#!/usr/bin/env bash
threads=1
batchsize=20 #200000
inputfile=entries_test.json
outdir="$(pwd)/datalake"
maxforks=50 # how many concurrent processes
extension=tsv #{tsv, parquet}
profile=local #{local, short, prod}
set -u
set -e
echo ".-- BEGUN $(date) --."
command="nextflow run upjson2parquet.nf --inputfile ${inputfile} --outdir ${outdir} -profile ${profile} --batchsize ${batchsize} -ansi-log true --processthreads ${threads} --extension ${extension} --maxforks ${maxforks} -resume"
echo $command
eval $command
echo "'-- ENDED $(date) --'"
