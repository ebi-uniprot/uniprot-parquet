//Mon  9 Mar 11:42:23 GMT 2026 0.1
//Mon  9 Mar 12:13:54 GMT 2026 0.2
//Tue 10 Mar 16:21:23 GMT 2026 0.3 completed upjson_processor.py; added concatenate processes for jsonl&tsv
//Wed 11 Mar 11:13:51 GMT 2026 0.4 added simple concatenate process for parquet

/* PARAMS */
params.inputfile = "${projectDir}/entries.jsonl"
params.outdir = "${projectDir}/datalake"
params.mapping = "${projectDir}/taxanames.yaml"
params.release = "2026_01"
year = params.release.substring(0, 4)
params.processthreads = 1 // increase if processor is refactored as multithreaded
params.batchsize = 200000 // 200k entries per chunk
params.extension = "tsv"
params.maxforks = 50 //maximum number of jobs to launch in parallel (caps what specified by single processes)

def extension = params.extension

/* CONSTANTS */
def taxdivs = [ //not really needed
    "archaea",
    "bacteria",
    "fungi",
    "human",
    "invertebrates",
    "mammals",
    "plants",
    "rodents",
    "unclassified",
    "vertebrates",
    "viruses",
]

/* SCRIPTS */
process_json = "${projectDir}/bin/upjson_processor.py"

/* PROCESSES */
process processChunk {
    /*
    process a chunk of json lines and create files in the format taxdiv.tablename.extension
    and taxdiv.jsonl under outdir/
    */
    tag { "${accfile}" }
    cpus = params.processthreads
    memory = '8 GB'
    maxForks Math.min(params.maxforks ?: Integer.MAX_VALUE, 50) //min between maxForks and 50
    errorStrategy 'retry'
    maxRetries 2

    input:
    path(accfile)

    output:
    path("outdir")

    script:
    """
    set -e
    set -u
    function c1grep() {
        grep \"\$@\" || test \$? = 1
    }

    if [[ "${task.attempt}" == "1" ]]; then
        echo " .-- BEGUN ${task.process} for ${accfile}... \$(date)"
    else
        echo " .-- BEGUN ${task.process} for ${accfile}...; Attempt ${task.attempt} \$(date)"
    fi
    if [ -s "${params.mapping}" ]; then
        ${process_json} ${accfile} -o outdir -e ${extension} -m "${params.mapping}"
    else
        ${process_json} ${accfile} -o outdir -e ${extension}
    fi
    echo " '-- ENDED ${task.process} \$(date)"
    """
}

process concatenate_jsonl {
    // concatenate all ${taxdiv}.jsonl chunks together into ${outdir}/${taxdiv}.jsonl
    maxForks Math.min(params.maxforks ?: Integer.MAX_VALUE, 12) //min between maxForks and...
    cpus = 1
    memory = '2 GB'

    input:
    tuple val(taxdiv), path(files, stageAs: "?/*")

    output:
    path "${taxdiv}.jsonl"

    publishDir "${params.outdir}", mode: 'copy'

    tag { "${taxdiv}" }

    """
    set -e
    set -u
    echo " .-- BEGUN ${task.process} \$(date)"
    cat */${taxdiv}.jsonl >${taxdiv}.jsonl
    echo " '-- ENDED ${task.process} \$(date)"
    """
}

process concatenate_tsv {
    // concatenate all ${taxdiv}.${tablename} chunks together into ${outdir}/${taxdiv}/${tablename}
    // note, if number of files is too large, this could hit shell argument length limits
    // maybe better to change to use find . -maxdepth 1 -type f -name "*.${extension}" -print0
    maxForks Math.min(params.maxforks ?: Integer.MAX_VALUE, 20) //min between maxForks and..
    cpus = 1
    memory = '2 GB'

    input:
    //tuple val(taxdiv), val(tablename), path(subdirs, stageAs: "?/*")
    tuple val(taxdiv), val(tablename), path(files, stageAs: "?/*")

    output:
    path "${taxdiv}/${tablename}.${extension}"

    publishDir "${params.outdir}", mode: 'copy'

    tag { "${taxdiv}.${tablename}" }

    """
    set -e
    set -u
    echo " .-- BEGUN ${task.process} \$(date)"
    mkdir ${taxdiv}
    #cat */${taxdiv}.${tablename}.${extension} >${taxdiv}/${tablename}.${extension}

    first=1
    for f in ${files}; do
        if [ \$first -eq 1 ]; then
            cat "\$f" > ${taxdiv}/${tablename}.${extension}
            first=0
        else
            tail -n +2 "\$f" >> ${taxdiv}/${tablename}.${extension}
        fi
    done
    echo " '-- ENDED ${task.process} \$(date)"
    """
}

process concatenate_parquet {
    // concatenate all ${taxdiv}.${tablename} chunks together into ${outdir}/${taxdiv}/${tablename}
    maxForks Math.min(params.maxforks ?: Integer.MAX_VALUE, 20) //min between maxForks and..
    cpus = 1
    memory = '8 GB'

    input:
    //tuple val(taxdiv), val(tablename), path(subdirs, stageAs: "?/*")
    tuple val(taxdiv), val(tablename), path(files, stageAs: "?/*")

    output:
    path "${taxdiv}/${tablename}.${extension}"

    publishDir "${params.outdir}", mode: 'copy'

    tag { "${taxdiv}.${tablename}" }

    script:
    """
    echo " .-- BEGUN ${task.process} \$(date)"
    mkdir -p ${taxdiv}

    python - << 'PY'
    import sys
    import polars as pl
    from pathlib import Path

    files = ${files.collect{ "\"$it\"" }}

    df = pl.concat(
        [pl.read_parquet(f) for f in files],
        how="vertical"
    )

    outfile = Path("${taxdiv}") / "${tablename}.parquet"

    df.write_parquet(
        outfile,
        compression="zstd"
    )
    PY
    echo " '-- ENDED ${task.process} \$(date)"
    """
}

/* WORKFLOW */
workflow {
    println "CONFIGURATION:"
    println "inputfile: ${params.inputfile}"
    println "release: ${params.release}"
    println "outdir: ${params.outdir}"
    println "df format: ${params.extension}"
    if( file(params.mapping).exists() ) {
        println "taxamap: ${params.mapping}"
    }
    println "processthreads: ${params.processthreads}"
    println "-----"

    //0 input/output checks
    if( !file(params.inputfile).exists() ) {
        log.error "Missing input file!"
        System.exit(2)
    }

    if( !file(params.outdir).exists() ) {
        log.info "NOTICE: Creating outdir '${params.outdir}'"
        file(params.outdir).mkdirs()
    }

    //1) split input in chunks
    //e.g. entries1.json entries2.json..
    Channel
        .fromPath(params.inputfile)
        .splitText(by: params.batchsize, file: true)
        .set { chunks_ch }
    chunks_ch.count().view { n -> "total number of batches (entry file chunks): ${n}" }

    //2) process each chunk in parallel
    results_ch = chunks_ch | processChunk
    //results_ch.view() //debug

    //3) group according to taxdiv (jsonl) and taxdiv and table (dataframes)
    concat_inputs_ch = results_ch.flatMap { outdir ->
        def outputs = []
        // ---- JSONL grouped only by taxdiv ----
        file("${outdir}/*.jsonl").each { f ->
            def taxdiv = f.baseName
            outputs << tuple("jsonl", taxdiv, null, f)
        }
        // ---- PARQUET grouped by taxdiv + table ----
        file("${outdir}/*.${extension}").each { f ->
            def parts = f.baseName.tokenize('.')
            def taxdiv = parts[0]
            def tablename = parts[1]
            outputs << tuple(extension, taxdiv, tablename, f)
        }
        return outputs
    }
    jsonl_grouped_ch = concat_inputs_ch
        .filter { type, taxdiv, tablename, f -> type == "jsonl" }
        .map { type, taxdiv, tablename, f -> tuple(taxdiv, f) }
        .groupTuple(by: 0)
    df_grouped_ch = concat_inputs_ch
        .filter { type, taxdiv, tablename, f -> type == extension }
        .map { type, taxdiv, tablename, f -> tuple(taxdiv, tablename, f) }
        .groupTuple(by: [0, 1])
    //jsonl_grouped_ch.view { "JSONL -> $it" } //debug
    //df_grouped_ch.view { "PARQUET -> $it" } //debug

    //4) concatenate into final table files under taxdiv subdirs
    if (extension == "tsv" ) {
        concatenate_tsv(df_grouped_ch)
    } else if (extension == "parquet" ) {
        concatenate_parquet(df_grouped_ch)
    }

    //5) concatenate into final jsonl taxdiv files
    concatenate_jsonl(jsonl_grouped_ch)
}
