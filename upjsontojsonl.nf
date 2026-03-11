// Minimal Nextflow to convert JSON -> JSONL
// 
params.inputfile = "${projectDir}/uniprot_results.json"
params.outdir = "${projectDir}/datalake"

process convertJsonToJsonl {
    /*
    Convert a JSON file (top-level array) to JSONL
    */
    cpus = 1
    memory = '2 GB'

    input:
    path(inputfile)

    output:
    path("output.jsonl")  // final JSONL file

    script:
    """
    set -e
    set -u

    python - << 'PY'
import json

with open('${inputfile}', 'r', encoding='utf-8') as f:
    data = json.load(f)  # top-level must be a list

with open('output.jsonl', 'w', encoding='utf-8') as out:
    for entry in data['results']:
        out.write(json.dumps(entry) + '\\n')
PY
    """
}

// Workflow
workflow {
    // Make output directory
    if (!file(params.outdir).exists()) {
        file(params.outdir).mkdirs()
    }

    // Run the process
    Channel.fromPath(params.inputfile) | convertJsonToJsonl
}
