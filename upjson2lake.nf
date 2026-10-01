#!/usr/bin/env nextflow

// UniProtKB JSON → JSONL.zst + sorted Parquet data lake
//
// Produces (inside <outdir>/<release>/):
//   sorted.jsonl.zst                  (reviewed+organism_id-sorted JSONL archive)
//   lake/<table>/review_status=swissprot|trembl/<table>_NNNNN.parquet
//       for entries, features, xrefs, comments, publications, accession_map
//   lake/manifest.json  datapackage.json  LICENSE  SHA256SUMS.txt  RELEASE.metalink
//   validation_report.txt / .json     (production validation; see bin/validate_lake.py)
//   SCHEMA.md                         (this release's schema, generated from the lake)
//   provenance.json                   (checksums, git commit, row counts)
//   RELEASE_COMPLETE                  (written last, only after validation passed)
//
// Architecture:
//   1. Stream JSON(.gz) → single zstd-compressed JSONL
//   2. Sort JSONL by reviewed DESC, organism_id ASC, accession ASC (Swiss-Prot first)
//   3. Transform sorted JSONL → sorted Parquet tables via DuckDB + PyArrow
//   4. Production validation against the sorted JSONL (lossless reconstruction
//      of a sample, counts, keys, sort order, checksums, partitions, ...)
//   5. SCHEMA.md, provenance and the release marker (only if validation passes)
//
// Schema: the input schema is never declared — DuckDB infers it from the whole
// dump on every run.  Source columns are UniProt's JSON keys, one per key, typed
// as the input, so a field UniProt adds is a new column, never a failure;
// shortcut columns have fixed names and declared types.
// Each release is a full rebuild.

// Written in the Nextflow strict syntax (the default parser since 26.04):
// only declarations at script level, so the release directory is a function
// and the completion handler is an onComplete: section of the entry workflow.

/* ── PARAMS ────────────────────────────────────────────────────────── */
params.inputfile      = "${projectDir}/tests/fixtures/small.json.gz"
params.outdir         = "${projectDir}/results/uniprot_parquet"   // also in nextflow.config (report paths)
params.release        = "2026_03"
params.process_memory = '96 GB'   // Total memory for heavy processes (Nextflow directive)
params.duckdb_pct     = 75        // % of process_memory allocated to DuckDB buffer pool
params.notify_email   = null      // Email for SLURM failure notifications (null = disabled)
params.duckdb_temp    = null      // DuckDB spill directory (null = use $TMPDIR or /tmp)
params.expected_count = null      // Expected entry count (from UniProt release stats, null = skip)
params.previous_manifest = null   // Previous release's lake/manifest.json: schema diff in the validation report
params.max_time       = '72h'     // Wall-time cap of a retried heavy process: at most the SLURM queue's limit

// Final output directory: <outdir>/<release>/.  Process directives cannot
// call script functions, so every publishDir spells the same path out.
// Every publishDir sets overwrite: true.  Nextflow's default is false under
// -resume, which leaves a published copy damaged by an interrupted run
// (a missing Parquet file, a truncated sorted.jsonl.zst) as it is; with
// overwrite, -resume republishes every cached output whole.
def release_dir() {
    "${params.outdir}/${params.release}"
}

// RELEASE_COMPLETE is staged under this name inside release_dir and renamed
// by the workflow's onComplete section once every publishDir copy is done.
def pending_marker() {
    "${release_dir()}/.RELEASE_COMPLETE.pending"
}

// Checks the published release directory against its own metadata
// (bin/verify_release.py: every file listed, sizes, no stray Parquet file,
// the .pending marker's checksum) before onComplete renames the marker.
// VALIDATE checked the work-directory lake, not the copy publishDir made.
// Returns null if the release is whole, else why not.  A release_dir that
// is not on the local file system (an object store) is not verified.
def verify_release_problems() {
    def dir = file(release_dir())
    if (dir.fileSystem != java.nio.file.FileSystems.default) {
        log.warn "verify_release.py not run: ${release_dir()} is not on a local file system"
        return null
    }
    def cmd = ['python3', "${projectDir}/bin/verify_release.py", dir.toString(),
               '--marker', file(pending_marker()).toString()]
    try {
        def proc = cmd.execute()
        def out = new StringBuilder()
        def err = new StringBuilder()
        proc.waitForProcessOutput(out, err)
        return proc.exitValue() == 0 ? null : "${out}${err}".trim()
    }
    catch (IOException e) {
        return "cannot run verify_release.py: ${e.message}"
    }
}

// The bin/ modules each process's scripts import.  Nextflow hashes a
// process's script text and inputs, not the modules a script imports:
// VALIDATE's validate_lake.py imports reconstruct.py, shortcut_reference.py,
// parquet_transform.py and duckdb_util.py, and without this a fix to any of
// them would be served from cache under -resume, republishing the old
// verdict.  Each process takes bin_digest(<process>) as an input, so a
// change to one of its modules re-runs it.  The lists name exactly the
// scripts' imports (tests/test_workflow.py checks them against the code):
// editing the validator never redoes a sort or a transform.
def bin_modules(String process) {
    [
        STREAM_JSONL:      [],
        SORT_JSONL:        ['duckdb_util.py'],
        PARQUET_TRANSFORM: ['duckdb_util.py'],
        VALIDATE:          ['parquet_transform.py', 'duckdb_util.py', 'reconstruct.py', 'shortcut_reference.py'],
        PROVENANCE:        ['parquet_transform.py', 'duckdb_util.py'],
    ][process]
}

// MD5 of bin_modules(process): their names and contents, in list order.
def bin_digest(String process) {
    def md = java.security.MessageDigest.getInstance('MD5')
    bin_modules(process).each { name ->
        md.update(name.bytes)
        md.update(file("${projectDir}/bin/${name}").bytes)
    }
    md.digest().encodeHex().toString()
}

def banner_line(String s) {
    "║  ${s.padRight(55)}║"
}

// OOM retry lives on the DuckDB processes themselves (not in a profile) so
// it applies under every executor.  Exit codes: 75 = DuckDB's own
// out-of-memory error (the scripts exit EX_TEMPFAIL, bin/duckdb_util.py),
// 137 = SIGKILL (OOM killer), 140 = SLURM OOM, 143 = SIGTERM (SLURM's
// pre-kill, for exceeding memory or wall time: the two cannot be told
// apart).  A retry gets params.process_memory × attempt, capped at 256 GB,
// and its base time × attempt, capped at params.max_time, so a wall-time
// kill is not retried with the same limit.  Other failures are terminal.
def oom_retry(exit_status) {
    exit_status in [75, 137, 140, 143] ? 'retry' : 'finish'
}

def scaled_memory(attempt) {
    [(params.process_memory as MemoryUnit) * attempt, 256.GB].min()
}

def scaled_time(String base, attempt) {
    [(base as Duration) * attempt, params.max_time as Duration].min()
}

// DuckDB's memory_limit: params.duckdb_pct of the task's memory, in MB (whole
// GB would truncate a small allocation to 0GB).  Evaluated per attempt, so
// DuckDB's buffer pool doubles with the retry's memory.
def duckdb_mem(MemoryUnit memory) {
    "${(memory.toBytes() * params.duckdb_pct / 100 / 1048576).longValue()}MB"
}

// Shell lines giving a DuckDB process its spill directory under
// --duckdb_temp: one per release and process, emptied before use.  An
// attempt killed by the OOM killer never runs its own cleanup, so its spill
// (up to ~1-2 TB) stays behind; the retry, or a later run of the same
// release, reclaims it here instead of adding a second one.  Only runs of
// this release and process use the directory, and those never overlap.
// '' with no --duckdb_temp: DuckDB then spills under $TMPDIR or /tmp.
// Read through a function, so the spill location is not part of a task's
// -resume hash: moving the scratch directory does not redo a sort.
def spill_setup(process) {
    if (!params.duckdb_temp) return ''
    def dir = "${params.duckdb_temp}/${params.release}.${process}"
    return "SPILL='${dir}'\n    rm -rf \"\$SPILL\"\n    mkdir -p \"\$SPILL\""
}

def spill_flag() {
    params.duckdb_temp ? '--temp-dir "$SPILL"' : ''
}

// Scripts live in bin/ which Nextflow adds to $PATH automatically.
// This allows the pipeline to run from remote URLs (e.g. GitHub).


/* ── PROCESS: Stream input to single zstd-compressed JSONL ────────── */
// Emits a sidecar entry_count.txt that VALIDATE checks the sorted JSONL and
// the entries table against — the one count taken upstream of SORT_JSONL.
// Post-hoc verification: decompresses the output and counts lines
// to catch pipe-level data loss between stream_jsonl and zstd.
// Optionally checks against params.expected_count (from release metadata).
// Accepts gzip-compressed (.gz) or plain JSON input.
process STREAM_JSONL {
    tag 'stream'
    cpus 4
    memory '4 GB'
    time '48h'

    input:
    path inputfile

    output:
    path "uniprot.jsonl.zst", emit: jsonl
    path "entry_count.txt",   emit: count_file

    script:
    def cpus = task.cpus
    def expected_flag = params.expected_count ? "--expected-count ${params.expected_count}" : ''
    def decompress = inputfile.name.endsWith('.gz') ? "pigz -dc ${inputfile}" : "cat ${inputfile}"
    """
    set -euo pipefail
    echo " .-- STREAM_JSONL BEGUN \$(date)"

    ${decompress} \
        | stream_jsonl.py --count-file entry_count.txt ${expected_flag} \
        | zstd -3 -T${cpus} -o uniprot.jsonl.zst

    # Post-hoc verification: line count of compressed output must match
    # the count stream_jsonl.py wrote.  Catches pipe-level data loss.
    EXPECTED=\$(cat entry_count.txt)
    ACTUAL=\$(zstd -dc uniprot.jsonl.zst | wc -l)
    if [ "\$EXPECTED" != "\$ACTUAL" ]; then
        echo "FATAL: JSONL line count mismatch after compression" >&2
        echo "  stream_jsonl wrote: \$EXPECTED entries" >&2
        echo "  zstd output has:    \$ACTUAL lines" >&2
        exit 1
    fi
    echo "  Verified: \$ACTUAL lines in compressed output match stream_jsonl count"

    echo " '-- STREAM_JSONL ENDED \$(date)"
    """
}


/* ── PROCESS: Sort JSONL by reviewed DESC, organism_id ASC, accession ASC ──────────────── */
// Sorts the JSONL lines, each kept byte for byte, before the Parquet
// transform.  Pre-sorted input gives the transform's ORDER BYs locality;
// each table is still sorted (and spilled) in full.
// Also the published deliverable for end users.
process SORT_JSONL {
    tag 'sort_jsonl'
    cpus 4
    memory { scaled_memory(task.attempt) }
    errorStrategy { oom_retry(task.exitStatus) }
    maxRetries 1
    time { scaled_time('24h', task.attempt) }
    disk '1 TB'          // DuckDB spill space for ORDER BY

    publishDir "${params.outdir}/${params.release}", mode: 'copy', overwrite: true

    input:
    path jsonl
    val bin_code         // bin_digest: the imported bin/ modules

    output:
    path "sorted.jsonl.zst", emit: sorted_jsonl

    script:
    """
    set -euo pipefail
    # bin/ modules: ${bin_code}
    echo " .-- SORT_JSONL BEGUN \$(date)"


    ${spill_setup(task.process)}

    sort_jsonl.py ${jsonl} \
        -o sorted.jsonl.zst \
        --memory-limit ${duckdb_mem(task.memory)} \
        --threads ${task.cpus} \
        ${spill_flag()}

    echo " '-- SORT_JSONL ENDED \$(date)"
    """
}


/* ── PROCESS: Transform sorted JSONL → Parquet tables ───────────── */
// Stages the sorted JSONL to Parquet once (single JSON parse), then all
// six table queries read from the staged Parquet — roughly 2× faster than
// re-parsing JSON for each table.  Every table has its own ORDER BY; the
// pre-sorted input gives them locality (child tables add the element's
// array position as the last key).
//
// NOTE on --skip-existing and retries:
//   --skip-existing is useful for manual re-runs (e.g. fix root cause, then
//   `nextflow run ... -resume`).  It does NOT help with the automatic OOM
//   retry below because Nextflow provisions a fresh work directory per
//   attempt — partial outputs from a failed attempt are not visible to the
//   retry.  This is a known limitation; splitting into per-table processes
//   would fix it but adds significant complexity for a rare failure mode.
//   A table is only skipped when its completion sentinel matches the files on
//   disk (see stream_to_parquet in bin/parquet_transform.py).
process PARQUET_TRANSFORM {
    tag 'transform'
    cpus 4
    memory { scaled_memory(task.attempt) }
    errorStrategy { oom_retry(task.exitStatus) }
    maxRetries 1
    time { scaled_time('48h', task.attempt) }
    disk '2 TB'          // DuckDB ORDER BY spill + Parquet output.  Every table is sorted in
                         // full (DuckDB does not reuse the input's order): child tables by
                         // (reviewed DESC, organism_id, accession, <x>_index).  xrefs (~5B rows) and
                         // features spill the most, one table at a time.

    publishDir "${params.outdir}/${params.release}", mode: 'copy', overwrite: true, pattern: 'lake'

    input:
    path sorted_jsonl
    val bin_code         // bin_digest: the imported bin/ modules

    output:
    path "lake", emit: lake

    script:
    """
    set -euo pipefail
    # bin/ modules: ${bin_code}
    echo " .-- PARQUET_TRANSFORM BEGUN \$(date)"

    ${spill_setup(task.process)}

    parquet_transform.py ${sorted_jsonl} \
        --outdir ./lake \
        --memory-limit ${duckdb_mem(task.memory)} \
        --threads ${task.cpus} \
        --release ${params.release} \
        --skip-existing \
        ${spill_flag()}

    # The completion sentinels only serve a manual --skip-existing re-run of
    # this directory (a retry gets a fresh work directory): not published.
    rm -rf lake/.complete

    echo " '-- PARQUET_TRANSFORM ENDED \$(date)"
    """
}


/* ── PROCESS: Production validation of the Parquet data lake ─────── */
// Validates completeness, uniqueness, referential integrity, sort order and
// array positions, lossless reconstruction of a sample against the source
// JSONL, Parquet file integrity, manifest and checksums, partitions.
// Exits 1 on ANY failure.  Uses the sorted JSONL as ground truth: its lines are
// STREAM_JSONL's, byte for byte, each holding its dump entry's exact data (not
// DuckDB's parse of it), anchored to entry_count.txt so a row lost in SORT_JSONL
// is caught.
//
// With --previous_manifest, the report also lists how this release's schema
// differs from the previous one (tables / columns added, removed, retyped).
// That section is informational and never fails: source columns follow
// UniProt's JSON by design.
//
// At full scale it decompresses the whole JSONL once, hashes every Parquet
// file (cpus at a time) and runs DuckDB joins over the child tables (xrefs
// ~5B rows), so it gets the heavy processes' memory, OOM retry and spill
// directory, and DuckDB is sized to the task.  Not yet timed on a full
// release: time is generous.
process VALIDATE {
    tag 'validate'
    cpus 8
    memory { scaled_memory(task.attempt) }
    errorStrategy { oom_retry(task.exitStatus) }
    maxRetries 1
    time { scaled_time('24h', task.attempt) }

    publishDir "${params.outdir}/${params.release}", mode: 'copy', overwrite: true

    input:
    path lake
    path sorted_jsonl
    path count_file
    path previous_manifest   // [] when unset; staged, so -resume hashes its path, size and mtime (default cache mode)
    val bin_code             // bin_digest: the imported bin/ modules

    output:
    path "validation_report.txt", emit: report
    path "validation_report.json", emit: report_json

    script:
    def previous_flag = previous_manifest ? "--previous-manifest '${previous_manifest}'" : ''
    """
    set -euo pipefail
    # bin/ modules: ${bin_code}
    echo " .-- VALIDATE BEGUN \$(date)"

    ${spill_setup(task.process)}

    validate_lake.py \
        --lake ${lake} \
        --jsonl ${sorted_jsonl} \
        --expected-count \$(cat ${count_file}) \
        --spot-check-n 1000 \
        --release \
        --memory-limit ${duckdb_mem(task.memory)} \
        --threads ${task.cpus} \
        ${spill_flag()} \
        ${previous_flag} \
        -o validation_report.txt

    echo " '-- VALIDATE ENDED \$(date)"
    """
}


/* ── PROCESS: SCHEMA.md, provenance manifest, release marker ──────── */
// PROVENANCE depends on VALIDATE (it takes the validation reports, which
// exist only if validation passed) so that it is only published for a lake
// that passed.  It records the size and SHA-256 of every file published
// beside lake/, which verify_release.py checks before the marker is written.  SCHEMA.md is
// generated from this build's manifest: source columns follow the input,
// so the schema document is per release, like the schema.
process PROVENANCE {
    tag 'provenance'
    cpus 1
    memory '1 GB'

    // RELEASE_COMPLETE must not appear under its final name through
    // publishDir: publishDir copies run asynchronously, so this tiny marker
    // would land in release_dir while PARQUET_TRANSFORM's multi-TB lake/ is
    // still being copied there.  It is published as a hidden .pending file
    // and renamed by the workflow's onComplete section, which Nextflow runs
    // only after every pending publish has finished.
    publishDir "${params.outdir}/${params.release}", mode: 'copy', overwrite: true,
        saveAs: { name -> name == 'RELEASE_COMPLETE' ? '.RELEASE_COMPLETE.pending' : name }

    input:
    path lake
    path sorted_jsonl
    path validation_reports   // validation_report.txt and .json; their arrival is the VALIDATE gate
    val bin_code              // bin_digest: the imported bin/ modules

    output:
    path "SCHEMA.md", emit: schema_doc
    path "provenance.json", emit: provenance
    // RELEASE_COMPLETE is written by release_manifest.py as its last action.
    // This process runs only after VALIDATE passed, so the marker cannot
    // appear for a failed release; mirrors check it first.
    path "RELEASE_COMPLETE", emit: complete

    script:
    """
    set -euo pipefail
    # bin/ modules: ${bin_code}

    gen_schema_md.py --lake ${lake} -o SCHEMA.md

    release_manifest.py \
        --lake ${lake} \
        --input-jsonl ${sorted_jsonl} \
        --release ${params.release} \
        --published ${sorted_jsonl} SCHEMA.md ${validation_reports} \
        -o provenance.json \
        --complete-marker RELEASE_COMPLETE
    """
}


/* ── WORKFLOW ─────────────────────────────────────────────────────── */
workflow {
    main:
    def bar = '═' * 57
    log.info """
    ╔${bar}╗
    ${banner_line('UniProtKB → Parquet Data Lake')}
    ${banner_line("Release: ${params.release}")}
    ╚${bar}╝
    Input:      ${params.inputfile}
    Output:     ${release_dir()}
    DuckDB mem: ${params.duckdb_pct}% of task.memory (scales on retry)
    DuckDB tmp: ${params.duckdb_temp ?: '(default: \$TMPDIR or /tmp)'}
    Expected:   ${params.expected_count ?: '(not set — post-hoc verification only)'}
    Previous:   ${params.previous_manifest ?: '(not set — no schema diff)'}
    ${'─' * 59}
    """.stripIndent()

    // Validate inputs
    if (!file(params.inputfile).exists()) {
        error "Input file not found: ${params.inputfile}"
    }
    if (params.previous_manifest && !file(params.previous_manifest).exists()) {
        error "Previous manifest not found: ${params.previous_manifest}"
    }
    // PARQUET_TRANSFORM republishes into release_dir while VALIDATE reads the
    // previous manifest, so one from inside it would be diffed against itself
    // (and its new mtime would re-run VALIDATE on every -resume).
    def this_release = file(release_dir())
    if (params.previous_manifest && this_release.exists()
            && file(params.previous_manifest).toRealPath().startsWith(this_release.toRealPath())) {
        error "Previous manifest is inside the release being built (${release_dir()}): ${params.previous_manifest}"
    }

    // A re-run republishes into an existing release_dir, so a marker left by
    // a previous successful run must be removed NOW — before the first
    // publishDir copy can land — or a mirror that checks RELEASE_COMPLETE
    // first would sync a mix of old and half-written files as a "complete"
    // release.  A fresh marker is written by onComplete only if this run
    // succeeds end to end.
    if (!workflow.preview) {
        def stale = file("${release_dir()}/RELEASE_COMPLETE")
        if (stale.exists()) {
            stale.delete()
            log.info "Removed stale RELEASE_COMPLETE from ${release_dir()} (re-run)"
        }
    }

    // 1. Stream input → single zstd-compressed JSONL
    def input_ch = channel.fromPath(params.inputfile)
    STREAM_JSONL(input_ch)

    // 2. Sort JSONL by reviewed DESC, organism_id ASC, accession ASC (lines kept as they are)
    SORT_JSONL(STREAM_JSONL.out.jsonl, bin_digest('SORT_JSONL'))

    // 3. Transform sorted JSONL → Parquet tables
    //    DuckDB infers the input schema from the whole file (read_json_auto).
    PARQUET_TRANSFORM(SORT_JSONL.out.sorted_jsonl, bin_digest('PARQUET_TRANSFORM'))

    // 4. Validate the Parquet lake (uses sorted JSONL as ground truth —
    //    same entries, same count, just reordered — anchored to the entry
    //    count taken before the sort)
    VALIDATE(
        PARQUET_TRANSFORM.out.lake,
        SORT_JSONL.out.sorted_jsonl,
        STREAM_JSONL.out.count_file,
        params.previous_manifest ? file(params.previous_manifest) : [],
        bin_digest('VALIDATE'),
    )

    // 5. SCHEMA.md and provenance record (only after validation passes)
    PROVENANCE(
        PARQUET_TRANSFORM.out.lake,
        SORT_JSONL.out.sorted_jsonl,
        VALIDATE.out.report.concat(VALIDATE.out.report_json).collect(),
        bin_digest('PROVENANCE'),
    )

    // 6. RELEASE_COMPLETE is the last file to reach release_dir.
    //    onComplete runs after every publishDir copy has finished, and
    //    renames the marker only if verify_release.py finds the published
    //    copy whole, so a mirror that sees the marker sees the whole lake/.  The rename is
    //    atomic within the directory; a failed run leaves no marker (and no
    //    .pending file either — VALIDATE failing means PROVENANCE never ran,
    //    and anything staged by an earlier attempt is removed).
    onComplete:
    def pending = file(pending_marker())
    def problems = (workflow.success && pending.exists()) ? verify_release_problems() : null
    if (problems) {
        pending.delete()
        log.error "RELEASE_COMPLETE not written: the published copy in ${release_dir()} " +
                  "does not match its metadata. Re-run with -resume to republish it.\n${problems}"
    }
    else if (workflow.success && pending.exists()) {
        // moveTo overwrites an existing file target atomically
        // (FilesEx.moveTo passes REPLACE_EXISTING), so no delete-first is
        // needed — and none is wanted: a separate delete would open a window
        // with no marker at all.  A stale marker from a previous run was
        // already removed at workflow start.
        pending.moveTo(file("${release_dir()}/RELEASE_COMPLETE"))
        log.info "RELEASE_COMPLETE written to ${release_dir()}"
    }
    else if (pending.exists()) {
        pending.delete()
    }
}
