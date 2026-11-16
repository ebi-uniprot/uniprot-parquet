package org.uniprot.parquet;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.json.parser.uniprot.UniprotKBJsonConfig;
import org.uniprot.core.scorer.uniprotkb.UniProtEntryScored;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.core.uniprotkb.xdb.impl.UniProtCrossReferenceBuilder;
import org.uniprot.cv.evidence.EvidenceHelper;
import org.uniprot.cv.evidence.GOEvidences;

/**
 * UniProtKB flat file to JSON lines, one entry per line, with UniProt's own code.
 *
 * <p>Each entry goes through the steps of uniprot-store's UniProtKBDataStoreIndexer
 * (spark-indexer, the job that fills the store REST serves entries from), in its order:
 *
 * <ol>
 *   <li>FlatFileToUniprotEntry: the flat-file parser, then EntryObjectConverter with the
 *       keyword, disease and subcellular-location files (and no GO file);
 *   <li>joinGoEvidences: GOEvidencesFileMapper + GOEvidenceMapper over GO.dr_extended;
 *   <li>joinUniParcId: UniProtKBUniParcMappingRDDTupleReader + UniParcMapper over uniparc_map;
 *   <li>UniProtKBAnnotationScoreMapper: UniProtEntryScored, scaled to 1-5.
 * </ol>
 *
 * The Spark joins become in-memory maps holding only the accessions of the entries converted,
 * which a first pass over the input (or its byte range) collects. Output keeps the input's entry
 * order. {@code --range} converts the entries starting in a byte range of an uncompressed file,
 * so a SLURM array can split a release; {@code uniparc-active} writes the uniparc_map lines the
 * indexer keeps, once, for those runs to share.
 */
public final class Dat2Jsonl {

    private static final String USAGE =
            String.join(
                    "\n",
                    "usage: java -jar dat2jsonl.jar --dat FILE --keywords keywlist.txt "
                            + "--diseases humdisease.txt --subcell subcell.txt",
                    "           [--go GO.dr_extended --gaf-eco gaf-eco-mapping.txt] [--uniparc FILE|DIR]",
                    "           [--range START:END] [--stats FILE] [--threads N] [--mapper simple|full]"
                            + " > out.jsonl",
                    "       java -jar dat2jsonl.jar uniparc-active --uniparc FILE|DIR > uniparc_active.txt",
                    "",
                    "  --dat       UniProtKB flat file, plain or .gz",
                    "  --keywords  keywlist.txt     (keyword ids and categories)",
                    "  --diseases  humdisease.txt   (disease ids)",
                    "  --subcell   subcell.txt      (subcellular location ids)",
                    "  --go        GO.dr_extended   (PubMed evidences on GO cross-references)",
                    "  --gaf-eco   gaf-eco-mapping.txt, needed with --go: uniprot-core otherwise"
                            + " downloads it,",
                    "              and on failure silently maps every GO code to no ECO",
                    "  --uniparc   uniparc_map file, or a directory of them (plain or .gz)",
                    "  --range     only the entries that start in bytes [START, END) of an"
                            + " uncompressed --dat",
                    "  --stats     write a one-row TSV: offsets covered, entries, accessions, time",
                    "  --threads   worker threads (default: the CPUs this JVM may use)",
                    "  --mapper    simple (default): the JSON REST serves; full: the store's form",
                    "",
                    "uniparc-active writes the uniparc_map lines the indexer keeps (status not Y),",
                    "unchanged, so that many --range runs read them once each instead of the whole map.",
                    "",
                    "JSON lines go to stdout; progress and a summary to stderr.");

    static final String STATS_HEADER =
            String.join(
                    "\t",
                    "dat",
                    "range_start",
                    "range_end",
                    "first_offset",
                    "end_offset",
                    "entries",
                    "with_go",
                    "with_uniparc",
                    "first_accession",
                    "last_accession",
                    "seconds");

    public static void main(String[] args) throws Exception {
        // Anything a library prints to System.out must not land in the JSON lines.
        OutputStream jsonOut =
                new BufferedOutputStream(new FileOutputStream(FileDescriptor.out), 1 << 20);
        System.setOut(System.err);
        try {
            int status;
            if (args.length > 0 && args[0].equals("uniparc-active")) {
                status = uniparcActive(Arrays.copyOfRange(args, 1, args.length), jsonOut);
            } else {
                status = run(parseArgs(args), jsonOut);
            }
            System.exit(status);
        } catch (UsageException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println(USAGE);
            System.exit(2);
        }
    }

    // ---- arguments -------------------------------------------------------------------------

    static final class UsageException extends Exception {
        UsageException(String message) {
            super(message);
        }
    }

    static final class Options {
        Path dat;
        String keywords;
        String diseases;
        String subcell;
        Path go;
        Path gafEco;
        Path uniparc;
        long rangeStart = 0;
        long rangeEnd = Long.MAX_VALUE;
        boolean ranged;
        Path stats;
        int threads = Runtime.getRuntime().availableProcessors();
        String mapper = "simple";
    }

    static Options parseArgs(String[] args) throws UsageException {
        Options o = new Options();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.equals("-h") || a.equals("--help")) {
                throw new UsageException("dat2jsonl: UniProtKB flat file to JSON lines");
            }
            if (i + 1 >= args.length) {
                throw new UsageException("missing value for " + a);
            }
            String v = args[++i];
            switch (a) {
                case "--dat":
                    o.dat = Paths.get(v);
                    break;
                case "--keywords":
                    o.keywords = v;
                    break;
                case "--diseases":
                    o.diseases = v;
                    break;
                case "--subcell":
                    o.subcell = v;
                    break;
                case "--go":
                    o.go = Paths.get(v);
                    break;
                case "--gaf-eco":
                    o.gafEco = Paths.get(v);
                    break;
                case "--uniparc":
                    o.uniparc = Paths.get(v);
                    break;
                case "--range":
                    String[] r = v.split(":", -1);
                    try {
                        o.rangeStart = Long.parseLong(r[0]);
                        o.rangeEnd = Long.parseLong(r[1]);
                    } catch (RuntimeException e) {
                        throw new UsageException("--range is START:END, in bytes");
                    }
                    if (r.length != 2 || o.rangeStart < 0 || o.rangeEnd < o.rangeStart) {
                        throw new UsageException("--range is START:END, in bytes");
                    }
                    o.ranged = true;
                    break;
                case "--stats":
                    o.stats = Paths.get(v);
                    break;
                case "--threads":
                    o.threads = Integer.parseInt(v);
                    break;
                case "--mapper":
                    o.mapper = v;
                    break;
                default:
                    throw new UsageException("unknown option " + a);
            }
        }
        if (o.dat == null || o.keywords == null || o.diseases == null || o.subcell == null) {
            throw new UsageException("--dat, --keywords, --diseases and --subcell are required");
        }
        if (o.go != null && o.gafEco == null) {
            throw new UsageException("--go needs --gaf-eco");
        }
        if (!o.mapper.equals("simple") && !o.mapper.equals("full")) {
            throw new UsageException("--mapper is simple or full");
        }
        if (o.ranged && o.dat.getFileName().toString().endsWith(".gz")) {
            throw new UsageException("--range needs an uncompressed --dat");
        }
        for (String f : new String[] {o.keywords, o.diseases, o.subcell}) {
            if (!Files.isRegularFile(Paths.get(f))) {
                throw new UsageException("no such file: " + f);
            }
        }
        for (Path p : new Path[] {o.dat, o.go, o.gafEco, o.uniparc}) {
            if (p != null && !Files.exists(p)) {
                throw new UsageException("no such file: " + p);
            }
        }
        if (o.threads < 1) {
            throw new UsageException("--threads must be at least 1");
        }
        return o;
    }

    // ---- the run ---------------------------------------------------------------------------

    static int run(Options o, OutputStream jsonOut) throws Exception {
        long t0 = System.nanoTime();
        String what = o.ranged ? o.dat + " [" + o.rangeStart + ", " + o.rangeEnd + ")" : o.dat.toString();

        if (o.gafEco != null) {
            // Read by GOEvidences on first use (CVSystemProperties.GAF_ECO_LOCATION)
            System.setProperty("cv.gaf.eco.location", o.gafEco.toAbsolutePath().toString());
            if (GOEvidences.INSTANCE.convertGAFToECO("IDA").isEmpty()) {
                System.err.println("error: no GAF-to-ECO mapping read from " + o.gafEco);
                return 1;
            }
        }

        Set<String> accessions = null;
        if (o.go != null || o.uniparc != null) {
            accessions = primaryAccessions(o);
            log("%,d entries in %s", accessions.size(), what);
        }
        Map<String, Map<String, List<Evidence>>> goEvidences =
                o.go == null ? Map.of() : loadGoEvidences(o.go, accessions);
        Map<String, String> uniParcIds =
                o.uniparc == null ? Map.of() : loadUniParcIds(o.uniparc, accessions);

        // FlatFileToUniprotEntry's SupportingDataMap: keyword, disease, subcell; no GO file
        SupportingDataMap supportingData =
                new SupportingDataMapImpl(o.keywords, o.diseases, null, o.subcell);
        if (supportingData.getKeywordMap().isEmpty()
                || supportingData.getDiseaseMap().isEmpty()
                || supportingData.getSubcellularLocationMap().isEmpty()) {
            System.err.println("error: a keyword, disease or subcellular-location file read as empty");
            return 1;
        }

        ObjectMapper mapper =
                o.mapper.equals("full")
                        ? UniprotKBJsonConfig.getInstance().getFullObjectMapper()
                        : UniprotKBJsonConfig.getInstance().getSimpleObjectMapper();

        Converter converter = new Converter(supportingData, goEvidences, uniParcIds, mapper);
        Totals t = convertAll(o, converter, jsonOut);

        double secs = (System.nanoTime() - t0) / 1e9;
        if (accessions != null && accessions.size() != t.entries) {
            System.err.printf(
                    "error: %,d entries converted, but the first pass found %,d%n",
                    t.entries, accessions.size());
            return 1;
        }
        log(
                "done: %,d entries in %.0f s (%,.0f/s); GO evidences added to %,d, UniParc id to %,d;"
                        + " bytes [%d, %d)",
                t.entries,
                secs,
                t.entries / secs,
                converter.withGo.get(),
                converter.withUniParc.get(),
                t.firstOffset,
                t.endOffset);
        if (o.stats != null) {
            String row =
                    String.join(
                            "\t",
                            o.dat.toString(),
                            Long.toString(o.rangeStart),
                            o.ranged ? Long.toString(o.rangeEnd) : "",
                            Long.toString(t.firstOffset),
                            Long.toString(t.endOffset),
                            Long.toString(t.entries),
                            Long.toString(converter.withGo.get()),
                            Long.toString(converter.withUniParc.get()),
                            t.firstAccession == null ? "" : t.firstAccession,
                            t.lastAccession == null ? "" : t.lastAccession,
                            String.format("%.1f", secs));
            Files.writeString(o.stats, STATS_HEADER + "\n" + row + "\n");
        }
        return 0;
    }

    static final class Totals {
        long entries;
        long firstOffset;
        long endOffset;
        String firstAccession;
        String lastAccession;
    }

    static final class Converted {
        final String accession;
        final byte[] json;

        Converted(String accession, byte[] json) {
            this.accession = accession;
            this.json = json;
        }
    }

    private static final Converted END = new Converted(null, null);

    /** Reads entries in order, converts them on a thread pool, writes them in input order. */
    static Totals convertAll(Options o, Converter converter, OutputStream out) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(o.threads);
        BlockingQueue<Future<Converted>> pending = new ArrayBlockingQueue<>(o.threads * 256);
        final Throwable[] writerError = new Throwable[1];
        final Totals t = new Totals();

        Thread writer =
                new Thread(
                        () -> {
                            try {
                                long next = 100_000;
                                long start = System.nanoTime();
                                while (true) {
                                    Converted c = pending.take().get();
                                    if (c == END) {
                                        break;
                                    }
                                    out.write(c.json);
                                    out.write('\n');
                                    if (t.firstAccession == null) {
                                        t.firstAccession = c.accession;
                                    }
                                    t.lastAccession = c.accession;
                                    if (++t.entries == next) {
                                        double s = (System.nanoTime() - start) / 1e9;
                                        log("%,d entries (%,.0f/s)", t.entries, t.entries / s);
                                        next += next < 1_000_000 ? 100_000 : 1_000_000;
                                    }
                                }
                                out.flush();
                            } catch (Throwable e) {
                                writerError[0] = e;
                            }
                        },
                        "writer");
        writer.start();

        try (EntryReader in = new EntryReader(o.dat, o.rangeStart, o.rangeEnd)) {
            t.firstOffset = in.firstOffset();
            byte[] entry;
            while (writerError[0] == null && (entry = in.next()) != null) {
                // UTF-8, as Spark's textFile decodes it
                String text = new String(entry, StandardCharsets.UTF_8);
                pending.put(pool.submit(() -> converter.convert(text)));
            }
            t.endOffset = in.endOffset();
        } finally {
            pending.put(pool.submit(() -> END));
            writer.join();
            pool.shutdownNow();
        }
        if (writerError[0] != null) {
            Throwable e = writerError[0];
            if (e instanceof ExecutionException && e.getCause() != null) {
                e = e.getCause();
            }
            throw new RuntimeException("conversion failed: " + e.getMessage(), e);
        }
        return t;
    }

    // ---- one entry -------------------------------------------------------------------------

    static final class Converter {
        private final SupportingDataMap supportingData;
        private final Map<String, Map<String, List<Evidence>>> goEvidences;
        private final Map<String, String> uniParcIds;
        private final ObjectMapper mapper;
        final AtomicLong withGo = new AtomicLong();
        final AtomicLong withUniParc = new AtomicLong();

        // FlatFileToUniprotEntry builds a parser and a converter per entry; one per thread
        // here (-Ddat2jsonl.freshParser=true builds them per entry, to compare).
        private final ThreadLocal<UniprotKBLineParser<EntryObject>> parser;
        private final ThreadLocal<EntryObjectConverter> entryConverter;

        Converter(
                SupportingDataMap supportingData,
                Map<String, Map<String, List<Evidence>>> goEvidences,
                Map<String, String> uniParcIds,
                ObjectMapper mapper) {
            this.supportingData = supportingData;
            this.goEvidences = goEvidences;
            this.uniParcIds = uniParcIds;
            this.mapper = mapper;
            boolean fresh = Boolean.getBoolean("dat2jsonl.freshParser");
            this.parser =
                    fresh
                            ? null
                            : ThreadLocal.withInitial(
                                    () -> new DefaultUniprotKBLineParserFactory().createEntryParser());
            this.entryConverter =
                    fresh
                            ? null
                            : ThreadLocal.withInitial(
                                    () -> new EntryObjectConverter(supportingData, false));
        }

        Converted convert(String text) throws Exception {
            UniProtKBEntry entry = parse(text);
            String accession = entry.getPrimaryAccession().getValue();

            Map<String, List<Evidence>> go = goEvidences.get(accession);
            if (go != null) {
                entry = addGoEvidences(entry, go);
                withGo.incrementAndGet();
            }
            String uniParcId = uniParcIds.get(accession);
            if (uniParcId != null) {
                // UniParcMapper
                entry =
                        UniProtKBEntryBuilder.from(entry)
                                .extraAttributesAdd(UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB, uniParcId)
                                .build();
                withUniParc.incrementAndGet();
            }
            // UniProtKBAnnotationScoreMapper
            int score = scaleAnnotationScore(new UniProtEntryScored(entry).score());
            entry = UniProtKBEntryBuilder.from(entry).annotationScore(score).build();

            return new Converted(accession, mapper.writeValueAsBytes(entry));
        }

        private UniProtKBEntry parse(String text) {
            // FlatFileToUniprotEntry
            UniprotKBLineParser<EntryObject> p =
                    parser == null
                            ? new DefaultUniprotKBLineParserFactory().createEntryParser()
                            : parser.get();
            EntryObjectConverter c =
                    entryConverter == null
                            ? new EntryObjectConverter(supportingData, false)
                            : entryConverter.get();
            try {
                return c.convert(p.parse(text));
            } catch (RuntimeException e) {
                int eol = text.indexOf('\n');
                throw new IllegalStateException(
                        "cannot parse entry " + (eol > 0 ? text.substring(0, eol) : text), e);
            }
        }
    }

    /** GOEvidenceMapper: the evidences for each GO cross-reference's GO id replace its own. */
    static UniProtKBEntry addGoEvidences(
            UniProtKBEntry entry, Map<String, List<Evidence>> goEvidenceMap) {
        List<UniProtKBCrossReference> xrefs =
                entry.getUniProtKBCrossReferences().stream()
                        .map(
                                xref -> {
                                    if (!Objects.equals(xref.getDatabase().getName(), "GO")) {
                                        return xref;
                                    }
                                    List<Evidence> evidences = goEvidenceMap.get(xref.getId());
                                    if (evidences == null || evidences.isEmpty()) {
                                        return xref;
                                    }
                                    return new UniProtCrossReferenceBuilder()
                                            .database(xref.getDatabase())
                                            .id(xref.getId())
                                            .isoformId(xref.getIsoformId())
                                            .evidencesSet(evidences)
                                            .propertiesSet(xref.getProperties())
                                            .build();
                                })
                        .collect(Collectors.toList());
        return UniProtKBEntryBuilder.from(entry).uniProtCrossReferencesSet(xrefs).build();
    }

    /** SparkUtils.scaleAnnotationScore */
    static int scaleAnnotationScore(double score) {
        int q = (int) (score / 20d);
        return q > 4 ? 5 : q + 1;
    }

    // ---- supporting files ------------------------------------------------------------------

    /** The primary accession (first AC on the first AC line) of every entry to convert. */
    static Set<String> primaryAccessions(Options o) throws IOException {
        Set<String> accs = new HashSet<>();
        try (EntryReader in = new EntryReader(o.dat, o.rangeStart, o.rangeEnd)) {
            byte[] entry;
            while ((entry = in.next()) != null) {
                String acc = EntryReader.primaryAccession(entry);
                if (acc == null || !accs.add(acc)) {
                    throw new IOException(
                            acc == null
                                    ? "an entry without an AC line in " + o.dat
                                    : "accession " + acc + " twice in " + o.dat);
                }
            }
        }
        return accs;
    }

    /**
     * GOEvidencesFileMapper + GOEvidencesRDDReader's groupByKey + GOEvidenceMapper's map: for
     * each accession, GO id to its evidences in file order, duplicates dropped.
     */
    static Map<String, Map<String, List<Evidence>>> loadGoEvidences(Path file, Set<String> accessions)
            throws IOException {
        Map<String, Map<String, LinkedHashSet<Evidence>>> byAcc = new HashMap<>();
        long rows = 0;
        long kept = 0;
        try (BufferedReader in = reader(file)) {
            String line;
            while ((line = in.readLine()) != null) {
                rows++;
                String[] f = line.split("\t");
                if (f.length < 7) {
                    throw new IllegalArgumentException(
                            "unable to parse line: '" + line + "' in go evidence file");
                }
                String accession = f[0];
                if (accessions != null && !accessions.contains(accession)) {
                    continue;
                }
                String goId = f[1];
                String eco = uniProtEcoId(f[4], f[5]);
                String value = f[6].replace("PMID", eco + "|PubMed");
                Evidence evidence;
                try {
                    evidence = EvidenceHelper.parseEvidenceLine(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            "Failed parsing line " + line + " with uniProtEcoId is " + eco, e);
                }
                byAcc.computeIfAbsent(accession, a -> new LinkedHashMap<>())
                        .computeIfAbsent(goId, g -> new LinkedHashSet<>())
                        .add(evidence);
                kept++;
            }
        }
        Map<String, Map<String, List<Evidence>>> out = new HashMap<>(byAcc.size() * 2);
        byAcc.forEach(
                (acc, byGo) -> {
                    Map<String, List<Evidence>> m = new HashMap<>(byGo.size() * 2);
                    byGo.forEach((go, evs) -> m.put(go, new ArrayList<>(evs)));
                    out.put(acc, m);
                });
        log("%s: %,d rows, %,d for these entries (%,d accessions)", file, rows, kept, out.size());
        return out;
    }

    /** GOEvidencesFileMapper.getUniProtECOId */
    static String uniProtEcoId(String ecoId, String gafCode) {
        return Arrays.stream(EvidenceCode.values())
                .filter(ev -> ecoId.equalsIgnoreCase(ev.getCode()))
                .findFirst()
                .map(EvidenceCode::getCode)
                .orElse(GOEvidences.INSTANCE.convertGAFToECO(gafCode).orElse(""));
    }

    /** One uniparc_map line: "UPI accession status", split as uniprot-store splits it. */
    static String[] uniParcTokens(Path file, String line) {
        String[] tokens = line.split("\s"); // a single space, as in uniprot-store
        if (tokens.length != 3) {
            throw new IllegalArgumentException(
                    "Unable to parse UniProtKBUniParcMapping line in " + file + ": " + line);
        }
        return tokens;
    }

    /**
     * UniProtKBUniParcMappingRDDTupleReader(active = true)'s StatusFilter: keeps a line when its
     * status is not Y ("TODO: Source data is inverted").
     */
    static boolean keptByIndexer(String[] tokens) {
        boolean lineStatus = tokens[2].strip().equalsIgnoreCase("Y");
        return !lineStatus;
    }

    static List<Path> uniParcFiles(Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            return List.of(path);
        }
        try (Stream<Path> s = Files.list(path)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().startsWith("."))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /** The indexer's StatusFilter, then UniProtKBUniParcMapper: accession to UniParc id. */
    static Map<String, String> loadUniParcIds(Path path, Set<String> accessions)
            throws IOException {
        List<Path> files = uniParcFiles(path);
        Map<String, String> out = new HashMap<>();
        long rows = 0;
        long active = 0;
        long repeated = 0;
        for (Path file : files) {
            try (BufferedReader in = reader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    rows++;
                    String[] tokens = uniParcTokens(file, line);
                    if (!keptByIndexer(tokens)) {
                        continue;
                    }
                    active++;
                    String accession = tokens[1].strip();
                    if (accessions != null && !accessions.contains(accession)) {
                        continue;
                    }
                    String previous = out.putIfAbsent(accession, tokens[0].strip());
                    if (previous != null) {
                        repeated++;
                    }
                }
            }
        }
        log(
                "%s: %,d files, %,d rows, %,d kept by status, %,d for these entries%s",
                path,
                files.size(),
                rows,
                active,
                out.size(),
                repeated == 0
                        ? ""
                        : String.format(
                                " (%,d repeated accessions: the first kept; the indexer's join"
                                        + " would write the entry once per line)",
                                repeated));
        return out;
    }

    /** uniparc-active: the lines loadUniParcIds keeps, unchanged, from every file. */
    static int uniparcActive(String[] args, OutputStream out) throws Exception {
        if (args.length != 2 || !args[0].equals("--uniparc")) {
            throw new UsageException("uniparc-active takes --uniparc FILE|DIR");
        }
        Path path = Paths.get(args[1]);
        if (!Files.exists(path)) {
            throw new UsageException("no such file: " + path);
        }
        long rows = 0;
        long kept = 0;
        List<Path> files = uniParcFiles(path);
        for (Path file : files) {
            try (BufferedReader in = reader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    rows++;
                    if (keptByIndexer(uniParcTokens(file, line))) {
                        out.write(line.getBytes(StandardCharsets.UTF_8));
                        out.write('\n');
                        kept++;
                    }
                }
            }
        }
        out.flush();
        log("%s: %,d files, %,d rows, %,d kept by status", path, files.size(), rows, kept);
        return 0;
    }

    // ---- io --------------------------------------------------------------------------------

    static BufferedReader reader(Path p) throws IOException {
        InputStream in = Files.newInputStream(p);
        if (p.getFileName().toString().endsWith(".gz")) {
            in = new GZIPInputStream(in, 1 << 16);
        }
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 20);
    }

    static void log(String format, Object... args) {
        PrintStream err = System.err;
        err.println("dat2jsonl: " + String.format(format, args));
        err.flush();
    }

    private Dat2Jsonl() {}
}
