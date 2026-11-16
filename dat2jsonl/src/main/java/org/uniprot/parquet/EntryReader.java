package org.uniprot.parquet;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

/**
 * The entries of a UniProtKB flat file, or of a byte range of one, as raw bytes.
 *
 * <p>An entry starts at offset 0 or right after a "//" line, and runs up to and including its own
 * "//" line, newline included: the text uniprot-store's UniProtKBRDDTupleReader hands the parser
 * (records split on "\n//\n", each given "\n//\n" back).
 *
 * <p>A range [start, end) holds the entries that start inside it, so ranges that tile a file give
 * every entry exactly once. {@link #firstOffset()} is where the range's first entry starts and
 * {@link #endOffset()} where its last one ends (both the file's size if there is none); for
 * consecutive ranges the first's end offset equals the next's first offset, which is how the runs
 * are checked to have covered the file.
 *
 * <p>Ranges need an uncompressed file. A .gz file is read whole.
 */
final class EntryReader implements Closeable {

    private static final byte[] SEPARATOR = {'\n', '/', '/', '\n'};

    private final InputStream in;
    private final long end;
    private final long firstOffset;
    private long pos;
    private long endOffset = -1;

    private byte[] buf = new byte[1 << 16]; // the entry being read
    private int len;
    private final byte[] ibuf = new byte[1 << 20]; // input, unsynchronised
    private int ipos;
    private int ilen;

    EntryReader(Path file, long start, long end) throws IOException {
        if (start < 0 || end < start) {
            throw new IllegalArgumentException("bad range " + start + ":" + end);
        }
        this.end = end;
        if (file.getFileName().toString().endsWith(".gz")) {
            if (start != 0 || end != Long.MAX_VALUE) {
                throw new IllegalArgumentException("a byte range needs an uncompressed file: " + file);
            }
            in = new GZIPInputStream(Files.newInputStream(file), 1 << 16);
            firstOffset = 0;
        } else {
            FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
            long first = start == 0 ? 0 : firstEntryAtOrAfter(ch, start);
            ch.position(first);
            in = Channels.newInputStream(ch);
            firstOffset = first;
        }
        pos = firstOffset;
    }

    /** Offset of the first entry start at or after {@code start} (> 0), or the file's size. */
    private static long firstEntryAtOrAfter(FileChannel ch, long start) throws IOException {
        // An entry starts right after "\n//\n"; a match ending at or after start begins at
        // start - 4 or later.
        long from = Math.max(0, start - SEPARATOR.length);
        ByteBuffer bb = ByteBuffer.allocate(1 << 20);
        long base = from;
        int matched = 0;
        ch.position(from);
        while (true) {
            bb.clear();
            int n = ch.read(bb);
            if (n < 0) {
                return ch.size();
            }
            byte[] a = bb.array();
            for (int i = 0; i < n; i++) {
                byte b = a[i];
                if (b == SEPARATOR[matched]) {
                    matched++;
                } else {
                    matched = b == SEPARATOR[0] ? 1 : 0;
                }
                if (matched == SEPARATOR.length) {
                    long after = base + i + 1;
                    if (after >= start) {
                        return after;
                    }
                    // "\n//\n" overlaps the next one's leading "\n"
                    matched = 1;
                }
            }
            base += n;
        }
    }

    /** The next entry's bytes, or null after the range's last. */
    byte[] next() throws IOException {
        if (endOffset >= 0) {
            return null;
        }
        if (pos >= end) {
            endOffset = pos;
            return null;
        }
        len = 0;
        int lineStart = 0;
        while (true) {
            int b = read();
            if (b < 0) {
                if (len == 0) {
                    endOffset = pos;
                    return null;
                }
                throw new IOException(
                        "input ends inside an entry (no closing //) at offset " + pos);
            }
            if (len == buf.length) {
                buf = Arrays.copyOf(buf, buf.length * 2);
            }
            buf[len++] = (byte) b;
            pos++;
            if (b == '\n') {
                if (len - lineStart == 3 && buf[lineStart] == '/' && buf[lineStart + 1] == '/') {
                    return Arrays.copyOf(buf, len);
                }
                lineStart = len;
            }
        }
    }

    private int read() throws IOException {
        if (ipos == ilen) {
            ilen = in.readNBytes(ibuf, 0, ibuf.length);
            ipos = 0;
            if (ilen <= 0) {
                ilen = 0;
                return -1;
            }
        }
        return ibuf[ipos++] & 0xff;
    }

    long firstOffset() {
        return firstOffset;
    }

    /** Where the range's last entry ends; valid once {@link #next()} has returned null. */
    long endOffset() {
        return endOffset;
    }

    /** The first accession on the entry's first AC line. */
    static String primaryAccession(byte[] entry) {
        int i = 0;
        while (i < entry.length) {
            int eol = i;
            while (eol < entry.length && entry[eol] != '\n') {
                eol++;
            }
            if (eol - i > 5
                    && entry[i] == 'A'
                    && entry[i + 1] == 'C'
                    && entry[i + 2] == ' '
                    && entry[i + 3] == ' '
                    && entry[i + 4] == ' ') {
                int s = i + 5;
                while (s < eol && entry[s] == ' ') {
                    s++;
                }
                int e = s;
                while (e < eol && entry[e] != ';' && entry[e] != ' ') {
                    e++;
                }
                return new String(entry, s, e - s, java.nio.charset.StandardCharsets.US_ASCII);
            }
            i = eol + 1;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
