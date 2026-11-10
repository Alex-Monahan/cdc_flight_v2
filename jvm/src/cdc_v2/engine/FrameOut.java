package cdc_v2.engine;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Writes frames (docs/design/engine.md, "Byte layout"):
 *
 * <pre>
 * magic "CDCE" | type u8 | 3 zero bytes | seq u64 | length u32 | payload (length bytes) | crc32 u32
 * </pre>
 *
 * All integers little-endian. The CRC-32 (the one of zlib and java.util.zip.CRC32) covers every byte before it.
 * Frame sequence numbers start at 0 and have no gaps. One writer at a time (synchronized).
 */
final class FrameOut {

    static final byte[] MAGIC = {'C', 'D', 'C', 'E'};
    static final int HEADER = 20;

    static final int HELLO = 1;
    static final int SCHEMA = 2;
    static final int ROWS = 3;
    static final int BEGIN = 4;
    static final int END = 5;
    static final int TRUNCATE = 6;
    static final int MESSAGE = 7;
    static final int HEARTBEAT = 8;
    static final int BATCH_END = 9;
    static final int FATAL = 10;

    static final int PROTOCOL_VERSION = 1;

    private final OutputStream os;
    private final CRC32 crc = new CRC32();
    private final byte[] header = new byte[HEADER];
    private final ByteBuf hb = new ByteBuf(HEADER);
    private long seq;
    private long bytesWritten;

    FrameOut(OutputStream os) {
        this.os = os;
    }

    long frames() {
        return seq;
    }

    long bytes() {
        return bytesWritten;
    }

    synchronized void frame(int type, ByteBuf payload) throws IOException {
        Payload p = new Payload(payload);
        p.cut();
        frame(type, p);
    }

    synchronized void frame(int type, Payload p) throws IOException {
        long len = p.length();
        if (len > 0xFFFFFFFFL) {
            throw new IOException("frame payload too large: " + len);
        }
        hb.reset();
        hb.bytes(MAGIC, 0, 4);
        hb.u8(type);
        hb.zeros(3);
        hb.u64(seq);
        hb.u32((int) len);
        System.arraycopy(hb.a, 0, header, 0, HEADER);
        crc.reset();
        crc.update(header, 0, HEADER);
        os.write(header, 0, HEADER);
        for (Payload.Chunk c : p.chunks) {
            byte[] arr = c.array();
            crc.update(arr, c.off, c.len);
            os.write(arr, c.off, c.len);
        }
        int v = (int) crc.getValue();
        byte[] t = {(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24)};
        os.write(t, 0, 4);
        seq++;
        bytesWritten += HEADER + len + 4;
    }

    synchronized void flush() throws IOException {
        os.flush();
    }

    /**
     * A payload assembled from slices of a metadata buffer and of other arrays, so that large column buffers are
     * written without another copy. {@link #pad8()} pads with zero bytes to the next multiple of 8 counted from the
     * start of the payload.
     */
    static final class Payload {
        static final byte[] ZEROS = new byte[8];

        final ByteBuf meta;
        final List<Chunk> chunks = new ArrayList<>();
        private int cutAt;
        private long length;

        Payload(ByteBuf meta) {
            this.meta = meta;
        }

        long length() {
            return length;
        }

        /** Appends the metadata written since the last cut as one chunk. */
        void cut() {
            if (meta.n > cutAt) {
                chunks.add(new Chunk(meta, null, cutAt, meta.n - cutAt));
                length += meta.n - cutAt;
                cutAt = meta.n;
            }
        }

        void add(byte[] arr, int off, int len) {
            cut();
            if (len > 0) {
                chunks.add(new Chunk(null, arr, off, len));
                length += len;
            }
        }

        void pad8() {
            cut();
            int p = (int) ((8 - (length & 7)) & 7);
            if (p > 0) {
                chunks.add(new Chunk(null, ZEROS, 0, p));
                length += p;
            }
        }

        /** u32 length, padding, the bytes, padding: the form of every column buffer in a ROWS frame. */
        void buffer(byte[] arr, int len) {
            meta.u32(len);
            pad8();
            add(arr, 0, len);
            pad8();
        }

        static final class Chunk {
            final ByteBuf meta;
            final byte[] arr;
            final int off;
            final int len;

            Chunk(ByteBuf meta, byte[] arr, int off, int len) {
                this.meta = meta;
                this.arr = arr;
                this.off = off;
                this.len = len;
            }

            byte[] array() {
                return meta != null ? meta.a : arr;
            }
        }
    }
}
