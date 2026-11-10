package cdc_v2.engine;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes a deterministic frame stream from a seed, with the same generator as
 * src/cdc_v2/testing/frames.py ({@code generate}), so that the Java encoder and the Python encoder can be compared
 * byte for byte and the Python decoder can be run on Java output. Usage: {@code CdcEngine --framegen <seed> <out>}.
 */
final class FrameGen {

    long state;

    FrameGen(long seed) {
        state = seed;
    }

    long next() {
        state += 1;
        return Crash.splitmix64(state);
    }

    int u(int n) {
        return (int) Long.remainderUnsigned(next(), n);
    }

    boolean coin() {
        return (next() & 1) == 1;
    }

    String text(int n) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < n; i++) {
            int k = u(5);
            int cp;
            if (k == 0) {
                cp = u(128);
            }
            else if (k == 1) {
                cp = 0x80 + u(0x780);
            }
            else if (k == 2) {
                cp = 0x800 + u(0xD800 - 0x800);
            }
            else if (k == 3) {
                cp = 0xE000 + u(0x2000);
            }
            else {
                cp = 0x10000 + u(0x100000);
            }
            b.appendCodePoint(cp);
        }
        return b.toString();
    }

    byte[] bytes(int n) {
        byte[] out = new byte[n];
        for (int i = 0; i < n; i++) {
            out[i] = (byte) next();
        }
        return out;
    }

    Long optLong() {
        boolean present = coin();
        return present ? next() : null;
    }

    void posmap(ByteBuf b) {
        int n = u(5);
        b.u16(n);
        for (int i = 0; i < n; i++) {
            b.str("k" + i);
            int kind = u(4);
            b.u8(kind);
            if (kind == 1) {
                b.u64(next());
            }
            else if (kind == 2) {
                b.str(text(u(6)));
            }
            else if (kind == 3) {
                b.u8(coin() ? 1 : 0);
            }
        }
    }

    static final short[] E16 = {0, -1, 1, Short.MIN_VALUE, Short.MAX_VALUE};
    static final int[] E32 = {0, -1, 1, Integer.MIN_VALUE, Integer.MAX_VALUE};
    static final long[] E64 = {0, -1, 1, Long.MIN_VALUE, Long.MAX_VALUE};
    static final int[] F32 = {0x7fc00000, 0x7f800000, 0xff800000, 0x80000000, 0x00000001, 0x7f7fffff};
    static final long[] F64 = {0x7ff8000000000000L, 0x7ff0000000000000L, 0xfff0000000000000L, 0x8000000000000000L,
            1L, 0x7fefffffffffffffL};

    void value(RowsBuffer rb, int ci, int ptype) {
        if (u(5) == 0) {
            rb.setNull(ci);
            return;
        }
        switch (ptype) {
            case RowsBuffer.P_BOOL:
                rb.setBool(ci, coin());
                break;
            case RowsBuffer.P_INT16:
                rb.setI16(ci, u(4) == 0 ? E16[u(5)] : (short) next());
                break;
            case RowsBuffer.P_INT32:
                rb.setI32(ci, u(4) == 0 ? E32[u(5)] : (int) next());
                break;
            case RowsBuffer.P_INT64:
                rb.setI64(ci, u(4) == 0 ? E64[u(5)] : next());
                break;
            case RowsBuffer.P_FLOAT32:
                rb.setRaw32(ci, u(4) == 0 ? F32[u(6)] : (int) next());
                break;
            case RowsBuffer.P_FLOAT64:
                rb.setRaw64(ci, u(4) == 0 ? F64[u(6)] : next());
                break;
            case RowsBuffer.P_UTF8:
                rb.setStr(ci, text(u(20)));
                break;
            default:
                byte[] b = bytes(u(20));
                rb.setBytes(ci, b, 0, b.length);
        }
    }

    void write(FrameOut out) throws IOException {
        ByteBuf b = new ByteBuf(1024);
        // HELLO
        b.u32(1);
        b.str("gen-" + u(1000));
        b.str("slot_" + u(100));
        b.optU64(optLong());
        b.u32(u(100000));
        b.str("e" + u(1000));
        b.optU64(optLong());
        b.optU64(optLong());
        b.str(text(u(12)));
        out.frame(FrameOut.HELLO, b);
        // SCHEMA
        b = new ByteBuf(1024);
        String schema = "s" + u(3);
        String table = "t" + text(u(6));
        int version = 1 + u(5);
        boolean hasKey = coin();
        int ncols = 1 + u(8);
        b.str(schema);
        b.str(table);
        b.u32(version);
        b.u8(hasKey ? 1 : 0);
        b.u16(hasKey ? 1 : 0);
        if (hasKey) {
            b.u16(0);
        }
        b.u16(ncols);
        int[] ptypes = new int[ncols];
        boolean[] unchanged = new boolean[ncols];
        for (int i = 0; i < ncols; i++) {
            int p = 1 + u(8);
            ptypes[i] = p;
            b.u16(i);
            b.str(text(u(10)));
            b.str("T" + p);
            b.str(coin() ? "sem." + u(9) : "");
            b.str("f" + p);
            b.u8(p);
            b.str(text(u(8)));
            b.u32(coin() ? u(1000) : -1);
            b.u32(coin() ? u(20) : -1);
            b.u8(coin() ? 1 : 0);
            b.u8(hasKey && i == 0 ? 1 : 0);
            unchanged[i] = p >= RowsBuffer.P_UTF8 && coin();
            b.u8(unchanged[i] ? 1 : 0);
        }
        out.frame(FrameOut.SCHEMA, b);
        // BEGIN
        b = new ByteBuf(256);
        b.u64(u(1 << 30));
        b.str(text(u(10)));
        b.u64(next());
        posmap(b);
        b.optU64(optLong());
        out.frame(FrameOut.BEGIN, b);
        // ROWS
        java.util.List<String> names = new java.util.ArrayList<>();
        java.util.List<Integer> types = new java.util.ArrayList<>();
        names.add("seq");
        types.add(RowsBuffer.P_INT64);
        names.add("op");
        types.add(RowsBuffer.P_UTF8);
        for (int i = 0; i < ncols; i++) {
            names.add("a" + i);
            types.add(ptypes[i]);
        }
        if (!hasKey) {
            for (int i = 0; i < ncols; i++) {
                names.add("b" + i);
                types.add(ptypes[i]);
            }
        }
        for (int i = 0; i < ncols; i++) {
            if (unchanged[i]) {
                names.add("u" + i);
                types.add(RowsBuffer.P_BOOL);
            }
        }
        if (hasKey) {
            names.add("k0");
            types.add(ptypes[0]);
        }
        String[] na = names.toArray(new String[0]);
        int[] ta = types.stream().mapToInt(Integer::intValue).toArray();
        int nframes = u(4);
        long seq = u(1000);
        ByteBuf meta = new ByteBuf(1024);
        for (int f = 0; f < nframes; f++) {
            RowsBuffer rb = new RowsBuffer(schema, table, version, na, ta);
            int nrows = u(40);
            for (int r = 0; r < nrows; r++) {
                rb.startRow();
                rb.setI64(0, seq++);
                rb.setStr(1, String.valueOf("cud".charAt(u(3))));
                for (int c = 2; c < na.length; c++) {
                    value(rb, c, ta[c]);
                }
                rb.endRow();
            }
            rb.write(out, meta);
        }
        // TRUNCATE
        b = new ByteBuf(256);
        b.u64(u(1 << 30));
        b.str(text(u(5)));
        b.str(text(u(5)));
        posmap(b);
        out.frame(FrameOut.TRUNCATE, b);
        // MESSAGE
        b = new ByteBuf(256);
        b.u64(u(1 << 30));
        b.str(text(u(8)));
        b.u8(coin() ? 1 : 0);
        b.bin(bytes(u(30)));
        posmap(b);
        out.frame(FrameOut.MESSAGE, b);
        // END
        b = new ByteBuf(256);
        b.u64(u(1 << 30));
        b.str(text(u(10)));
        b.u64(next());
        posmap(b);
        b.optU64(optLong());
        b.u64(u(1_000_000));
        int nc = u(3);
        b.u32(nc);
        for (int i = 0; i < nc; i++) {
            b.str(text(u(8)));
            b.u64(u(1000));
        }
        out.frame(FrameOut.END, b);
        // HEARTBEAT
        b = new ByteBuf(256);
        b.u64(u(1 << 30));
        posmap(b);
        b.optU64(optLong());
        out.frame(FrameOut.HEARTBEAT, b);
        // BATCH_END
        b = new ByteBuf(64);
        b.u32(u(10000));
        b.optU64(optLong());
        out.frame(FrameOut.BATCH_END, b);
        // FATAL
        b = new ByteBuf(256);
        b.str(text(u(20)));
        b.str(text(u(40)));
        int n = u(3);
        b.u16(n);
        for (int i = 0; i < n; i++) {
            b.str(text(u(10)));
            b.str(text(u(10)));
        }
        out.frame(FrameOut.FATAL, b);
    }

    static void main(String[] args) throws IOException {
        long first = Long.parseUnsignedLong(args[0]);
        int count = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(args[1]), 1 << 16)) {
            FrameOut out = new FrameOut(os);
            for (int i = 0; i < count; i++) {
                new FrameGen(first + i).write(out);
            }
            out.flush();
        }
    }
}
