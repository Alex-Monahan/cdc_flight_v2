package cdc_v2.engine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The rows of one ROWS frame under construction: one table, one schema version, columns in Arrow buffer layout
 * (validity bitmap, 32-bit offsets for text and bytes, values), little-endian.
 *
 * <p>Use: {@link #startRow()}, then set every column exactly once, then {@link #endRow()}; {@link #rollbackRow()}
 * undoes the row started last (used to move an oversized row into a frame of its own).
 */
final class RowsBuffer {

    static final int P_BOOL = 1;
    static final int P_INT16 = 2;
    static final int P_INT32 = 3;
    static final int P_INT64 = 4;
    static final int P_FLOAT32 = 5;
    static final int P_FLOAT64 = 6;
    static final int P_UTF8 = 7;
    static final int P_BINARY = 8;

    static boolean isVar(int p) {
        return p == P_UTF8 || p == P_BINARY;
    }

    static int width(int p) {
        switch (p) {
            case P_INT16:
                return 2;
            case P_INT32:
            case P_FLOAT32:
                return 4;
            case P_INT64:
            case P_FLOAT64:
                return 8;
            default:
                return 0;
        }
    }

    static final class Col {
        final String name;
        final int ptype;
        final ByteBuf valid = new ByteBuf(64);
        final ByteBuf data;
        final ByteBuf offsets;
        int nulls;
        int mData;
        int mOffsets;
        int mNulls;

        Col(String name, int ptype) {
            this.name = name;
            this.ptype = ptype;
            this.data = new ByteBuf(isVar(ptype) ? 4096 : 256);
            this.offsets = isVar(ptype) ? new ByteBuf(256) : null;
            clear();
        }

        void clear() {
            valid.release(1 << 16);
            data.release(1 << 22);
            nulls = 0;
            if (offsets != null) {
                offsets.release(1 << 18);
                offsets.u32(0);
            }
        }

        long bytes() {
            return valid.n + data.n + (offsets == null ? 0 : offsets.n);
        }
    }

    final String schema;
    final String table;
    final int version;
    final Col[] cols;
    int rows;
    private int mRows;

    RowsBuffer(String schema, String table, int version, String[] names, int[] ptypes) {
        this.schema = schema;
        this.table = table;
        this.version = version;
        this.cols = new Col[names.length];
        for (int i = 0; i < names.length; i++) {
            cols[i] = new Col(names[i], ptypes[i]);
        }
    }

    long bytes() {
        long b = 0;
        for (Col c : cols) {
            b += c.bytes();
        }
        return b;
    }

    void startRow() {
        mRows = rows;
        for (Col c : cols) {
            c.mData = c.data.n;
            c.mOffsets = c.offsets == null ? 0 : c.offsets.n;
            c.mNulls = c.nulls;
        }
        if ((rows & 7) == 0) {
            for (Col c : cols) {
                c.valid.u8(0);
                if (c.ptype == P_BOOL) {
                    c.data.u8(0);
                }
            }
        }
    }

    void endRow() {
        rows++;
    }

    void rollbackRow() {
        rows = mRows;
        int bitmapBytes = (rows + 7) >>> 3;
        for (Col c : cols) {
            if ((rows & 7) != 0) {
                // clear the bits of the rolled-back row (and beyond) in the last byte
                int keep = (1 << (rows & 7)) - 1;
                c.valid.a[bitmapBytes - 1] &= (byte) keep;
                if (c.ptype == P_BOOL) {
                    c.data.a[bitmapBytes - 1] &= (byte) keep;
                }
            }
            c.valid.n = bitmapBytes;
            c.nulls = c.mNulls;
            c.data.n = c.ptype == P_BOOL ? bitmapBytes : c.mData;
            if (c.offsets != null) {
                c.offsets.n = c.mOffsets;
            }
        }
    }

    void clear() {
        rows = 0;
        for (Col c : cols) {
            c.clear();
        }
    }

    private static void setBit(ByteBuf b, int i, boolean v) {
        int idx = i >>> 3;
        int mask = 1 << (i & 7);
        if (v) {
            b.a[idx] |= (byte) mask;
        }
        else {
            b.a[idx] &= (byte) ~mask;
        }
    }

    void setNull(int ci) {
        Col c = cols[ci];
        setBit(c.valid, rows, false);
        c.nulls++;
        switch (c.ptype) {
            case P_BOOL:
                setBit(c.data, rows, false);
                break;
            case P_UTF8:
            case P_BINARY:
                c.offsets.u32(c.data.n);
                break;
            default:
                c.data.zeros(width(c.ptype));
        }
    }

    void setBool(int ci, boolean v) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        setBit(c.data, rows, v);
    }

    void setI16(int ci, short v) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        c.data.u16(v);
    }

    void setI32(int ci, int v) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        c.data.u32(v);
    }

    void setI64(int ci, long v) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        c.data.u64(v);
    }

    void setF32(int ci, float v) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        c.data.f32(v);
    }

    void setF64(int ci, double v) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        c.data.f64(v);
    }

    /** Raw bits into a 32-bit column (float32 with an exact NaN payload; used by the frame generator). */
    void setRaw32(int ci, int bits) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        c.data.u32(bits);
    }

    void setRaw64(int ci, long bits) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        c.data.u64(bits);
    }

    void setBytes(int ci, byte[] b, int off, int len) {
        Col c = cols[ci];
        setBit(c.valid, rows, true);
        c.data.bytes(b, off, len);
        c.offsets.u32(c.data.n);
    }

    void setStr(int ci, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        setBytes(ci, b, 0, b.length);
    }

    /** Byte length of the value just set in column ci of the current row (text and bytes only). */
    int lastLength(int ci) {
        Col c = cols[ci];
        int n = c.offsets.n;
        return ByteBuf.getI32(c.offsets.a, n - 4) - ByteBuf.getI32(c.offsets.a, n - 8);
    }

    /**
     * Payload of the ROWS frame: schema str, table str, version u32, rows u32, ncols u16, then per column:
     * name str, ptype u8, null_count u32, validity buffer, [offsets buffer], data buffer. A buffer is u32 length,
     * zero padding to a multiple of 8, the bytes, zero padding to a multiple of 8. The validity buffer is empty when
     * the column has no NULL.
     */
    FrameOut.Payload payload(ByteBuf meta) {
        meta.reset();
        FrameOut.Payload p = new FrameOut.Payload(meta);
        meta.str(schema);
        meta.str(table);
        meta.u32(version);
        meta.u32(rows);
        meta.u16(cols.length);
        int bitmapBytes = (rows + 7) >>> 3;
        for (Col c : cols) {
            meta.str(c.name);
            meta.u8(c.ptype);
            meta.u32(c.nulls);
            if (c.nulls == 0) {
                p.buffer(c.valid.a, 0);
            }
            else {
                p.buffer(c.valid.a, bitmapBytes);
            }
            if (c.offsets != null) {
                p.buffer(c.offsets.a, c.offsets.n);
            }
            p.buffer(c.data.a, c.data.n);
        }
        p.cut();
        return p;
    }

    void write(FrameOut out, ByteBuf meta) throws IOException {
        out.frame(FrameOut.ROWS, payload(meta));
    }
}
