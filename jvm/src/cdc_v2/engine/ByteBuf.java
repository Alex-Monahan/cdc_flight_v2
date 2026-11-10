package cdc_v2.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** A growable little-endian byte buffer. Not thread safe. */
final class ByteBuf {

    private static final VarHandle I16 = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle I32 = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle I64 = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /** The largest buffer this class creates (a Java array limit with some margin). */
    static final int MAX = Integer.MAX_VALUE - 64;

    byte[] a;
    int n;
    private final int initial;

    ByteBuf(int capacity) {
        initial = Math.max(16, capacity);
        a = new byte[initial];
    }

    void ensure(int extra) {
        long need = (long) n + extra;
        if (need > a.length) {
            if (need > MAX) {
                throw new IllegalStateException("buffer would exceed " + MAX + " bytes");
            }
            long cap = Math.max(need, Math.min((long) MAX, (long) a.length * 2));
            a = Arrays.copyOf(a, (int) cap);
        }
    }

    void reset() {
        n = 0;
    }

    /** Empties the buffer and gives memory back when it grew far beyond its initial size. */
    void release(int keep) {
        n = 0;
        if (a.length > Math.max(keep, initial)) {
            a = new byte[initial];
        }
    }

    void u8(int v) {
        ensure(1);
        a[n++] = (byte) v;
    }

    void u16(int v) {
        ensure(2);
        I16.set(a, n, (short) v);
        n += 2;
    }

    void u32(int v) {
        ensure(4);
        I32.set(a, n, v);
        n += 4;
    }

    void u64(long v) {
        ensure(8);
        I64.set(a, n, v);
        n += 8;
    }

    void f32(float v) {
        u32(Float.floatToRawIntBits(v));
    }

    void f64(double v) {
        u64(Double.doubleToRawLongBits(v));
    }

    void bytes(byte[] b, int off, int len) {
        ensure(len);
        System.arraycopy(b, off, a, n, len);
        n += len;
    }

    void zeros(int k) {
        ensure(k);
        Arrays.fill(a, n, n + k, (byte) 0);
        n += k;
    }

    /** u32 byte length, then the UTF-8 bytes. */
    void str(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        u32(b.length);
        bytes(b, 0, b.length);
    }

    /** u32 byte length, then the bytes. */
    void bin(byte[] b) {
        u32(b.length);
        bytes(b, 0, b.length);
    }

    void optU64(Long v) {
        if (v == null) {
            u8(0);
        }
        else {
            u8(1);
            u64(v);
        }
    }

    void putI32At(int pos, int v) {
        I32.set(a, pos, v);
    }

    static int getI32(byte[] b, int pos) {
        return (int) I32.get(b, pos);
    }
}
