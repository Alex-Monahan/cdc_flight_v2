package cdc_v2.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The END records the engine holds until a DURABLE confirms them (docs/design/engine.md, section 5).
 *
 * <p>Bounded and uniformly spaced: every k-th END is held as a sample, plus always the newest END (the tail). When
 * more than {@code limit} samples are held, every second one is dropped (the newest and the oldest are kept) and k
 * doubles; when the list empties, k returns to 1. Dropping is always safe: a DURABLE then confirms an older END, never a newer one.
 * Bound: after a {@code DURABLE p}, the newest END confirmed so far lies at most k − 1 END records below the newest
 * END at or below p, with k ≤ max(1, 2 × lead / limit), where lead is the number of END records the engine has
 * received beyond the oldest one not yet confirmed; it is never above p. This holds for every limit ≥ 2.
 */
final class HeldEnds<T> {

    static final class Entry<T> {
        final long ack;
        final T record;

        Entry(long ack, T record) {
            this.ack = ack;
            this.record = record;
        }
    }

    private final int limit;
    private final ArrayDeque<Entry<T>> samples = new ArrayDeque<>();
    private Entry<T> tail;
    private long k = 1;
    private long sinceSample;

    HeldEnds(int limit) {
        if (limit < 2) {
            throw new IllegalArgumentException("limit must be >= 2");
        }
        this.limit = limit;
    }

    long spacing() {
        return k;
    }

    int size() {
        return samples.size() + (tail == null ? 0 : 1);
    }

    /** Hold an END record (its ack_position must not be below the previous one; the caller checks that). */
    void add(long ack, T record) {
        Entry<T> e = new Entry<>(ack, record);
        sinceSample++;
        if (sinceSample >= k) {
            samples.addLast(e);
            sinceSample = 0;
            tail = null;
            if (samples.size() > limit) {
                thin();
            }
        }
        else {
            tail = e;
        }
    }

    /**
     * Keep the newest sample and every second one counted back from it (gaps become 2k), and the oldest one (its gap
     * to the next kept sample is k or 2k). So no gap exceeds the new k, whatever the limit, odd or even.
     */
    private void thin() {
        List<Entry<T>> all = new ArrayList<>(samples);
        samples.clear();
        int n = all.size();
        int first = (n - 1) % 2;            // index of the oldest sample on the newest's parity
        if (first == 1) {
            samples.addLast(all.get(0));
        }
        for (int i = first; i < n; i += 2) {
            samples.addLast(all.get(i));
        }
        k *= 2;
    }

    /** The newest held entry with ack ≤ p, removed together with every older one; null if there is none. */
    Entry<T> take(long p) {
        Entry<T> best = null;
        while (!samples.isEmpty() && Long.compareUnsigned(samples.peekFirst().ack, p) <= 0) {
            best = samples.pollFirst();
        }
        if (tail != null && samples.isEmpty() && Long.compareUnsigned(tail.ack, p) <= 0) {
            best = tail;
            tail = null;
        }
        if (samples.isEmpty() && tail == null) {
            k = 1;
            sinceSample = 0;
        }
        return best;
    }

    /** The commit end of an END record's offset: lsn_commit, never another field (plan C2). */
    static long endAck(Map<String, ?> offset) {
        Object c = offset == null ? null : offset.get("lsn_commit");
        if (!(c instanceof Number)) {
            throw new CdcEngine.EngineFatal("END record without lsn_commit in its offset");
        }
        return ((Number) c).longValue();
    }
}
