package cdc_v2.engine;

import java.util.HashMap;
import java.util.Map;

/**
 * Test mode {@code CdcEngine --held-selftest}: exercises HeldEnds without Debezium and prints one JSON object.
 * 60,000 END records with ack = 1000 × n, limit 4096; then DURABLE positions are applied in turn; for each it reports
 * which END was confirmed. Also checks that an END offset without lsn_commit is refused.
 */
final class HeldSelftest {

    static void main() {
        HeldEnds<Integer> h = new HeldEnds<>(4096);
        int n = 60_000;
        for (int i = 1; i <= n; i++) {
            h.add(1000L * i, i);
        }
        StringBuilder b = new StringBuilder("{\"spacing_after_60000\":").append(h.spacing())
                .append(",\"held\":").append(h.size()).append(",\"takes\":[");
        long[] durables = {500, 1000L * 20_000, 1000L * 20_000 + 999, 1000L * 30_000, 1000L * 36_000,
                1000L * 59_999, 1000L * 60_000, 1000L * 60_001};
        boolean first = true;
        for (long p : durables) {
            HeldEnds.Entry<Integer> e = h.take(p);
            b.append(first ? "" : ",").append("{\"durable\":").append(p).append(",\"confirmed\":")
                    .append(e == null ? "null" : Long.toString(e.ack)).append(",\"spacing\":").append(h.spacing())
                    .append("}");
            first = false;
        }
        b.append("],\"after_empty_spacing\":").append(h.spacing());
        // after the list emptied, k is 1 again: every END is held
        for (int i = 1; i <= 10; i++) {
            h.add(1000L * (n + 10 + i), i);
        }
        HeldEnds.Entry<Integer> e = h.take(1000L * (n + 15));
        b.append(",\"refill_confirmed\":").append(e == null ? "null" : Long.toString(e.ack));
        String refused;
        try {
            Map<String, Object> off = new HashMap<>();
            off.put("lsn_proc", 5L);
            HeldEnds.endAck(off);
            refused = "no";
        }
        catch (CdcEngine.EngineFatal ex) {
            refused = ex.getMessage();
        }
        b.append(",\"end_without_lsn_commit\":").append(Json.quote(refused)).append("}");
        System.out.println(b);
    }

    /**
     * Test mode {@code CdcEngine --held-property <seed> <rounds>}: random limits (2..41, odd ones included) and random
     * sequences of add and take. END acks are 1, 2, 3, …; for every take(p) it checks that the entry returned has
     * ack ≤ p, and that afterwards the newest END ≤ p lies at most k − 1 records above the newest END confirmed so far
     * (k: the spacing before or after the take, the larger). Prints one JSON object with the counts.
     */
    static void property(long seed, int rounds) {
        java.util.Random rnd = new java.util.Random(seed);
        long takes = 0;
        long adds = 0;
        long above = 0;
        long distance = 0;
        int oddLimits = 0;
        long maxK = 0;
        for (int r = 0; r < rounds; r++) {
            int limit = 2 + rnd.nextInt(40);
            oddLimits += limit % 2;
            HeldEnds<Long> h = new HeldEnds<>(limit);
            long next = 1;
            long confirmed = 0;
            int steps = 500 + rnd.nextInt(5000);
            for (int s = 0; s < steps; s++) {
                if (rnd.nextInt(10) < 8) {
                    int burst = 1 + rnd.nextInt(rnd.nextBoolean() ? 3 : 400);
                    for (int i = 0; i < burst; i++) {
                        h.add(next, next);
                        next++;
                        adds++;
                    }
                    continue;
                }
                long p;
                int kind = rnd.nextInt(4);
                if (kind == 0) {
                    p = next + rnd.nextInt(5);                        // above everything
                }
                else if (kind == 1) {
                    p = Math.max(0, confirmed - rnd.nextInt(5));      // at or below what is confirmed
                }
                else {
                    p = confirmed + (long) (rnd.nextDouble() * (next - confirmed));
                }
                long kBefore = h.spacing();
                HeldEnds.Entry<Long> e = h.take(p);
                long k = Math.max(kBefore, h.spacing());
                maxK = Math.max(maxK, k);
                takes++;
                if (e != null) {
                    if (e.ack > p) {
                        above++;
                    }
                    confirmed = Math.max(confirmed, e.ack);
                }
                long newestLe = Math.min(p, next - 1);
                if (newestLe > confirmed && newestLe - confirmed > k - 1) {
                    distance++;
                }
            }
        }
        System.out.println("{\"seed\":" + seed + ",\"rounds\":" + rounds + ",\"odd_limits\":" + oddLimits
                + ",\"adds\":" + adds + ",\"takes\":" + takes + ",\"returned_above_p\":" + above
                + ",\"beyond_k_minus_1\":" + distance + ",\"max_k\":" + maxK + "}");
    }

    private HeldSelftest() {
    }
}
