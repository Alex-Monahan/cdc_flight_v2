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

    private HeldSelftest() {
    }
}
