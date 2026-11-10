package cdc_v2.engine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The check of the effective configuration at start (hazard H-016, plan C2): every property that carries a guarantee
 * must have exactly our value, and properties that add behaviour must be absent. The Python side refuses the same keys
 * in its "extra" properties (cdc_v2.stream.engine.PROTECTED_KEYS); this is the second line. Messages name the key,
 * never a value.
 */
final class ProtectedConfig {

    /** Thrown when the configuration is refused. Its message names a key and contains no value. */
    static final class Refused extends RuntimeException {
        Refused(String message) {
            super(message);
        }
    }

    static final Map<String, String> REQUIRED = new LinkedHashMap<>();

    static {
        REQUIRED.put("skipped.operations", "none");
        REQUIRED.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        REQUIRED.put("offset.flush.interval.ms", "0");
        REQUIRED.put("lsn.flush.mode", "connector");
        REQUIRED.put("errors.max.retries", "0");
        REQUIRED.put("slot.max.retries", "0");
        REQUIRED.put("provide.transaction.metadata", "true");
        REQUIRED.put("snapshot.mode", "no_data");
        REQUIRED.put("publication.autocreate.mode", "disabled");
        REQUIRED.put("converters", "cdcv2");
        REQUIRED.put("cdcv2.type", "cdc_v2.engine.TransportConverter");
        REQUIRED.put("tombstones.on.delete", "false");
        REQUIRED.put("offset.mismatch.strategy", "no_validation");
        REQUIRED.put("plugin.name", "pgoutput");
        REQUIRED.put("record.processing.order", "ORDERED");
        REQUIRED.put("include.unknown.datatypes", "true");
        REQUIRED.put("schema.refresh.mode", "columns_diff");
    }

    /** Keys (or prefixes ending in '.') that must not be present at all. */
    static final List<String> FORBIDDEN = List.of("heartbeat.action.query", "replica.identity.autoset.values",
            "read.only", "signal.", "offset.commit.policy", "offset.storage.",
            // options that silently change or drop values or rows
            "column.", "transforms", "transforms.", "predicates", "predicates.", "message.key.columns",
            "table.exclude.list", "schema.include.list", "schema.exclude.list");

    static void check(Map<String, String> c) {
        for (Map.Entry<String, String> e : REQUIRED.entrySet()) {
            String v = c.get(e.getKey());
            if (v == null || !v.trim().equals(e.getValue())) {
                throw new Refused("protected property " + e.getKey() + " is missing or differs from the required value");
            }
        }
        for (String k : c.keySet()) {
            for (String f : FORBIDDEN) {
                if (f.endsWith(".") ? k.startsWith(f) : k.equals(f)) {
                    throw new Refused("property " + k + " is not allowed");
                }
            }
            if (k.startsWith("cdcv2.") && !k.equals("cdcv2.type")) {
                throw new Refused("property " + k + " is not allowed");
            }
        }
        String hb = c.get("heartbeat.interval.ms");
        long h;
        try {
            h = Long.parseLong(hb == null ? "" : hb.trim());
        }
        catch (NumberFormatException ex) {
            throw new Refused("protected property heartbeat.interval.ms is missing or not a number");
        }
        if (h <= 0 || h > 10_000) {
            throw new Refused("protected property heartbeat.interval.ms must be in 1..10000");
        }
        if (empty(c.get("database.hostname")) || empty(c.get("database.port"))) {
            throw new Refused("database.hostname and database.port are required (no default host or port)");
        }
    }

    private static boolean empty(String s) {
        return s == null || s.trim().isEmpty();
    }

    private ProtectedConfig() {
    }
}
