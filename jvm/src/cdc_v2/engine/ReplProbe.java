package cdc_v2.engine;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Test tool (not used by the engine): opens a logical replication stream on a slot with pgjdbc at a start position
 * of our choice and sends standby status updates with a flush position of our choice, so that a test can see what
 * the server does with a flush position below the slot's confirmed_flush_lsn (core design, open point 14).
 *
 * <p>Usage: {@code CdcEngine --repl-probe}; standard input: one line {@code CONFIG <json>} with database.hostname,
 * database.port, database.dbname, database.user, database.password, database.sslmode, database.sslrootcert,
 * slot.name, publication.name, probe.start (LSN text), probe.flush (LSN text), probe.hold.ms.
 */
final class ReplProbe {

    static void main() throws Exception {
        InputStream in = System.in;
        String line = CdcEngine.readLine(in);
        Map<String, String> c = Json.parseStringObject(line.substring("CONFIG ".length()));
        Properties p = new Properties();
        PGProperty.USER.set(p, c.get("database.user"));
        PGProperty.PASSWORD.set(p, c.get("database.password"));
        PGProperty.SSL_MODE.set(p, c.getOrDefault("database.sslmode", "verify-full"));
        if (c.containsKey("database.sslrootcert")) {
            PGProperty.SSL_ROOT_CERT.set(p, c.get("database.sslrootcert"));
        }
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(p, "10");
        PGProperty.REPLICATION.set(p, "database");
        PGProperty.PREFER_QUERY_MODE.set(p, "simple");
        PGProperty.APPLICATION_NAME.set(p, "cdc_v2 repl probe");
        String url = "jdbc:postgresql://" + c.get("database.hostname") + ":" + c.get("database.port") + "/"
                + c.get("database.dbname");
        LogSequenceNumber start = LogSequenceNumber.valueOf(c.get("probe.start"));
        LogSequenceNumber flush = LogSequenceNumber.valueOf(c.get("probe.flush"));
        long hold = Long.parseLong(c.getOrDefault("probe.hold.ms", "3000"));
        try (Connection conn = DriverManager.getConnection(url, p)) {
            PGConnection pg = conn.unwrap(PGConnection.class);
            PGReplicationStream stream = pg.getReplicationAPI().replicationStream().logical()
                    .withSlotName(c.get("slot.name")).withStartPosition(start)
                    .withSlotOption("proto_version", 1).withSlotOption("publication_names", c.get("publication.name"))
                    .withStatusInterval(200, TimeUnit.MILLISECONDS).start();
            long until = System.currentTimeMillis() + hold;
            while (System.currentTimeMillis() < until) {
                stream.readPending();
                stream.setFlushedLSN(flush);
                stream.setAppliedLSN(flush);
                stream.forceUpdateStatus();
                Thread.sleep(100);
            }
            System.out.println("PROBE_DONE start=" + start.asString() + " flush=" + flush.asString()
                    + " last_receive=" + stream.getLastReceiveLSN().asString());
            stream.close();
        }
    }

    private ReplProbe() {
    }
}
