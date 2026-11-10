package cdc_v2.engine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * One short JDBC query, made once per engine life when the first batch arrives (so after START_REPLICATION has
 * succeeded): which backend holds the slot. Debezium names every replication connection "Debezium Streaming" and
 * offers no way to give it a name of our own (docs/design/engine.md, "Attach rule"), so HELLO reports the holder's
 * process id and start time instead.
 */
final class SlotProbe {

    static final class Holder {
        Long activePid;
        Long backendStartUs;
        String applicationName = "";
        Integer clientPort;
    }

    private final String url;
    private final Properties props = new Properties();

    SlotProbe(Map<String, String> c) {
        String host = c.get("database.hostname");
        String port = c.get("database.port");
        if (host == null || host.isBlank() || port == null || port.isBlank()) {
            // never a default host or port: port 5432 belongs to the owner's own server
            throw new ProtectedConfig.Refused("database.hostname and database.port are required for the slot probe");
        }
        url = "jdbc:postgresql://" + host.trim() + ":" + Integer.parseInt(port.trim()) + "/"
                + c.getOrDefault("database.dbname", "");
        put(c, "database.user", "user");
        put(c, "database.password", "password");
        put(c, "database.sslmode", "sslmode");
        put(c, "database.sslrootcert", "sslrootcert");
        put(c, "database.sslcert", "sslcert");
        put(c, "database.sslkey", "sslkey");
        props.setProperty("ApplicationName", "cdc_v2 engine probe");
        props.setProperty("connectTimeout", "10");
        props.setProperty("socketTimeout", "10");
        props.setProperty("loginTimeout", "10");
        props.setProperty("tcpKeepAlive", "true");
    }

    private void put(Map<String, String> c, String from, String to) {
        String v = c.get(from);
        if (v != null && !v.isEmpty()) {
            props.setProperty(to, v);
        }
    }

    Holder holder(String slot) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, props);
             PreparedStatement st = conn.prepareStatement(
                     "select s.active_pid, (extract(epoch from a.backend_start) * 1000000)::bigint, a.application_name, "
                             + "a.client_port from pg_replication_slots s left join pg_stat_activity a on a.pid = s.active_pid "
                             + "where s.slot_name = ?")) {
            st.setQueryTimeout(10);
            st.setString(1, slot);
            Holder h = new Holder();
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    long pid = rs.getLong(1);
                    h.activePid = rs.wasNull() ? null : pid;
                    long bs = rs.getLong(2);
                    h.backendStartUs = rs.wasNull() ? null : bs;
                    String an = rs.getString(3);
                    h.applicationName = an == null ? "" : an;
                    int port = rs.getInt(4);
                    h.clientPort = rs.wasNull() ? null : port;
                }
            }
            return h;
        }
    }
}
