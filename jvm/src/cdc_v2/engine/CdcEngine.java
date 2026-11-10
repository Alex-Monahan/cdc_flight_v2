package cdc_v2.engine;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The engine: the stock Debezium embedded engine (PostgreSQL connector, pgoutput) inside our own JVM program, run
 * as a child of the Python pipeline (docs/adr/0001-architecture.md, decision 1; docs/design/engine.md).
 *
 * <ul>
 *   <li>Standard input, one command per line: {@code CONFIG <json>} (first line, once: the Debezium properties and
 *       our {@code cdc_v2.*} settings, including the password), {@code DURABLE <position>}, {@code STOP}. End of
 *       input means the parent is gone: the program halts at once and acknowledges nothing.</li>
 *   <li>File descriptor 3 ({@code -Dcdc_v2.frames}, default {@code /dev/fd/3}): the frames.</li>
 *   <li>Standard output and standard error: logs only ({@code System.out} is redirected to standard error).</li>
 * </ul>
 *
 * <p>Exit codes: 0 after STOP and a clean close; 1 after a FATAL frame; 2 bad usage or configuration; 3 STOP but
 * Debezium did not close in time (halted); 4 parent gone (end of standard input, or the parent process died);
 * 5 the frame pipe is closed.
 */
public final class CdcEngine {

    static final String ACK_MARKING = Crash.declare("engine.ack_marking",
            "DURABLE applied: the END or HEARTBEAT record to acknowledge is chosen, markProcessed not called yet");
    static final String ACKED = Crash.declare("engine.acked",
            "markProcessed and markBatchFinished returned: the acknowledgement is handed to Debezium");
    static final String FLUSH_SENT = Crash.declare("engine.flush_sent",
            "first batch after an acknowledgement: Debezium's poll has sent the flush position to the walsender");

    // ---------------------------------------------------------------------------------------------------- settings

    static final class Settings {
        String slot = "";
        String engineId = "";
        String probeSlot = "";   // test hook (cdc_v2.test.probe.slot): the slot the HELLO probe reads
        Long startPosition;
        long probeTimeoutMs = 30_000;
        int frameMaxRows = 65_536;
        long frameTargetBytes = 8L << 20;
        long frameMaxBytes = (1L << 30) + (16L << 20);
        long maxValueBytes = 256L << 20;
        int ackMaxPending = 4096;
        long stopTimeoutMs = 15_000;
        long parentPollMs = 500;
        long statIntervalMs = 10_000;

        static Settings from(Map<String, String> c) {
            Settings s = new Settings();
            s.slot = c.getOrDefault("slot.name", "");
            s.engineId = c.getOrDefault("cdc_v2.engine.id", "");
            s.probeSlot = c.getOrDefault("cdc_v2.test.probe.slot", s.slot);
            s.probeTimeoutMs = Long.parseLong(c.getOrDefault("cdc_v2.probe.timeout.ms", "30000"));
            String sp = c.get("cdc_v2.start.position");
            s.startPosition = sp == null || sp.isEmpty() ? null : Long.parseUnsignedLong(sp);
            s.frameMaxRows = Integer.parseInt(c.getOrDefault("cdc_v2.frame.max.rows", "65536"));
            s.frameTargetBytes = Long.parseLong(c.getOrDefault("cdc_v2.frame.target.bytes", Long.toString(8L << 20)));
            s.frameMaxBytes = Long.parseLong(c.getOrDefault("cdc_v2.frame.max.bytes", Long.toString((1L << 30) + (16L << 20))));
            s.maxValueBytes = Long.parseLong(c.getOrDefault("cdc_v2.max.value.bytes", Long.toString(256L << 20)));
            s.ackMaxPending = Integer.parseInt(c.getOrDefault("cdc_v2.ack.max.pending", "4096"));
            s.stopTimeoutMs = Long.parseLong(c.getOrDefault("cdc_v2.stop.timeout.ms", "15000"));
            s.parentPollMs = Long.parseLong(c.getOrDefault("cdc_v2.parent.poll.ms", "500"));
            s.statIntervalMs = Long.parseLong(c.getOrDefault("cdc_v2.stat.interval.ms", "10000"));
            if (s.frameMaxRows < 1 || s.frameTargetBytes < 1 || s.ackMaxPending < 2
                    || s.frameMaxBytes > 0xFFFFFFFFL || s.maxValueBytes > s.frameMaxBytes - (1 << 20)
                    || s.frameTargetBytes > s.frameMaxBytes - (1 << 20)) {
                throw new IllegalArgumentException("cdc_v2 settings out of range");
            }
            return s;
        }
    }

    // ------------------------------------------------------------------------------------------------------ state

    static Settings settings;
    static SlotProbe probe;
    static String debeziumVersion;
    static FrameOut out;
    static volatile DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    static final AtomicBoolean stopping = new AtomicBoolean();
    static final AtomicBoolean finished = new AtomicBoolean();
    static final ConcurrentLinkedQueue<Long> durables = new ConcurrentLinkedQueue<>();
    static final Map<String, String> secrets = new HashMap<>();   // value -> placeholder, for scrubbing messages

    // statistics (read by the STAT thread)
    static final AtomicLong records = new AtomicLong();
    static final AtomicLong rowsOut = new AtomicLong();
    static final AtomicLong batches = new AtomicLong();
    static final AtomicLong handlerCpuNs = new AtomicLong();
    static final AtomicLong handlerWriteNs = new AtomicLong();
    static volatile long ackedPosition = -1;
    static volatile int pendingSize;

    static void log(String event, String detail) {
        System.err.println("CDCV2 " + event + " " + System.currentTimeMillis() + (detail.isEmpty() ? "" : " " + detail));
        System.err.flush();
    }

    // ------------------------------------------------------------------------------------------------------- main

    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equals("--list-crash-points")) {
            System.out.println(Crash.listJson());
            System.out.flush();
            return;
        }
        if (args.length >= 1 && args[0].equals("--framegen")) {
            FrameGen.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length == 1 && args[0].equals("--repl-probe")) {
            ReplProbe.main();
            return;
        }
        if (args.length == 1 && args[0].equals("--held-selftest")) {
            HeldSelftest.main();
            return;
        }
        if (args.length >= 1 && args[0].equals("--splitmix")) {
            for (int i = 1; i < args.length; i++) {
                System.out.println(Long.toUnsignedString(Crash.splitmix64(Long.parseUnsignedLong(args[i]))));
            }
            return;
        }
        if (args.length == 3 && args[0].equals("--crash-selftest")) {
            // Calls the point args[1] args[2] times under the environment contract (tests of the Java side).
            Crash.init(System.getenv());
            int n = Integer.parseInt(args[2]);
            for (int i = 1; i <= n; i++) {
                try {
                    Crash.point(args[1]);
                }
                catch (Crash.CrashPointHit e) {
                    System.out.println("raised " + i);
                    System.out.flush();
                    return;
                }
                System.out.println("after " + i);
                System.out.flush();
            }
            Thread.sleep(Long.parseLong(System.getProperty("cdc_v2.selftest.linger.ms", "0")));
            return;
        }
        if (args.length != 0) {
            System.err.println("usage: CdcEngine | --list-crash-points | --framegen <seed> <out>");
            Runtime.getRuntime().halt(2);
        }
        System.setOut(System.err);
        try {
            Crash.init(System.getenv());
        }
        catch (IllegalArgumentException e) {
            log("BAD_CRASH_ENV", Json.quote(e.getMessage()));
            Runtime.getRuntime().halt(2);
        }
        String framesPath = System.getProperty("cdc_v2.frames", "/dev/fd/3");
        OutputStream sink = new FileOutputStream(framesPath);
        out = new FrameOut(new BufferedOutputStream(sink, 1 << 16));

        InputStream stdin = System.in;
        String first = readLine(stdin);
        if (first == null) {
            log("HALT", "reason=stdin_eof_before_config");
            Runtime.getRuntime().halt(4);
        }
        if (!first.startsWith("CONFIG ")) {
            log("BAD_CONFIG", "reason=first_line_is_not_CONFIG");
            Runtime.getRuntime().halt(2);
        }
        Map<String, String> config;
        try {
            config = Json.parseStringObject(first.substring(7));
        }
        catch (RuntimeException e) {
            log("BAD_CONFIG", "class=" + e.getClass().getName());
            Runtime.getRuntime().halt(2);
            return;
        }
        first = null;
        rememberSecret(config.get("database.password"), "<password>");
        rememberSecret(config.get("database.user"), "<user>");
        rememberSecret(config.get("database.hostname"), "<host>");
        rememberSecret(config.get("database.dbname"), "<dbname>");
        try {
            settings = Settings.from(config);
            ProtectedConfig.check(config);
            probe = new SlotProbe(config);
        }
        catch (RuntimeException e) {
            fatal(e instanceof ProtectedConfig.Refused ? e : new ProtectedConfig.Refused(
                    "cdc_v2 settings refused (" + e.getClass().getName() + ")"));
            return;
        }

        Properties props = new Properties();
        StringBuilder keys = new StringBuilder();
        for (Map.Entry<String, String> e : config.entrySet()) {
            if (!e.getKey().startsWith("cdc_v2.")) {
                props.setProperty(e.getKey(), e.getValue());
            }
            keys.append(keys.length() == 0 ? "" : ",").append(e.getKey());
        }
        config = null;
        String dbzVersion = io.debezium.connector.postgresql.Module.version();
        log("CONFIG", "keys=" + keys + " debezium=" + dbzVersion + " java=" + System.getProperty("java.version")
                + " max_heap=" + Runtime.getRuntime().maxMemory() + " crash_points=" + Crash.active());

        debeziumVersion = dbzVersion;

        Thread in = new Thread(() -> readCommands(stdin), "cdc_v2-stdin");
        in.setDaemon(true);
        in.start();
        Thread parent = new Thread(CdcEngine::watchParent, "cdc_v2-parent");
        parent.setDaemon(true);
        parent.start();
        Thread stats = new Thread(CdcEngine::statLoop, "cdc_v2-stat");
        stats.setDaemon(true);
        stats.start();

        Consumer consumer = new Consumer();
        DebeziumEngine.CompletionCallback completion = (success, message, error) -> {
            log("ENGINE_COMPLETED", "success=" + success + " stopping=" + stopping.get());
            if (!stopping.get()) {
                fatal(error != null ? error : new IllegalStateException("engine stopped: " + message));
            }
        };
        DebeziumEngine.ConnectorCallback callback = new DebeziumEngine.ConnectorCallback() {
            @Override
            public void taskStarted() {
                log("TASK_STARTED", "");
            }

            @Override
            public void pollingStarted() {
                log("POLLING_STARTED", "");
            }
        };
        try {
            engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                    .using(props).using(completion).using(callback).notifying(consumer).build();
        }
        catch (RuntimeException e) {
            fatal(e);
            return;
        }
        props = null;
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "cdc_v2-engine"));
        executor.execute(engine);
        executor.shutdown();
        while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            // the engine runs until STOP, a failure or the parent's end
        }
        // The engine ended. After STOP the stop thread decides the exit; otherwise the completion callback did.
        if (stopping.get()) {
            synchronized (finished) {
                while (!finished.get()) {
                    finished.wait(1000);
                }
            }
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    static void rememberSecret(String value, String placeholder) {
        if (value != null && !value.isEmpty() && (value.length() >= 3 || placeholder.equals("<password>"))) {
            secrets.put(value, placeholder);
        }
    }

    static String scrub(String s) {
        if (s == null) {
            return "";
        }
        // longest first, so a user or host name inside the password cannot break the password's replacement
        List<Map.Entry<String, String>> order = new ArrayList<>(secrets.entrySet());
        order.sort((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()));
        for (Map.Entry<String, String> e : order) {
            s = s.replace(e.getKey(), e.getValue());
        }
        s = s.replaceAll("(?i)password=[^\\s&;,]*", "password=<password>");
        s = s.replaceAll("jdbc:postgresql://[^\\s,;]*", "jdbc:postgresql://<redacted>");
        return s.length() > 2000 ? s.substring(0, 2000) + "..." : s;
    }

    /** Reads one line of US-ASCII from standard input; null at end of input. */
    static String readLine(InputStream in) throws IOException {
        StringBuilder b = new StringBuilder();
        while (true) {
            int c = in.read();
            if (c < 0) {
                return b.length() == 0 ? null : b.toString();
            }
            if (c == '\n') {
                return b.toString();
            }
            b.append((char) c);
        }
    }

    static void readCommands(InputStream in) {
        try {
            String line;
            while ((line = readLine(in)) != null) {
                if (line.startsWith("DURABLE ")) {
                    try {
                        durables.add(Long.parseUnsignedLong(line.substring(8).trim()));
                    }
                    catch (NumberFormatException e) {
                        log("BAD_COMMAND", "durable_not_a_number length=" + line.length());
                    }
                }
                else if (line.equals("STOP")) {
                    requestStop();
                }
                else {
                    log("BAD_COMMAND", "length=" + line.length());
                }
            }
        }
        catch (IOException | RuntimeException e) {
            log("STDIN_ERROR", "class=" + e.getClass().getName());
        }
        // The parent is gone: nobody can consume or acknowledge. Halt at once; acknowledge nothing.
        log("HALT", "reason=stdin_eof");
        Runtime.getRuntime().halt(4);
    }

    static void watchParent() {
        ProcessHandle parent = ProcessHandle.current().parent().orElse(null);
        long ppid = parent == null ? -1 : parent.pid();
        while (true) {
            try {
                Thread.sleep(settings.parentPollMs);
            }
            catch (InterruptedException e) {
                return;
            }
            ProcessHandle now = ProcessHandle.current().parent().orElse(null);
            if (parent == null || !parent.isAlive() || now == null || now.pid() != ppid) {
                log("HALT", "reason=parent_gone");
                Runtime.getRuntime().halt(4);
            }
        }
    }

    static void requestStop() {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        log("STOP", "handler_cpu_ms=" + handlerCpuNs.get() / 1_000_000L + " handler_write_ms="
                + handlerWriteNs.get() / 1_000_000L + " rows=" + rowsOut.get() + threadCpuSummary());
        Thread t = new Thread(() -> {
            long t0 = System.nanoTime();
            Thread closer = new Thread(() -> {
                try {
                    DebeziumEngine<?> e = engine;
                    if (e != null) {
                        e.close();
                    }
                }
                catch (Exception ex) {
                    log("CLOSE_ERROR", "class=" + ex.getClass().getName());
                }
            }, "cdc_v2-close");
            closer.setDaemon(true);
            closer.start();
            try {
                closer.join(settings.stopTimeoutMs);
            }
            catch (InterruptedException ignored) {
                // fall through
            }
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            if (closer.isAlive()) {
                log("HALT", "reason=close_timeout elapsed_ms=" + ms);
                Runtime.getRuntime().halt(3);
            }
            try {
                out.flush();
            }
            catch (IOException ignored) {
                // the parent may be gone
            }
            log("EXIT", "reason=stop elapsed_ms=" + ms + " handler_cpu_ms=" + handlerCpuNs.get() / 1_000_000L
                    + " handler_write_ms=" + handlerWriteNs.get() / 1_000_000L + " rows=" + rowsOut.get() + threadCpuSummary());
            Runtime.getRuntime().halt(0);
        }, "cdc_v2-stop");
        t.setDaemon(true);
        t.start();
    }

    /** Position fields of the last record handed over (framed), and the table of the record being handled. */
    static volatile Map<String, ?> lastHandedOffset;
    static volatile String fatalSchema = "";
    static volatile String fatalTable = "";

    /** Message prefixes of Debezium and Kafka Connect exceptions that never quote a source value. */
    static final List<String> SAFE_PREFIXES = List.of(
            "An exception occurred in the change event producer",
            "Failed to start replication stream at",
            "Failed to properly convert data value for",
            "Error while processing event at offset",
            "Timed out while waiting for committing task offset",
            "Publication autocreation is disabled",
            "Couldn't obtain encoding for database",
            "engine stopped:");

    /**
     * A message that can be shown without risk of quoting a source value: ours (built without values), the SQLState
     * of a SQL exception, a known safe prefix of a Debezium or Connect message (up to the first ';'), else nothing.
     */
    static String safeMessage(Throwable t) {
        String m = t.getMessage();
        if (m == null) {
            return "";
        }
        if (t instanceof EngineFatal || t instanceof ProtectedConfig.Refused) {
            return scrub(m);
        }
        if (t instanceof java.sql.SQLException) {
            String state = ((java.sql.SQLException) t).getSQLState();
            return "SQLState " + (state == null ? "unknown" : state);
        }
        for (String p : SAFE_PREFIXES) {
            if (m.startsWith(p)) {
                int cut = m.indexOf(';');
                return scrub(cut < 0 ? m : m.substring(0, cut));
            }
        }
        return "";
    }

    /** Writes a FATAL frame and halts with 1: class, safe message, cause chain, position fields of the last record
     *  handed over, schema and table of the record being handled (empty when unknown). Never a source value. */
    static synchronized void fatal(Throwable error) {
        try {
            ByteBuf b = new ByteBuf(1024);
            b.str(error.getClass().getName());
            b.str(safeMessage(error));
            List<Throwable> causes = new ArrayList<>();
            for (Throwable c = error.getCause(); c != null && causes.size() < 16 && !causes.contains(c); c = c.getCause()) {
                causes.add(c);
            }
            b.u16(causes.size());
            for (Throwable c : causes) {
                b.str(c.getClass().getName());
                b.str(safeMessage(c));
            }
            Consumer.posmap(b, lastHandedOffset);
            b.str(fatalSchema);
            b.str(fatalTable);
            out.frame(FrameOut.FATAL, b);
            out.flush();
        }
        catch (Throwable t) {
            log("FATAL_WRITE_ERROR", "class=" + t.getClass().getName());
        }
        log("EXIT", "reason=fatal class=" + error.getClass().getName());
        Runtime.getRuntime().halt(1);
    }

    static void statLoop() {
        Runtime rt = Runtime.getRuntime();
        while (true) {
            try {
                Thread.sleep(settings.statIntervalMs);
            }
            catch (InterruptedException e) {
                return;
            }
            log("STAT", "records=" + records.get() + " rows=" + rowsOut.get() + " batches=" + batches.get()
                    + " frames=" + out.frames() + " frame_bytes=" + out.bytes() + " acked=" + ackedPosition
                    + " pending=" + pendingSize + " heap_used=" + (rt.totalMemory() - rt.freeMemory())
                    + " handler_cpu_ms=" + handlerCpuNs.get() / 1_000_000L
                    + " handler_write_ms=" + handlerWriteNs.get() / 1_000_000L
                    + (TransportConverter.UNEXPECTED.isEmpty() ? "" : " unexpected=" + TransportConverter.UNEXPECTED)
                    + threadCpuSummary());
        }
    }

    /** CPU time per thread group: Debezium's streaming thread, the engine's poll thread, GC, others. */
    static String threadCpuSummary() {
        try {
            ThreadMXBean tb = ManagementFactory.getThreadMXBean();
            Map<String, Long> byKind = new TreeMap<>();
            for (long id : tb.getAllThreadIds()) {
                ThreadInfo ti = tb.getThreadInfo(id);
                long cpu = tb.getThreadCpuTime(id);
                if (ti == null || cpu < 0) {
                    continue;
                }
                String n = ti.getThreadName();
                String kind = n.contains("change-event-source-coordinator") ? "dbz_stream"
                        : n.startsWith("cdc_v2-engine") || n.contains("pool-") ? "engine_poll"
                        : n.startsWith("cdc_v2-") ? "cdc_v2_aux" : "other";
                byKind.merge(kind, cpu, Long::sum);
            }
            long gcMs = 0;
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                gcMs += Math.max(0, gc.getCollectionTime());
            }
            StringBuilder b = new StringBuilder();
            for (Map.Entry<String, Long> e : byKind.entrySet()) {
                b.append(" cpu_").append(e.getKey()).append("_ms=").append(e.getValue() / 1_000_000L);
            }
            b.append(" gc_ms=").append(gcMs);
            b.append(" process_cpu_ms=").append(
                    ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                            .getProcessCpuTime() / 1_000_000L);
            return b.toString();
        }
        catch (Throwable t) {
            return " cpu=unavailable";
        }
    }

    // ---------------------------------------------------------------------------------------------- the consumer

    /** Thrown for a condition of ours that must stop the engine with a FATAL frame. */
    static final class EngineFatal extends RuntimeException {
        EngineFatal(String message) {
            super(message);
        }
    }

    static final String TX_SCHEMA_SUFFIX = "TransactionMetadataValue";
    static final String HEARTBEAT_SCHEMA = "io.debezium.connector.common.Heartbeat";

    /** Per table and schema version: the column layout of its ROWS frames. */
    static final class TableState {
        final String schema;
        final String table;
        int version;
        Schema rowSchema;
        Schema keySchema;
        Field[] fields;
        int[] physical;
        boolean[] unchangedPossible;
        boolean[] isKey;
        int[] keyPositions;
        boolean hasKey;
        // column indexes in the ROWS frame
        int[] aCol;
        int[] bCol;     // -1 when the table has a key
        int[] uCol;     // -1 when the column cannot be "unchanged"
        int[] kCol;     // -1 for non-key columns
        String[] names;
        int[] ptypes;

        TableState(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }
    }

    static int physicalOf(Schema s) {
        String p = s.parameters() == null ? null : s.parameters().get("cdc_v2.physical");
        if (p != null) {
            return Integer.parseInt(p);
        }
        switch (s.type()) {
            case BOOLEAN:
                return RowsBuffer.P_BOOL;
            case INT8:
            case INT16:
                return RowsBuffer.P_INT16;
            case INT32:
                return RowsBuffer.P_INT32;
            case INT64:
                return RowsBuffer.P_INT64;
            case FLOAT32:
                return RowsBuffer.P_FLOAT32;
            case FLOAT64:
                return RowsBuffer.P_FLOAT64;
            case BYTES:
                return "org.apache.kafka.connect.data.Decimal".equals(s.name()) ? RowsBuffer.P_UTF8 : RowsBuffer.P_BINARY;
            default:
                return RowsBuffer.P_UTF8;
        }
    }

    static String param(Schema s, String key, String dflt) {
        if (s.parameters() == null) {
            return dflt;
        }
        return s.parameters().getOrDefault(key, dflt);
    }

    static final class Pending {
        final long ack;
        final RecordChangeEvent<SourceRecord> record;

        Pending(long ack, RecordChangeEvent<SourceRecord> record) {
            this.ack = ack;
            this.record = record;
        }
    }

    static final class Consumer implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> {

        final Map<String, TableState> tables = new HashMap<>();
        final ByteBuf meta = new ByteBuf(1 << 16);
        HeldEnds<RecordChangeEvent<SourceRecord>> held;
        boolean ackedSinceLastBatch;
        RowsBuffer current;
        TableState currentTable;
        long seq;
        long lastAck = -1;
        boolean inTxn;
        final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

        @Override
        public void handleBatch(List<RecordChangeEvent<SourceRecord>> batch,
                                DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer)
                throws InterruptedException {
            long cpu0 = threads.getCurrentThreadCpuTime();
            try {
                if (held == null) {
                    held = new HeldEnds<>(settings.ackMaxPending);
                }
                if (!helloSent) {
                    hello();
                }
                if (ackedSinceLastBatch) {
                    // Debezium's poll() that produced this batch ran performCommit() first, which sent the flush of
                    // the previous acknowledgement to the walsender (BaseSourceTask.poll, commitOffset).
                    ackedSinceLastBatch = false;
                    Crash.point(FLUSH_SENT);
                }
                applyDurable(committer);
                if (stopping.get()) {
                    // STOP: nothing more is handed over; what was not acknowledged is delivered again next time.
                    return;
                }
                for (RecordChangeEvent<SourceRecord> e : batch) {
                    handle(e);
                    lastHandedOffset = e.record().sourceOffset();
                }
                fatalSchema = "";
                fatalTable = "";
                flushRows();
                ByteBuf b = new ByteBuf(32);
                b.u32(batch.size());
                b.optU64(ackedPosition < 0 ? null : ackedPosition);
                long w0 = System.nanoTime();
                out.frame(FrameOut.BATCH_END, b);
                out.flush();
                handlerWriteNs.addAndGet(System.nanoTime() - w0);
                batches.incrementAndGet();
                records.addAndGet(batch.size());
                applyDurable(committer);
            }
            catch (IOException e) {
                log("HALT", "reason=frame_pipe_closed");
                Runtime.getRuntime().halt(5);
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (Crash.CrashPointHit e) {
                throw e;
            }
            catch (Throwable t) {
                fatal(t);
            }
            finally {
                handlerCpuNs.addAndGet(threads.getCurrentThreadCpuTime() - cpu0);
            }
        }

        boolean helloSent;

        /**
         * HELLO goes out with the first batch: records exist only after START_REPLICATION has succeeded, so the slot
         * is held by this engine's walsender at this moment (a slot has one holder at a time).
         */
        void hello() throws Exception {
            SlotProbe.Holder h = probe.holder(settings.probeSlot);
            if (h.activePid == null || h.backendStartUs == null) {
                throw new EngineFatal("slot not held at probe");
            }
            ByteBuf b = new ByteBuf(256);
            b.u32(FrameOut.PROTOCOL_VERSION);
            b.str(debeziumVersion);
            b.str(settings.slot);
            b.optU64(settings.startPosition);
            b.u32((int) ProcessHandle.current().pid());
            b.str(settings.engineId);
            b.optU64(h.activePid);
            b.optU64(h.backendStartUs);
            b.str(h.applicationName);
            out.frame(FrameOut.HELLO, b);
            out.flush();
            helloSent = true;
            log("HELLO", "active_pid=" + h.activePid + " application_name=" + Json.quote(h.applicationName));
        }

        /** The newest DURABLE position received; kept, so a DURABLE that matches no held record yet still counts
         *  when such a record arrives later (after a restart the pipeline may send it before the redelivery). */
        Long lastDurable;

        void applyDurable(DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer)
                throws InterruptedException {
            Long p = lastDurable;
            Long d;
            while ((d = durables.poll()) != null) {
                if (p == null || Long.compareUnsigned(d, p) > 0) {
                    p = d;
                }
            }
            if (p == null) {
                return;
            }
            lastDurable = p;
            HeldEnds.Entry<RecordChangeEvent<SourceRecord>> best = held.take(p);
            pendingSize = held.size();
            if (best == null) {
                return;
            }
            Crash.point(ACK_MARKING);
            committer.markProcessed(best.record);
            committer.markBatchFinished();
            ackedPosition = best.ack;
            ackedSinceLastBatch = true;
            Crash.point(ACKED);
        }


        void checkAck(Long ack) {
            if (ack == null) {
                return;
            }
            if (lastAck >= 0 && Long.compareUnsigned(ack, lastAck) < 0) {
                throw new EngineFatal("ack_position decreased from " + lastAck + " to " + ack);
            }
            lastAck = ack;
        }

        static Long ackOf(Map<String, ?> offset) {
            if (offset == null) {
                return null;
            }
            Object c = offset.get("lsn_commit");
            if (c instanceof Number) {
                return ((Number) c).longValue();
            }
            Object p = offset.get("lsn_proc");
            if (p instanceof Number) {
                return ((Number) p).longValue();
            }
            return null;
        }

        static void posmap(ByteBuf b, Map<String, ?> offset) {
            TreeMap<String, Object> sorted = new TreeMap<>();
            if (offset != null) {
                sorted.putAll(offset);
            }
            b.u16(sorted.size());
            for (Map.Entry<String, Object> e : sorted.entrySet()) {
                b.str(e.getKey());
                Object v = e.getValue();
                if (v == null) {
                    b.u8(0);
                }
                else if (v instanceof Number) {
                    b.u8(1);
                    b.u64(((Number) v).longValue());
                }
                else if (v instanceof Boolean) {
                    b.u8(3);
                    b.u8((Boolean) v ? 1 : 0);
                }
                else {
                    b.u8(2);
                    b.str(v.toString());
                }
            }
        }

        void handle(RecordChangeEvent<SourceRecord> e) throws IOException {
            SourceRecord r = e.record();
            Object v = r.value();
            if (v == null) {
                return;   // tombstone (tombstones.on.delete=false makes these rare)
            }
            Schema vs = r.valueSchema();
            String name = vs == null ? null : vs.name();
            if (name != null && name.endsWith(TX_SCHEMA_SUFFIX)) {
                transaction(e, (Struct) v);
                return;
            }
            if (HEARTBEAT_SCHEMA.equals(name) || r.topic().startsWith("__debezium-heartbeat")) {
                heartbeat(e);
                return;
            }
            if (!(v instanceof Struct) || vs.field("op") == null) {
                throw new EngineFatal("unknown record kind: schema " + name + " topic " + r.topic());
            }
            Struct val = (Struct) v;
            String op = val.getString("op");
            switch (op) {
                case "c":
                case "u":
                case "d":
                    row(r, val, vs, op.charAt(0));
                    break;
                case "t":
                    truncate(r, val);
                    break;
                case "m":
                    message(r, val);
                    break;
                default:
                    throw new EngineFatal("unexpected operation " + op + " on topic " + r.topic());
            }
        }

        void transaction(RecordChangeEvent<SourceRecord> e, Struct v) throws IOException {
            flushRows();
            SourceRecord r = e.record();
            String status = v.getString("status");
            Long ack = ackOf(r.sourceOffset());
            Long ts = v.getInt64("ts_ms");
            ByteBuf b = new ByteBuf(256);
            b.u64(seq++);
            b.str(v.getString("id"));
            b.u64(ts == null ? Long.MIN_VALUE : ts);
            posmap(b, r.sourceOffset());
            b.optU64(ack);
            if ("BEGIN".equals(status)) {
                out.frame(FrameOut.BEGIN, b);
                inTxn = true;
            }
            else if ("END".equals(status)) {
                long endAck = HeldEnds.endAck(r.sourceOffset());     // FATAL without lsn_commit (plan C2)
                Long count = v.getInt64("event_count");
                b.u64(count == null ? 0 : count);
                List<Object> cols = v.getArray("data_collections");
                b.u32(cols == null ? 0 : cols.size());
                if (cols != null) {
                    for (Object o : cols) {
                        Struct c = (Struct) o;
                        b.str(c.getString("data_collection"));
                        Long n = c.getInt64("event_count");
                        b.u64(n == null ? 0 : n);
                    }
                }
                checkAck(endAck);
                out.frame(FrameOut.END, b);
                inTxn = false;
                held.add(endAck, e);
                pendingSize = held.size();
            }
            else {
                throw new EngineFatal("unknown transaction status " + status);
            }
        }

        void heartbeat(RecordChangeEvent<SourceRecord> e) throws IOException {
            flushRows();
            // Never held, never acknowledged (plan C2): Debezium dispatches a heartbeat that already carries a
            // transaction's commit position before that transaction's END record. Position fields for information.
            SourceRecord r = e.record();
            ByteBuf b = new ByteBuf(128);
            b.u64(seq++);
            posmap(b, r.sourceOffset());
            out.frame(FrameOut.HEARTBEAT, b);
        }

        void truncate(SourceRecord r, Struct v) throws IOException {
            flushRows();
            Struct src = v.getStruct("source");
            ByteBuf b = new ByteBuf(128);
            b.u64(seq++);
            b.str(src.getString("schema"));
            b.str(src.getString("table"));
            posmap(b, r.sourceOffset());
            out.frame(FrameOut.TRUNCATE, b);
        }

        void message(SourceRecord r, Struct v) throws IOException {
            flushRows();
            Struct m = v.getStruct("message");
            Object content = m.get("content");
            byte[] bytes;
            if (content instanceof ByteBuffer) {
                ByteBuffer bb = ((ByteBuffer) content).duplicate();
                bytes = new byte[bb.remaining()];
                bb.get(bytes);
            }
            else if (content instanceof byte[]) {
                bytes = (byte[]) content;
            }
            else if (content == null) {
                bytes = new byte[0];
            }
            else {
                bytes = content.toString().getBytes(StandardCharsets.UTF_8);
            }
            if (bytes.length > settings.maxValueBytes) {
                throw new EngineFatal("logical message content of " + bytes.length + " bytes exceeds cdc_v2.max.value.bytes");
            }
            String prefix = m.getString("prefix");
            ByteBuf b = new ByteBuf(bytes.length + 256);
            b.u64(seq++);
            b.str(prefix == null ? "" : prefix);
            b.u8(inTxn ? 1 : 0);
            b.bin(bytes);
            posmap(b, r.sourceOffset());
            out.frame(FrameOut.MESSAGE, b);
        }

        TableState tableFor(String schema, String table, Schema rowSchema, Schema keySchema) throws IOException {
            String k = schema + "\u0000" + table;
            TableState t = tables.get(k);
            if (t != null && t.rowSchema == rowSchema && t.keySchema == keySchema) {
                return t;
            }
            if (t != null && t.rowSchema.equals(rowSchema) && java.util.Objects.equals(t.keySchema, keySchema)) {
                t.rowSchema = rowSchema;
                t.keySchema = keySchema;
                return t;
            }
            if (t == null) {
                t = new TableState(schema, table);
                tables.put(k, t);
            }
            flushRows();
            layout(t, rowSchema, keySchema);
            t.version++;
            writeSchema(t);
            return t;
        }

        void layout(TableState t, Schema rowSchema, Schema keySchema) {
            t.rowSchema = rowSchema;
            t.keySchema = keySchema;
            List<Field> fs = rowSchema.fields();
            int n = fs.size();
            t.fields = fs.toArray(new Field[0]);
            t.physical = new int[n];
            t.unchangedPossible = new boolean[n];
            t.isKey = new boolean[n];
            List<Integer> keyPos = new ArrayList<>();
            if (keySchema != null && keySchema.type() == Schema.Type.STRUCT) {
                for (Field kf : keySchema.fields()) {
                    Field f = rowSchema.field(kf.name());
                    if (f == null) {
                        throw new EngineFatal("key field not in the row schema of " + t.schema + "." + t.table);
                    }
                    keyPos.add(f.index());
                    t.isKey[f.index()] = true;
                }
            }
            t.keyPositions = keyPos.stream().mapToInt(Integer::intValue).toArray();
            t.hasKey = t.keyPositions.length > 0;
            List<String> names = new ArrayList<>();
            List<Integer> ptypes = new ArrayList<>();
            names.add("seq");
            ptypes.add(RowsBuffer.P_INT64);
            names.add("op");
            ptypes.add(RowsBuffer.P_UTF8);
            t.aCol = new int[n];
            t.bCol = new int[n];
            t.uCol = new int[n];
            t.kCol = new int[n];
            for (int i = 0; i < n; i++) {
                Schema s = t.fields[i].schema();
                t.physical[i] = physicalOf(s);
                t.unchangedPossible[i] = Boolean.parseBoolean(param(s, "cdc_v2.unchanged", "false"));
                t.aCol[i] = names.size();
                names.add("a" + i);
                ptypes.add(t.physical[i]);
            }
            for (int i = 0; i < n; i++) {
                t.bCol[i] = -1;
                if (!t.hasKey) {
                    t.bCol[i] = names.size();
                    names.add("b" + i);
                    ptypes.add(t.physical[i]);
                }
            }
            for (int i = 0; i < n; i++) {
                t.uCol[i] = -1;
                if (t.unchangedPossible[i]) {
                    t.uCol[i] = names.size();
                    names.add("u" + i);
                    ptypes.add(RowsBuffer.P_BOOL);
                }
            }
            for (int i = 0; i < n; i++) {
                t.kCol[i] = -1;
                if (t.isKey[i]) {
                    t.kCol[i] = names.size();
                    names.add("k" + i);
                    ptypes.add(t.physical[i]);
                }
            }
            t.names = names.toArray(new String[0]);
            t.ptypes = ptypes.stream().mapToInt(Integer::intValue).toArray();
        }

        void writeSchema(TableState t) throws IOException {
            ByteBuf b = new ByteBuf(1024);
            b.str(t.schema);
            b.str(t.table);
            b.u32(t.version);
            b.u8(t.hasKey ? 1 : 0);
            b.u16(t.keyPositions.length);
            for (int p : t.keyPositions) {
                b.u16(p);
            }
            b.u16(t.fields.length);
            for (int i = 0; i < t.fields.length; i++) {
                Field f = t.fields[i];
                Schema s = f.schema();
                b.u16(i);
                b.str(f.name());
                b.str(s.type().name());
                b.str(s.name() == null ? "" : s.name());
                String form = param(s, "cdc_v2.form", null);
                b.str(form != null ? form : "connect:" + s.type().name() + (s.name() == null ? "" : ":" + s.name()));
                b.u8(t.physical[i]);
                b.str(param(s, "cdc_v2.type", ""));
                b.u32(Integer.parseInt(param(s, "cdc_v2.length", "-1")));
                b.u32(Integer.parseInt(param(s, "cdc_v2.scale", "-1")));
                b.u8(Boolean.parseBoolean(param(s, "cdc_v2.nullable", Boolean.toString(s.isOptional()))) ? 1 : 0);
                b.u8(t.isKey[i] ? 1 : 0);
                b.u8(t.unchangedPossible[i] ? 1 : 0);
            }
            out.frame(FrameOut.SCHEMA, b);
        }

        void flushRows() throws IOException {
            if (current != null && current.rows > 0) {
                long w0 = System.nanoTime();
                current.write(out, meta);
                handlerWriteNs.addAndGet(System.nanoTime() - w0);
                rowsOut.addAndGet(current.rows);
                current.clear();
            }
            meta.release(1 << 20);
        }

        void row(SourceRecord r, Struct val, Schema vs, char op) throws IOException {
            Struct src = val.getStruct("source");
            String schema = src.getString("schema");
            String table = src.getString("table");
            fatalSchema = schema;
            fatalTable = table;
            Schema rowSchema = vs.field("after").schema();
            TableState t = tableFor(schema, table, rowSchema, r.keySchema());
            if (currentTable != t || current == null || current.version != t.version) {
                flushRows();
                current = new RowsBuffer(t.schema, t.table, t.version, t.names, t.ptypes);
                currentTable = t;
            }
            Struct oldKey = null;
            if (op == 'c' && t.hasKey) {
                Header h = r.headers().lastWithName("__debezium.oldkey");
                if (h != null && h.value() instanceof Struct) {
                    oldKey = (Struct) h.value();
                }
            }
            long mySeq = seq++;
            Struct after = val.getStruct("after");
            Struct before = val.getStruct("before");
            fill(t, current, mySeq, op, after, before, oldKey);
            if (current.rows > 1 && current.bytes() > settings.frameTargetBytes) {
                // This row would make the frame too large: it goes into a frame of its own.
                current.rollbackRow();
                flushRows();
                fill(t, current, mySeq, op, after, before, oldKey);
            }
            if (current.rows == 1 && current.bytes() > settings.frameMaxBytes - (1 << 20)) {
                throw new EngineFatal("row of " + current.bytes() + " bytes in " + t.schema + "." + t.table
                        + " (seq " + mySeq + ") exceeds cdc_v2.frame.max.bytes");
            }
            if (current.rows >= settings.frameMaxRows || current.bytes() >= settings.frameTargetBytes) {
                flushRows();
            }
        }

        void fill(TableState t, RowsBuffer rb, long mySeq, char op, Struct after, Struct before, Struct oldKey) {
            rb.startRow();
            rb.setI64(0, mySeq);
            rb.setBytes(1, OPS[op], 0, 1);
            int n = t.fields.length;
            for (int i = 0; i < n; i++) {
                Object x = null;
                boolean unchanged = false;
                if (op == 'd') {
                    if (t.hasKey && t.isKey[i] && before != null) {
                        x = before.get(t.fields[i]);
                    }
                }
                else if (after != null) {
                    x = after.get(t.fields[i]);
                }
                if (x == TransportConverter.UNCHANGED_TEXT || x == TransportConverter.UNCHANGED_BYTES) {
                    unchanged = true;
                    x = null;
                }
                if (unchanged && t.uCol[i] < 0) {
                    throw new EngineFatal("unchanged value in column " + i + " of " + t.schema + "." + t.table
                            + ", which cannot be unchanged");
                }
                put(rb, t.aCol[i], t.physical[i], x, t, i, mySeq);
                if (t.uCol[i] >= 0) {
                    rb.setBool(t.uCol[i], unchanged);
                }
                if (t.bCol[i] >= 0) {
                    Object y = (op == 'u' || op == 'd') && before != null ? before.get(t.fields[i]) : null;
                    if (y == TransportConverter.UNCHANGED_TEXT || y == TransportConverter.UNCHANGED_BYTES) {
                        throw new EngineFatal("unchanged value in the old image of " + t.schema + "." + t.table);
                    }
                    put(rb, t.bCol[i], t.physical[i], y, t, i, mySeq);
                }
                if (t.kCol[i] >= 0) {
                    Object k = null;
                    if (oldKey != null) {
                        Field kf = oldKey.schema().field(t.fields[i].name());
                        if (kf != null) {
                            k = oldKey.get(kf);
                        }
                    }
                    put(rb, t.kCol[i], t.physical[i], k, t, i, mySeq);
                }
            }
            rb.endRow();
        }

        static final byte[][] OPS = new byte[128][];

        static {
            OPS['c'] = new byte[] {'c'};
            OPS['u'] = new byte[] {'u'};
            OPS['d'] = new byte[] {'d'};
        }

        void put(RowsBuffer rb, int ci, int p, Object x, TableState t, int pos, long mySeq) {
            if (x == null) {
                rb.setNull(ci);
                return;
            }
            switch (p) {
                case RowsBuffer.P_BOOL:
                    rb.setBool(ci, (Boolean) x);
                    return;
                case RowsBuffer.P_INT16:
                    rb.setI16(ci, ((Number) x).shortValue());
                    return;
                case RowsBuffer.P_INT32:
                    rb.setI32(ci, ((Number) x).intValue());
                    return;
                case RowsBuffer.P_INT64:
                    rb.setI64(ci, ((Number) x).longValue());
                    return;
                case RowsBuffer.P_FLOAT32:
                    rb.setF32(ci, ((Number) x).floatValue());
                    return;
                case RowsBuffer.P_FLOAT64:
                    rb.setF64(ci, ((Number) x).doubleValue());
                    return;
                case RowsBuffer.P_BINARY: {
                    byte[] b;
                    if (x instanceof byte[]) {
                        b = (byte[]) x;
                    }
                    else if (x instanceof ByteBuffer) {
                        ByteBuffer bb = ((ByteBuffer) x).duplicate();
                        b = new byte[bb.remaining()];
                        bb.get(b);
                    }
                    else {
                        b = x.toString().getBytes(StandardCharsets.UTF_8);
                    }
                    checkSize(b.length, t, pos, mySeq);
                    rb.setBytes(ci, b, 0, b.length);
                    return;
                }
                default: {
                    String s = x instanceof String ? (String) x : TransportConverter.text(x);
                    byte[] b = s.getBytes(StandardCharsets.UTF_8);
                    checkSize(b.length, t, pos, mySeq);
                    rb.setBytes(ci, b, 0, b.length);
                }
            }
        }

        void checkSize(long n, TableState t, int pos, long mySeq) {
            if (n > settings.maxValueBytes) {
                throw new EngineFatal("value of " + n + " bytes in column " + pos + " of " + t.schema + "." + t.table
                        + " (seq " + mySeq + ") exceeds cdc_v2.max.value.bytes");
            }
        }
    }

    private CdcEngine() {
    }
}
