package cdc_v2.engine;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Named crash points, Java side of the environment contract in docs/testing/harness.md section 1 (the Python side
 * is src/cdc_v2/crashpoints.py; keep both in step).
 *
 * <p>Variables, read once by {@link #init}: CDC_V2_CRASH_POINT, CDC_V2_CRASH_HIT (N or seed:S:M), CDC_V2_CRASH_ACTION
 * (kill, kill_later, stop, hang, raise), CDC_V2_CRASH_DELAY_MS, CDC_V2_CRASH_LOG, CDC_V2_CRASH_TAG. An invalid value
 * fails at start-up. A point fires at most once per process. kill and stop signal this process with the real
 * signal (SIGKILL, SIGSTOP) through /bin/kill, because Runtime.halt is an exit, not a kill.
 */
final class Crash {

    static final String[] ACTIONS = {"kill", "kill_later", "stop", "hang", "raise"};
    private static final Pattern NAME = Pattern.compile("^[a-z][a-z0-9_]*(\\.[a-z0-9_]+)+$");
    private static final Pattern TAG = Pattern.compile("^[A-Za-z0-9_.-]{0,64}$");
    private static final Pattern HIT_SEED = Pattern.compile("^seed:(\\d+):(\\d+)$");

    private static final Map<String, String> DECLARED = new LinkedHashMap<>();

    /** Thrown at the firing hit with the action raise. An Error, so that catch (Exception) does not swallow it. */
    static final class CrashPointHit extends Error {
        CrashPointHit(String point, long hit) {
            super("crash point " + point + " fired at hit " + hit);
        }
    }

    static synchronized String declare(String name, String description) {
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("bad crash point name " + name);
        }
        if (description == null || description.isBlank() || description.contains("\n")) {
            throw new IllegalArgumentException("crash point " + name + " needs a one-line description");
        }
        String old = DECLARED.putIfAbsent(name, description);
        if (old != null && !old.equals(description)) {
            throw new IllegalArgumentException("crash point " + name + " declared twice with different descriptions");
        }
        return name;
    }

    static synchronized Map<String, String> declared() {
        return new LinkedHashMap<>(DECLARED);
    }

    static long splitmix64(long x) {
        x += 0x9E3779B97F4A7C15L;
        long z = x;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    static long hitFromSeed(long seed, long max) {
        if (max < 1) {
            throw new IllegalArgumentException("max hit must be >= 1");
        }
        return 1 + Long.remainderUnsigned(splitmix64(seed), max);
    }

    static long parseHit(String text) {
        text = text == null ? "" : text.trim();
        if (text.isEmpty()) {
            return 1;
        }
        Matcher m = HIT_SEED.matcher(text);
        if (m.matches()) {
            long seed;
            long max;
            try {
                seed = Long.parseUnsignedLong(m.group(1));
                max = Long.parseLong(m.group(2));
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("CDC_V2_CRASH_HIT=" + text + ": seed must fit 64 bits");
            }
            if (max < 1) {
                throw new IllegalArgumentException("CDC_V2_CRASH_HIT=" + text + ": M must be >= 1");
            }
            return hitFromSeed(seed, max);
        }
        if (!text.chars().allMatch(Character::isDigit)) {
            throw new IllegalArgumentException("CDC_V2_CRASH_HIT=" + text + ": expected N >= 1 or seed:S:M");
        }
        long n;
        try {
            n = Long.parseLong(text);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("CDC_V2_CRASH_HIT=" + text + ": too large");
        }
        if (n < 1) {
            throw new IllegalArgumentException("CDC_V2_CRASH_HIT=" + text + ": expected N >= 1");
        }
        return n;
    }

    private static volatile State state;

    private static final class State {
        final String point;
        final long target;
        final String action;
        final long delayMs;
        final String tag;
        final FileOutputStream log;
        final Map<String, Long> counts = new HashMap<>();
        boolean fired;

        State(String point, long target, String action, long delayMs, String tag, FileOutputStream log) {
            this.point = point;
            this.target = target;
            this.action = action;
            this.delayMs = delayMs;
            this.tag = tag;
            this.log = log;
        }
    }

    /** Reads the environment contract. Throws IllegalArgumentException for an invalid value. */
    static synchronized boolean init(Map<String, String> env) throws IOException {
        String point = trim(env.get("CDC_V2_CRASH_POINT"));
        String logPath = trim(env.get("CDC_V2_CRASH_LOG"));
        String action = trim(env.get("CDC_V2_CRASH_ACTION"));
        if (action.isEmpty()) {
            action = "kill";
        }
        String tag = trim(env.get("CDC_V2_CRASH_TAG"));
        if (!point.isEmpty() && !NAME.matcher(point).matches()) {
            throw new IllegalArgumentException("CDC_V2_CRASH_POINT=" + point + " is not a crash point name");
        }
        boolean known = false;
        for (String a : ACTIONS) {
            known |= a.equals(action);
        }
        if (!known) {
            throw new IllegalArgumentException("CDC_V2_CRASH_ACTION=" + action + ": expected one of kill, kill_later, stop, hang, raise");
        }
        if (!TAG.matcher(tag).matches()) {
            throw new IllegalArgumentException("CDC_V2_CRASH_TAG=" + tag + ": [A-Za-z0-9_.-], at most 64 characters");
        }
        long hit = parseHit(env.get("CDC_V2_CRASH_HIT"));
        String delay = trim(env.get("CDC_V2_CRASH_DELAY_MS"));
        if (delay.isEmpty()) {
            delay = "0";
        }
        if (!delay.chars().allMatch(Character::isDigit)) {
            throw new IllegalArgumentException("CDC_V2_CRASH_DELAY_MS=" + delay + ": expected milliseconds >= 0");
        }
        if (point.isEmpty() && logPath.isEmpty()) {
            state = null;
            return false;
        }
        FileOutputStream log = logPath.isEmpty() ? null : new FileOutputStream(logPath, true);
        state = new State(point, hit, action, Long.parseLong(delay), tag, log);
        return true;
    }

    private static String trim(String s) {
        return s == null ? "" : s.trim();
    }

    static boolean active() {
        return state != null;
    }

    /** Marks a crash point. Returns at once unless the environment switched the harness on. */
    static void point(String name) {
        State s = state;
        if (s == null) {
            return;
        }
        boolean fire;
        long n;
        synchronized (Crash.class) {
            if (!DECLARED.containsKey(name)) {
                throw new IllegalStateException("crash point " + name + " is not declared");
            }
            n = s.counts.merge(name, 1L, Long::sum);
            fire = !s.fired && name.equals(s.point) && n == s.target;
            if (fire) {
                s.fired = true;
            }
            if (s.log != null) {
                java.time.Instant now = java.time.Instant.now();
                String t = String.format(Locale.ROOT, "%d.%06d", now.getEpochSecond(), now.getNano() / 1000);
                String line = "{\"point\":\"" + name + "\",\"hit\":" + n + ",\"pid\":" + ProcessHandle.current().pid()
                        + ",\"tag\":\"" + s.tag + "\",\"t\":" + t + ",\"fire\":" + fire + ",\"action\":\""
                        + (fire ? s.action : "") + "\"}\n";
                try {
                    s.log.write(line.getBytes(StandardCharsets.UTF_8));
                }
                catch (IOException e) {
                    throw new IllegalStateException("cannot write the crash hit log", e);
                }
            }
        }
        if (fire) {
            act(s.action, name, n, s.delayMs);
        }
    }

    private static void signalSelf(String sig) {
        try {
            Process p = new ProcessBuilder("/bin/kill", "-" + sig, Long.toString(ProcessHandle.current().pid()))
                    .redirectErrorStream(true).start();
            p.waitFor();
        }
        catch (IOException e) {
            throw new IllegalStateException("cannot signal this process", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void blockForever() {
        while (true) {
            try {
                Thread.sleep(3_600_000L);
            }
            catch (InterruptedException e) {
                // keep blocking: a crash point never returns
            }
        }
    }

    private static void act(String action, String name, long hit, long delayMs) {
        switch (action) {
            case "kill":
                signalSelf("KILL");
                blockForever();
                break;
            case "kill_later":
                Thread t = new Thread(() -> {
                    try {
                        Thread.sleep(delayMs);
                    }
                    catch (InterruptedException e) {
                        // kill anyway
                    }
                    signalSelf("KILL");
                }, "cdc_v2-crash-kill");
                t.setDaemon(true);
                t.start();
                break;
            case "stop":
                signalSelf("STOP");
                break;
            case "hang":
                blockForever();
                break;
            case "raise":
                throw new CrashPointHit(name, hit);
            default:
                throw new IllegalStateException("unknown action " + action);
        }
    }

    /** JSON list of the declared points, in the format of python -m cdc_v2.crashpoints. */
    static String listJson() {
        StringBuilder b = new StringBuilder("[");
        boolean first = true;
        for (Map.Entry<String, String> e : declared().entrySet()) {
            if (!first) {
                b.append(",");
            }
            first = false;
            b.append("{\"name\":").append(Json.quote(e.getKey())).append(",\"description\":")
                    .append(Json.quote(e.getValue())).append(",\"module\":\"jvm:cdc_v2.engine\",\"once\":false}");
        }
        return b.append("]").toString();
    }

    private Crash() {
    }
}
