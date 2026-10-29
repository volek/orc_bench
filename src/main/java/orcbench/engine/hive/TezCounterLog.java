package orcbench.engine.hive;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads {@code HDFS_BYTES_READ} out of a Hive/Tez execution summary.
 * The JDBC result set does not carry Tez counters; they show up in the
 * statement log when {@code hive.tez.exec.print.summary=true}.
 */
public final class TezCounterLog {
    private static final Pattern HDFS_BYTES = Pattern.compile(
            "HDFS_BYTES_READ\\s*[=:]\\s*(\\d+)");

    private TezCounterLog() {}

    /**
     * Sum of every {@code HDFS_BYTES_READ} value in {@code lines}.
     * Returns null when the counter is absent — a missing log is not zero bytes.
     */
    public static Long sumHdfsBytesRead(List<String> lines) {
        if (lines == null || lines.isEmpty()) {
            return null;
        }
        long sum = 0L;
        boolean seen = false;
        for (String line : lines) {
            if (line == null || line.isEmpty()) {
                continue;
            }
            Matcher m = HDFS_BYTES.matcher(line);
            while (m.find()) {
                seen = true;
                sum += Long.parseLong(m.group(1));
            }
        }
        return seen ? Long.valueOf(sum) : null;
    }

    /**
     * A concurrency cell with one of these errors is not a load measurement.
     * One Tez AM shutdown or a missing jar in {@code _tez_session_dir} takes
     * the neighbouring slots with it; the p50 of the survivors is not throughput.
     */
    public static boolean invalidatesLoadMeasurement(String errorMessage) {
        if (errorMessage == null || errorMessage.isEmpty()) {
            return false;
        }
        if (errorMessage.contains("DAG_KILL") || errorMessage.contains("DAG_TERMINATE")) {
            return true;
        }
        boolean missingJar = errorMessage.contains("FileNotFoundException")
                || errorMessage.contains("FileNotFound");
        return missingJar
                && (errorMessage.contains("_tez_session_dir")
                || errorMessage.contains("hive-hcatalog-core.jar"));
    }
}
