package orcbench.metrics;

import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.RunResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Metrics append / aggregate / JSONL read helpers (Checkpoint 6–8).
 * Full report writers live in {@link orcbench.report.ReportWriter}.
 */
public final class MetricsStore {
    private MetricsStore() {}

    public static void appendJsonl(String path, List<RunResult> results) throws Exception {
        if (results == null || results.isEmpty()) {
            return;
        }
        Path p = Paths.get(path);
        if (p.getParent() != null) {
            Files.createDirectories(p.getParent());
        }
        StringBuilder sb = new StringBuilder();
        for (RunResult r : results) {
            sb.append(toJson(r)).append('\n');
        }
        Files.write(p, sb.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * Reads {@code metrics.jsonl} produced by runners / {@link #appendJsonl}.
     * Tolerates missing optional fields (local fixtures / partial cluster dumps).
     */
    public static List<RunResult> readJsonl(String path) throws Exception {
        List<RunResult> out = new ArrayList<RunResult>();
        if (path == null) {
            return out;
        }
        Path p = Paths.get(path);
        if (!Files.isRegularFile(p)) {
            return out;
        }
        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty() || !trimmed.startsWith("{")) {
                continue;
            }
            out.add(parseJsonLine(trimmed));
        }
        return out;
    }

    private static RunResult parseJsonLine(String json) {
        RunResult r = new RunResult();
        r.runId = strField(json, "runId");
        r.experimentId = strField(json, "experimentId");
        r.variantLabel = strField(json, "variantLabel");
        r.engine = strField(json, "engine");
        r.queryId = strField(json, "queryId");
        r.repetition = (int) longField(json, "repetition", 0);
        r.warmup = boolField(json, "warmup", false);
        r.concurrency = (int) longField(json, "concurrency", 1);
        r.cacheState = strFieldNullable(json, "cacheState");
        r.latencyMs = longField(json, "latencyMs", 0);
        r.status = strField(json, "status");
        r.resultHash = strField(json, "resultHash");
        r.logicalRows = longField(json, "logicalRows", 0);
        r.orcFileBytes = longObjField(json, "orcFileBytes");
        r.hdfsBytesRead = longObjField(json, "hdfsBytesRead");
        r.enginePlan = strFieldNullable(json, "enginePlan");
        r.errorMessage = strFieldNullable(json, "errorMessage");
        return r;
    }

    private static String strField(String json, String key) {
        String v = strFieldNullable(json, key);
        return v == null ? "" : v;
    }

    private static String strFieldNullable(String json, String key) {
        String needle = "\"" + key + "\":";
        int i = json.indexOf(needle);
        if (i < 0) {
            return null;
        }
        int start = i + needle.length();
        while (start < json.length() && json.charAt(start) == ' ') {
            start++;
        }
        if (start >= json.length()) {
            return null;
        }
        if (json.startsWith("null", start)) {
            return null;
        }
        if (json.charAt(start) != '"') {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int j = start + 1; j < json.length(); j++) {
            char c = json.charAt(j);
            if (c == '\\' && j + 1 < json.length()) {
                char n = json.charAt(++j);
                if (n == 'n') {
                    sb.append('\n');
                } else {
                    sb.append(n);
                }
                continue;
            }
            if (c == '"') {
                break;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static long longField(String json, String key, long def) {
        Long v = longObjField(json, key);
        return v == null ? def : v.longValue();
    }

    private static Long longObjField(String json, String key) {
        String needle = "\"" + key + "\":";
        int i = json.indexOf(needle);
        if (i < 0) {
            return null;
        }
        int start = i + needle.length();
        while (start < json.length() && json.charAt(start) == ' ') {
            start++;
        }
        if (json.startsWith("null", start)) {
            return null;
        }
        int end = start;
        while (end < json.length()) {
            char c = json.charAt(end);
            if (c == ',' || c == '}' || Character.isWhitespace(c)) {
                break;
            }
            end++;
        }
        try {
            return Long.parseLong(json.substring(start, end));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static boolean boolField(String json, String key, boolean def) {
        String needle = "\"" + key + "\":";
        int i = json.indexOf(needle);
        if (i < 0) {
            return def;
        }
        int start = i + needle.length();
        while (start < json.length() && json.charAt(start) == ' ') {
            start++;
        }
        if (json.startsWith("true", start)) {
            return true;
        }
        if (json.startsWith("false", start)) {
            return false;
        }
        return def;
    }

    public static List<AggregateStats> aggregate(List<RunResult> results, long slaThresholdMs) {
        Map<String, List<RunResult>> groups = new HashMap<String, List<RunResult>>();
        if (results != null) {
            for (RunResult r : results) {
                if (r.warmup || !"ok".equals(r.status)) {
                    continue;
                }
                String key = r.experimentId + "|" + r.variantLabel + "|" + r.engine + "|"
                        + r.queryId + "|" + r.concurrency + "|" + r.cacheState;
                List<RunResult> bucket = groups.get(key);
                if (bucket == null) {
                    bucket = new ArrayList<RunResult>();
                    groups.put(key, bucket);
                }
                bucket.add(r);
            }
        }
        List<AggregateStats> out = new ArrayList<AggregateStats>();
        for (List<RunResult> rows : groups.values()) {
            List<Double> lat = new ArrayList<Double>();
            for (RunResult r : rows) {
                lat.add((double) r.latencyMs);
            }
            Collections.sort(lat);
            int n = lat.size();
            double sum = 0;
            for (double v : lat) {
                sum += v;
            }
            int slaOk = 0;
            for (RunResult r : rows) {
                if (r.latencyMs <= slaThresholdMs) {
                    slaOk++;
                }
            }

            Long orcBytes = null;
            Long hdfs = null;
            for (RunResult r : rows) {
                if (orcBytes == null && r.orcFileBytes != null) {
                    orcBytes = r.orcFileBytes;
                }
                if (hdfs == null && r.hdfsBytesRead != null) {
                    hdfs = r.hdfsBytesRead;
                }
            }
            Double scan = null;
            if (orcBytes != null && hdfs != null && orcBytes > 0) {
                scan = hdfs.doubleValue() / orcBytes.doubleValue();
            }

            RunResult head = rows.get(0);
            AggregateStats a = new AggregateStats();
            a.experimentId = head.experimentId;
            a.variantLabel = head.variantLabel;
            a.engine = head.engine;
            a.queryId = head.queryId;
            a.concurrency = head.concurrency;
            a.cacheState = head.cacheState;
            a.n = n;
            a.avgMs = n == 0 ? 0 : sum / n;
            a.p50Ms = percentile(lat, 0.50);
            a.p95Ms = percentile(lat, 0.95);
            a.p99Ms = percentile(lat, 0.99);
            a.slaSuccessRate = n == 0 ? 0 : (double) slaOk / n;
            a.scanRatio = scan;
            a.resultHash = head.resultHash;
            out.add(a);
        }
        Collections.sort(out, new Comparator<AggregateStats>() {
            @Override
            public int compare(AggregateStats a, AggregateStats b) {
                int c = nullSafe(a.experimentId).compareTo(nullSafe(b.experimentId));
                if (c != 0) {
                    return c;
                }
                c = nullSafe(a.queryId).compareTo(nullSafe(b.queryId));
                if (c != 0) {
                    return c;
                }
                return Integer.compare(a.concurrency, b.concurrency);
            }
        });
        return out;
    }

    public static double percentile(List<Double> sorted, double p) {
        if (sorted == null || sorted.isEmpty()) {
            return 0;
        }
        int idx = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(p * sorted.size()) - 1));
        return sorted.get(idx);
    }

    /**
     * Simple rank by avg latency (then scan ratio) for unit tests / legacy callers.
     * Production BEST_ORC selection uses {@link orcbench.report.BestOrcSelector}
     * ({@code best_orc_rule}: min p50, SLA gate, ZSTD exclusion).
     */
    public static String rankBestOrc(List<AggregateStats> aggregates, Set<String> physicalExperimentIds) {
        if (aggregates == null || physicalExperimentIds == null) {
            return null;
        }
        Map<String, double[]> byExp = new HashMap<String, double[]>();
        Map<String, Integer> counts = new HashMap<String, Integer>();
        for (AggregateStats a : aggregates) {
            if (!physicalExperimentIds.contains(a.experimentId) || a.concurrency != 1) {
                continue;
            }
            double[] cur = byExp.get(a.experimentId);
            if (cur == null) {
                cur = new double[]{0, 0};
                byExp.put(a.experimentId, cur);
            }
            cur[0] += a.avgMs;
            cur[1] += a.scanRatio == null ? 1.0 : a.scanRatio;
            Integer n = counts.get(a.experimentId);
            counts.put(a.experimentId, n == null ? 1 : n + 1);
        }
        String best = null;
        double bestAvg = Double.MAX_VALUE;
        double bestScan = Double.MAX_VALUE;
        for (Map.Entry<String, double[]> e : byExp.entrySet()) {
            int n = counts.get(e.getKey());
            double avg = e.getValue()[0] / n;
            double scan = e.getValue()[1] / n;
            if (avg < bestAvg || (avg == bestAvg && scan < bestScan)) {
                best = e.getKey();
                bestAvg = avg;
                bestScan = scan;
            }
        }
        return best;
    }

    private static String toJson(RunResult r) {
        return "{"
                + "\"runId\":" + q(r.runId)
                + ",\"experimentId\":" + q(r.experimentId)
                + ",\"variantLabel\":" + q(r.variantLabel)
                + ",\"engine\":" + q(r.engine)
                + ",\"queryId\":" + q(r.queryId)
                + ",\"repetition\":" + r.repetition
                + ",\"warmup\":" + r.warmup
                + ",\"concurrency\":" + r.concurrency
                + ",\"cacheState\":" + qn(r.cacheState)
                + ",\"latencyMs\":" + r.latencyMs
                + ",\"status\":" + q(r.status)
                + ",\"resultHash\":" + q(r.resultHash)
                + ",\"logicalRows\":" + r.logicalRows
                + ",\"orcFileBytes\":" + n(r.orcFileBytes)
                + ",\"hdfsBytesRead\":" + n(r.hdfsBytesRead)
                + ",\"enginePlan\":" + qn(truncatePlan(r.enginePlan))
                + ",\"errorMessage\":" + qn(r.errorMessage)
                + "}";
    }

    private static String truncatePlan(String plan) {
        if (plan == null) {
            return null;
        }
        return plan.substring(0, Math.min(500, plan.length()));
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }

    private static String q(String s) {
        return "\"" + escape(s == null ? "" : s) + "\"";
    }

    private static String qn(String s) {
        return s == null ? "null" : q(s);
    }

    private static String n(Number v) {
        return v == null ? "null" : v.toString();
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
