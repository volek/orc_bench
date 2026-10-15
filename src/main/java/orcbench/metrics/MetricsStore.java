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

public final class MetricsStore {
    private MetricsStore() {}

    public static void appendJsonl(String path, List<RunResult> results) throws Exception {
        Path p = Paths.get(path);
        if (p.getParent() != null) Files.createDirectories(p.getParent());
        StringBuilder sb = new StringBuilder();
        for (RunResult r : results) sb.append(toJson(r)).append('\n');
        Files.write(p, sb.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public static List<AggregateStats> aggregate(List<RunResult> results, long slaThresholdMs) {
        Map<String, List<RunResult>> groups = new HashMap<>();
        for (RunResult r : results) {
            if (r.warmup || !"ok".equals(r.status)) continue;
            String key = r.experimentId + "|" + r.variantLabel + "|" + r.engine + "|"
                    + r.queryId + "|" + r.concurrency + "|" + r.cacheState;
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
        }
        List<AggregateStats> out = new ArrayList<>();
        for (List<RunResult> rows : groups.values()) {
            List<Double> lat = new ArrayList<>();
            for (RunResult r : rows) lat.add((double) r.latencyMs);
            Collections.sort(lat);
            int n = lat.size();
            double sum = 0;
            for (double v : lat) sum += v;
            int slaOk = 0;
            for (RunResult r : rows) if (r.latencyMs <= slaThresholdMs) slaOk++;

            Long orcBytes = null;
            Long hdfs = null;
            for (RunResult r : rows) {
                if (orcBytes == null && r.orcFileBytes != null) orcBytes = r.orcFileBytes;
                if (hdfs == null && r.hdfsBytesRead != null) hdfs = r.hdfsBytesRead;
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
        out.sort(Comparator.comparing((AggregateStats a) -> a.experimentId)
                .thenComparing(a -> a.queryId)
                .thenComparingInt(a -> a.concurrency));
        return out;
    }

    public static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) return 0;
        int idx = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(p * sorted.size()) - 1));
        return sorted.get(idx);
    }

    public static String rankBestOrc(List<AggregateStats> aggregates, Set<String> physicalExperimentIds) {
        Map<String, double[]> byExp = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();
        for (AggregateStats a : aggregates) {
            if (!physicalExperimentIds.contains(a.experimentId) || a.concurrency != 1) continue;
            double[] cur = byExp.getOrDefault(a.experimentId, new double[]{0, 0});
            cur[0] += a.avgMs;
            cur[1] += a.scanRatio == null ? 1.0 : a.scanRatio;
            byExp.put(a.experimentId, cur);
            counts.put(a.experimentId, counts.getOrDefault(a.experimentId, 0) + 1);
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
                + ",\"enginePlan\":" + qn(r.enginePlan == null ? null : r.enginePlan.substring(0, Math.min(500, r.enginePlan.length())))
                + ",\"errorMessage\":" + qn(r.errorMessage)
                + "}";
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
