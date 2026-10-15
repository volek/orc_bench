package orcbench.report;

import orcbench.metrics.MetricsStore;
import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.CapabilityReport;
import orcbench.model.Models.RunResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class ReportWriter {
    private ReportWriter() {}

    public static String writeRun(
            String resultsDir, String runId, List<RunResult> results, List<AggregateStats> aggregates,
            List<String[]> skipped, CapabilityReport capability, String bestOrc) throws Exception {

        Path dir = Paths.get(resultsDir, runId);
        Files.createDirectories(dir);
        MetricsStore.appendJsonl(dir.resolve("metrics.jsonl").toString(), results);

        StringBuilder csv = new StringBuilder();
        csv.append("experimentId,variantLabel,engine,queryId,concurrency,cacheState,n,avgMs,p50Ms,p95Ms,p99Ms,slaSuccessRate,scanRatio,resultHash\n");
        for (AggregateStats a : aggregates) {
            csv.append(a.experimentId).append(',')
                    .append(escape(a.variantLabel)).append(',')
                    .append(a.engine).append(',')
                    .append(a.queryId).append(',')
                    .append(a.concurrency).append(',')
                    .append(a.cacheState == null ? "" : a.cacheState).append(',')
                    .append(a.n).append(',')
                    .append(String.format("%.2f", a.avgMs)).append(',')
                    .append(String.format("%.2f", a.p50Ms)).append(',')
                    .append(String.format("%.2f", a.p95Ms)).append(',')
                    .append(String.format("%.2f", a.p99Ms)).append(',')
                    .append(String.format("%.4f", a.slaSuccessRate)).append(',')
                    .append(a.scanRatio == null ? "" : String.format("%.6f", a.scanRatio)).append(',')
                    .append(a.resultHash).append('\n');
        }
        Files.write(dir.resolve("summary.csv"), csv.toString().getBytes(StandardCharsets.UTF_8));

        StringBuilder skippedTxt = new StringBuilder();
        for (String[] s : skipped) skippedTxt.append(s[0]).append('\t').append(s[1]).append('\n');
        Files.write(dir.resolve("skipped.tsv"), skippedTxt.toString().getBytes(StandardCharsets.UTF_8));

        StringBuilder summary = new StringBuilder();
        summary.append("runId=").append(runId).append('\n');
        summary.append("generatedAt=").append(Instant.now()).append('\n');
        summary.append("results=").append(results.size()).append('\n');
        summary.append("aggregates=").append(aggregates.size()).append('\n');
        summary.append("skipped=").append(skipped.size()).append('\n');
        if (bestOrc != null) summary.append("bestOrc=").append(bestOrc).append('\n');
        if (capability != null) {
            summary.append("capabilities:\n");
            for (Map.Entry<String, Boolean> e : capability.capabilities.entrySet()) {
                summary.append("  ").append(e.getKey()).append('=').append(e.getValue()).append('\n');
            }
            for (String n : capability.notes) summary.append("note: ").append(n).append('\n');
        }
        Files.write(dir.resolve("SUMMARY.txt"), summary.toString().getBytes(StandardCharsets.UTF_8));
        System.out.println("[report] wrote " + dir.toAbsolutePath());
        return dir.toString();
    }

    private static String escape(String s) {
        if (s.contains(",") || s.contains("\"")) return "\"" + s.replace("\"", "\"\"") + "\"";
        return s;
    }
}
