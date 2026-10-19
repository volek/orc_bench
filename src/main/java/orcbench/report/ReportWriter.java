package orcbench.report;

import orcbench.metrics.MetricsStore;
import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.BestOrcChoice;
import orcbench.model.Models.CapabilityReport;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.RunResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Checkpoint 8 report artefacts under {@code results/<runId>/}:
 * {@code SUMMARY.txt}, {@code summary.csv}, {@code metrics.jsonl}, {@code skipped.tsv},
 * plus {@code final_comparison.tsv} when final cells are provided.
 */
public final class ReportWriter {
    private ReportWriter() {}

    public static Path writeRun(
            String resultsDir,
            String runId,
            String scale,
            List<RunResult> results,
            List<AggregateStats> aggregates,
            List<String[]> skipped,
            CapabilityReport capability,
            BestOrcChoice bestOrc,
            List<ExperimentInstance> finalCells,
            List<String> notes,
            long slaThresholdMs,
            boolean rewriteMetricsJsonl) throws Exception {

        Path dir = Paths.get(resultsDir == null ? "results" : resultsDir, runId);
        Files.createDirectories(dir);

        if (rewriteMetricsJsonl && results != null && !results.isEmpty()) {
            Path metrics = dir.resolve("metrics.jsonl");
            Files.deleteIfExists(metrics);
            MetricsStore.appendJsonl(metrics.toString(), results);
        }

        writeSummaryCsv(dir.resolve("summary.csv"), aggregates);
        writeSkippedTsv(dir.resolve("skipped.tsv"), skipped);
        if (finalCells != null && !finalCells.isEmpty()) {
            writeFinalComparisonTsv(dir.resolve("final_comparison.tsv"), finalCells, bestOrc);
        }
        writeSummaryTxt(
                dir.resolve("SUMMARY.txt"),
                runId,
                scale,
                results == null ? 0 : results.size(),
                aggregates == null ? 0 : aggregates.size(),
                skipped == null ? 0 : skipped.size(),
                bestOrc,
                capability,
                finalCells,
                notes,
                slaThresholdMs,
                aggregates);

        System.out.println("[report] wrote " + dir.toAbsolutePath());
        return dir;
    }

    private static void writeSummaryCsv(Path path, List<AggregateStats> aggregates) throws Exception {
        StringBuilder csv = new StringBuilder();
        csv.append("experimentId,variantLabel,engine,queryId,concurrency,cacheState,")
                .append("n,avgMs,p50Ms,p95Ms,p99Ms,slaSuccessRate,scanRatio,resultHash,slaPass\n");
        if (aggregates != null) {
            for (AggregateStats a : aggregates) {
                boolean slaPass = a.slaSuccessRate >= 0.95;
                csv.append(csvCell(a.experimentId)).append(',')
                        .append(csvCell(a.variantLabel)).append(',')
                        .append(csvCell(a.engine)).append(',')
                        .append(csvCell(a.queryId)).append(',')
                        .append(a.concurrency).append(',')
                        .append(csvCell(a.cacheState)).append(',')
                        .append(a.n).append(',')
                        .append(fmt(a.avgMs)).append(',')
                        .append(fmt(a.p50Ms)).append(',')
                        .append(fmt(a.p95Ms)).append(',')
                        .append(fmt(a.p99Ms)).append(',')
                        .append(String.format(Locale.ROOT, "%.4f", a.slaSuccessRate)).append(',')
                        .append(a.scanRatio == null ? "" : String.format(Locale.ROOT, "%.6f", a.scanRatio))
                        .append(',')
                        .append(csvCell(a.resultHash)).append(',')
                        .append(slaPass).append('\n');
            }
        }
        Files.write(path, csv.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void writeSkippedTsv(Path path, List<String[]> skipped) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("experiment\treason\n");
        if (skipped != null) {
            for (String[] row : skipped) {
                if (row == null || row.length == 0) {
                    continue;
                }
                sb.append(row[0] == null ? "" : row[0]).append('\t');
                sb.append(row.length > 1 && row[1] != null ? row[1] : "").append('\n');
            }
        }
        Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void writeFinalComparisonTsv(
            Path path, List<ExperimentInstance> finalCells, BestOrcChoice bestOrc) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("engineProfile\tengine\tlayoutPath\tqueries\tskipReason\n");
        for (ExperimentInstance i : finalCells) {
            if (i == null) {
                continue;
            }
            String layout = i.layoutPath;
            if (layout == null && bestOrc != null) {
                layout = bestOrc.layoutPath;
            }
            sb.append(nullSafe(i.experimentId)).append('\t')
                    .append(nullSafe(i.engine)).append('\t')
                    .append(nullSafe(layout)).append('\t')
                    .append(i.queries == null ? 0 : i.queries.size()).append('\t')
                    .append(nullSafe(i.skipReason)).append('\n');
        }
        Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void writeSummaryTxt(
            Path path,
            String runId,
            String scale,
            int results,
            int aggregates,
            int skipped,
            BestOrcChoice bestOrc,
            CapabilityReport capability,
            List<ExperimentInstance> finalCells,
            List<String> notes,
            long slaThresholdMs,
            List<AggregateStats> aggList) throws Exception {

        StringBuilder summary = new StringBuilder();
        summary.append("runId=").append(runId).append('\n');
        summary.append("scale=").append(scale == null ? "" : scale).append('\n');
        summary.append("generatedAt=").append(Instant.now()).append('\n');
        summary.append("results=").append(results).append('\n');
        summary.append("aggregates=").append(aggregates).append('\n');
        summary.append("skipped=").append(skipped).append('\n');
        summary.append("slaThresholdMs=").append(slaThresholdMs).append('\n');
        summary.append("slaGateOverall=").append(overallSla(aggList)).append('\n');

        if (bestOrc != null && bestOrc.experimentId != null) {
            summary.append("bestOrc.experimentId=").append(bestOrc.experimentId).append('\n');
            summary.append("bestOrc.variantLabel=")
                    .append(bestOrc.variantLabel == null ? "" : bestOrc.variantLabel).append('\n');
            summary.append("bestOrc.layoutPath=")
                    .append(bestOrc.layoutPath == null ? "" : bestOrc.layoutPath).append('\n');
            summary.append("bestOrc.meanP50Ms=")
                    .append(String.format(Locale.ROOT, "%.2f", bestOrc.meanP50Ms)).append('\n');
            summary.append("bestOrc.meanSlaSuccessRate=")
                    .append(String.format(Locale.ROOT, "%.4f", bestOrc.meanSlaSuccessRate)).append('\n');
            summary.append("bestOrc.slaPassed=").append(bestOrc.slaPassed).append('\n');
            summary.append("bestOrc.tieBreakClass=")
                    .append(bestOrc.tieBreakClass == null ? "" : bestOrc.tieBreakClass).append('\n');
        } else {
            summary.append("bestOrc=\n");
        }

        summary.append("finalComparisonCells=")
                .append(finalCells == null ? 0 : finalCells.size()).append('\n');
        if (finalCells != null) {
            for (ExperimentInstance i : finalCells) {
                summary.append("final.")
                        .append(i.experimentId)
                        .append(".engine=").append(i.engine)
                        .append(" layout=").append(i.layoutPath == null ? "" : i.layoutPath);
                if (i.skipReason != null) {
                    summary.append(" SKIP=").append(i.skipReason);
                }
                summary.append('\n');
            }
        }

        if (capability != null) {
            summary.append("capabilities:\n");
            for (Map.Entry<String, Boolean> e : capability.capabilities.entrySet()) {
                summary.append("  ").append(e.getKey()).append('=').append(e.getValue()).append('\n');
            }
            for (String n : capability.notes) {
                summary.append("note: ").append(n).append('\n');
            }
        }
        if (bestOrc != null) {
            for (String n : bestOrc.notes) {
                summary.append("bestOrc.note: ").append(n).append('\n');
            }
        }
        if (notes != null) {
            for (String n : notes) {
                summary.append("note: ").append(n).append('\n');
            }
        }
        Files.write(path, summary.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static boolean overallSla(List<AggregateStats> aggregates) {
        if (aggregates == null || aggregates.isEmpty()) {
            return true;
        }
        int ok = 0;
        for (AggregateStats a : aggregates) {
            if (a.slaSuccessRate >= 0.95) {
                ok++;
            }
        }
        return ok == aggregates.size();
    }

    private static String fmt(double v) {
        return String.format(Locale.ROOT, "%.2f", v);
    }

    private static String csvCell(String s) {
        if (s == null) {
            return "";
        }
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }
}
