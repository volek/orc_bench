package orcbench.report;

import orcbench.layout.ExperimentExpander;
import orcbench.metrics.MetricsStore;
import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.BestOrcChoice;
import orcbench.model.Models.BestOrcRule;
import orcbench.model.Models.CapabilityReport;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.ReportPhaseResult;
import orcbench.model.Models.RunResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Checkpoint 8 glue: build reports from {@code metrics.jsonl} / in-memory results
 * and expand {@code final_comparison} on the selected {@code BEST_ORC} layout.
 * <p>
 * Local/CI: works from fixtures without a Hadoop cluster. Live final query
 * execution remains cluster-side (use {@code --dry-run} / {@code --mock-run} here).
 */
public final class ReportPhase {
    private ReportPhase() {}

    public static ReportPhaseResult report(
            LoadedConfig cfg,
            ExperimentPlan plan,
            String scale,
            String resultsDir,
            String runId,
            String metricsJsonlPath,
            List<RunResult> inMemoryResults,
            List<String[]> skipped,
            CapabilityReport capability,
            boolean dryRun,
            boolean includeFinalComparison) throws Exception {

        ReportPhaseResult out = new ReportPhaseResult();
        out.phase = includeFinalComparison ? "final" : "report";
        out.scale = scale == null ? "S" : scale.toUpperCase(Locale.ROOT);
        out.dryRun = dryRun;
        out.runId = (runId == null || runId.trim().isEmpty())
                ? "report_" + UUID.randomUUID().toString().substring(0, 8)
                : runId;

        long slaMs = cfg.benchmark.metrics.slaSuccessThresholdMs;
        if (slaMs <= 0) {
            slaMs = cfg.benchmark.benchmark.slaMeanLatencyMs;
        }

        List<RunResult> results = new ArrayList<RunResult>();
        if (inMemoryResults != null) {
            results.addAll(inMemoryResults);
        }
        Path metricsPath = resolveMetricsPath(resultsDir, out.runId, metricsJsonlPath);
        if (results.isEmpty() && metricsPath != null && Files.isRegularFile(metricsPath)) {
            results.addAll(MetricsStore.readJsonl(metricsPath.toString()));
            out.notes.add("loaded metrics from " + metricsPath.toAbsolutePath());
        } else if (results.isEmpty() && dryRun) {
            out.notes.add("dry-run report: no metrics.jsonl — writing empty aggregates / plan only");
        } else if (results.isEmpty() && metricsJsonlPath != null) {
            throw new IllegalArgumentException("metrics file not found: " + metricsJsonlPath);
        } else if (results.isEmpty() && !dryRun) {
            out.notes.add("warning: no metrics rows; reports will be sparse "
                    + "(pass --metrics-jsonl, --run-id with metrics.jsonl, or --dry-run)");
        }
        out.metricsJsonlPath = metricsPath == null ? null : metricsPath.toString();
        out.resultRows = results.size();

        List<AggregateStats> aggregates = MetricsStore.aggregate(results, slaMs);
        out.aggregateRows = aggregates.size();

        List<String[]> skipRows = new ArrayList<String[]>();
        if (skipped != null) {
            skipRows.addAll(skipped);
        }
        for (RunResult r : results) {
            if (r != null && "skipped".equals(r.status)) {
                skipRows.add(new String[]{
                        r.experimentId + "/" + nullSafe(r.variantLabel),
                        r.errorMessage == null ? "skipped" : r.errorMessage
                });
            }
        }
        if (plan != null) {
            collectPlanSkips(plan.physical, skipRows);
            collectPlanSkips(plan.engine, skipRows);
        }
        out.skippedRows = skipRows.size();

        BestOrcRule rule = cfg.benchmark.bestOrcRule;
        List<ExperimentInstance> physical = plan == null ? null : plan.physical;
        BestOrcChoice best = BestOrcSelector.select(aggregates, rule, physical);
        out.bestOrc = best;
        if (best.experimentId != null) {
            out.notes.add("BEST_ORC=" + best.experimentId + "/" + nullSafe(best.variantLabel)
                    + " layout=" + nullSafe(best.layoutPath)
                    + " p50=" + String.format(Locale.ROOT, "%.2f", best.meanP50Ms)
                    + " sla=" + String.format(Locale.ROOT, "%.4f", best.meanSlaSuccessRate));
        } else {
            out.notes.add("BEST_ORC not selected (insufficient metrics)");
        }

        List<ExperimentInstance> finalCells = new ArrayList<ExperimentInstance>();
        if (includeFinalComparison) {
            String bestPath = best.layoutPath;
            if (bestPath == null || bestPath.trim().isEmpty()) {
                bestPath = cfg.benchmark.benchmark.hdfsRoot + "/" + out.scale + "/"
                        + cfg.benchmark.physicalBaseline.id;
                out.notes.add("final_comparison fallback layout=" + bestPath
                        + " (BEST_ORC layout unknown)");
            }
            finalCells = ExperimentExpander.finalProfiles(cfg.benchmark, bestPath);
            // Hive must never recommend/use ZSTD BEST_ORC — already excluded in selector.
            for (ExperimentInstance cell : finalCells) {
                if (cell != null
                        && cell.engine != null
                        && cell.engine.startsWith("hive")
                        && bestPath != null
                        && bestPath.toUpperCase(Locale.ROOT).contains("ZSTD")) {
                    cell.skipReason = "Hive ZSTD deferred (ORC 1.5.6); BEST_ORC must not be ZSTD";
                }
            }
            if (plan != null) {
                plan.finalComparison = finalCells;
            }
            out.finalComparisonCells = finalCells.size();
            if (dryRun) {
                out.notes.add("dry-run final_comparison cells=" + finalCells.size()
                        + " on layout=" + bestPath);
                ExperimentExpander.printPlan(
                        plan != null ? plan : stubPlan(out.scale, finalCells),
                        System.out);
            }
        }

        out.slaGateOverall = true;
        for (AggregateStats a : aggregates) {
            if (a.slaSuccessRate < 0.95) {
                out.slaGateOverall = false;
                break;
            }
        }

        boolean rewriteJsonl = metricsPath == null
                || !Files.isRegularFile(metricsPath)
                || (inMemoryResults != null && !inMemoryResults.isEmpty());
        Path dir = ReportWriter.writeRun(
                resultsDir,
                out.runId,
                out.scale,
                results,
                aggregates,
                skipRows,
                capability,
                best,
                includeFinalComparison ? finalCells : null,
                out.notes,
                slaMs,
                rewriteJsonl && !results.isEmpty());
        out.outputDir = dir.toString();
        if (out.metricsJsonlPath == null) {
            Path written = dir.resolve("metrics.jsonl");
            if (Files.isRegularFile(written)) {
                out.metricsJsonlPath = written.toString();
            }
        }
        return out;
    }

    public static Path resolveMetricsPath(String resultsDir, String runId, String metricsJsonlPath) {
        if (metricsJsonlPath != null && !metricsJsonlPath.trim().isEmpty()) {
            return Paths.get(metricsJsonlPath);
        }
        if (runId != null && !runId.trim().isEmpty()) {
            return Paths.get(resultsDir == null ? "results" : resultsDir, runId, "metrics.jsonl");
        }
        return null;
    }

    private static void collectPlanSkips(List<ExperimentInstance> cells, List<String[]> skipRows) {
        if (cells == null) {
            return;
        }
        for (ExperimentInstance i : cells) {
            if (i != null && i.skipReason != null) {
                skipRows.add(new String[]{
                        i.experimentId + "/" + nullSafe(i.variantLabel),
                        i.skipReason
                });
            }
        }
    }

    private static ExperimentPlan stubPlan(String scale, List<ExperimentInstance> finalCells) {
        ExperimentPlan p = new ExperimentPlan();
        p.scale = scale;
        p.finalComparison = finalCells;
        return p;
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }
}
