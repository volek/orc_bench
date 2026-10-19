package orcbench;

import orcbench.config.CliOptions;
import orcbench.config.ConfigLoader;
import orcbench.engine.hive.HiveJdbcRunner;
import orcbench.engine.spark.SparkQueryRunner;
import orcbench.generate.DatasetGenerator;
import orcbench.layout.ExperimentExpander;
import orcbench.layout.LayoutMaterializer;
import orcbench.model.Models.CapabilityReport;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.GenerateResult;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.MaterializeResult;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.ReportPhaseResult;
import orcbench.model.Models.RunPhaseResult;
import orcbench.model.Models.RunResult;
import orcbench.preflight.PreflightProbe;
import orcbench.preflight.PreflightReportWriter;
import orcbench.report.ReportPhase;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Checkpoint 8: {@code --phase report} builds SUMMARY/CSV/skipped from
 * {@code metrics.jsonl} / fixtures; {@code --phase final} selects BEST_ORC
 * (no Hive ZSTD) and expands {@code final_comparison}. Live cluster runs
 * remain node-side; local hosts use dry-run / mock / fixture metrics.
 */
public final class Main {
    private Main() {}

    public static void main(String[] args) {
        CliOptions.CliArgs cli = CliOptions.parse(args);
        if (cli == null) {
            System.exit(2);
            return;
        }
        try {
            run(cli);
        } catch (Throwable ex) {
            System.err.println("[fatal] " + ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

    static void run(CliOptions.CliArgs cli) throws Exception {
        System.out.println("[orc-bench] Checkpoint 8 — reporting / BEST_ORC / final_comparison");
        System.out.println("  configDir=" + cli.configDir
                + " scale=" + cli.scale
                + " phase=" + cli.phase
                + " resultsDir=" + cli.resultsDir
                + " runId=" + cli.runId
                + " metricsJsonl=" + cli.metricsJsonl
                + " dryRun=" + cli.dryRun
                + " mockGenerate=" + cli.mockGenerate
                + " mockMaterialize=" + cli.mockMaterialize
                + " mockRun=" + cli.mockRun
                + " cleanupLayouts=" + cli.cleanupLayouts
                + " keepLayouts=" + cli.keepLayouts
                + " engines=" + cli.engines
                + " experiments=" + cli.experiments);

        LoadedConfig cfg = ConfigLoader.load(cli.configDir, cli.envOverride);
        System.out.println("  loaded typed: benchmark.id=" + cfg.benchmark.benchmark.id
                + " hdfsRoot=" + cfg.benchmark.benchmark.hdfsRoot
                + " yarn.queue=" + cfg.environment.environment.yarn.queue
                + " spark.deployMode=" + cfg.environment.environment.spark.deployMode
                + " bestOrc.objective=" + cfg.benchmark.bestOrcRule.objectiveQueries);

        List<String> phases = "all".equals(cli.phase)
                ? Arrays.asList("preflight", "generate", "materialize", "run", "final")
                : Arrays.asList(cli.phase);

        ExperimentPlan plan = ExperimentExpander.buildPlan(cfg, cli.scale, cli.engines, cli.experiments);
        QuerySampleManifest samples = null;
        CapabilityReport capability = null;
        List<RunResult> collectedResults = new ArrayList<RunResult>();
        String sharedRunId = cli.runId;

        if (phases.contains("preflight")) {
            capability = runPreflight(cli, cfg, plan);
        }

        boolean onlyPreflight = phases.size() == 1 && "preflight".equals(phases.get(0));
        if (onlyPreflight) {
            System.out.println("[orc-bench] OK — preflight complete.");
            return;
        }

        if (phases.contains("generate")) {
            samples = runGenerate(cli, cfg);
        }

        boolean onlyGenerate = phases.size() == 1 && "generate".equals(phases.get(0));
        if (onlyGenerate) {
            System.out.println("[orc-bench] OK — generate complete (Checkpoint 4).");
            return;
        }

        if (phases.contains("materialize")) {
            runMaterialize(cli, cfg, plan);
        }

        boolean onlyMaterialize = phases.size() == 1 && "materialize".equals(phases.get(0));
        if (onlyMaterialize) {
            System.out.println("[orc-bench] OK — materialize complete (Checkpoint 5).");
            return;
        }

        if (phases.contains("run")) {
            if (sharedRunId == null || sharedRunId.trim().isEmpty()) {
                sharedRunId = Instant.now().toString().replace(':', '-')
                        + "_" + UUID.randomUUID().toString().substring(0, 8);
            }
            List<RunResult> fromRun = runQueryPhase(cli, cfg, plan, samples, sharedRunId);
            collectedResults.addAll(fromRun);
        }

        boolean onlyRun = phases.size() == 1 && "run".equals(phases.get(0));
        if (onlyRun) {
            System.out.println("[orc-bench] OK — run complete (Checkpoint 7: Spark + Hive).");
            return;
        }

        if (phases.contains("report") || phases.contains("final")) {
            boolean includeFinal = phases.contains("final");
            if (sharedRunId == null || sharedRunId.trim().isEmpty()) {
                sharedRunId = cli.runId;
            }
            if ((sharedRunId == null || sharedRunId.trim().isEmpty())
                    && (cli.metricsJsonl == null || cli.metricsJsonl.trim().isEmpty())
                    && collectedResults.isEmpty()
                    && !cli.dryRun) {
                sharedRunId = Instant.now().toString().replace(':', '-')
                        + "_" + UUID.randomUUID().toString().substring(0, 8);
            }
            ReportPhaseResult report = ReportPhase.report(
                    cfg,
                    plan,
                    cli.scale,
                    cli.resultsDir,
                    sharedRunId,
                    cli.metricsJsonl,
                    collectedResults.isEmpty() ? null : collectedResults,
                    capability == null ? null : capability.skippedExperiments,
                    capability,
                    cli.dryRun,
                    includeFinal);
            printReportResult(report);
            System.out.println("[orc-bench] OK — " + report.phase
                    + " complete (Checkpoint 8). Next: Checkpoint 9 after user confirmation.");
            return;
        }

        if (cli.dryRun) {
            ExperimentExpander.printPlan(plan, System.out);
            System.out.println("[orc-bench] dry-run plan printed.");
        }

        System.out.println("[orc-bench] OK — stop here (Checkpoint 8).");
    }

    private static List<RunResult> runQueryPhase(
            CliOptions.CliArgs cli,
            LoadedConfig cfg,
            ExperimentPlan plan,
            QuerySampleManifest samples,
            String runId) throws Exception {
        boolean wantSpark = enginesContain(cli.engines, "spark");
        boolean wantHive = enginesContain(cli.engines, "hive_tez")
                || enginesContain(cli.engines, "hive_llap");

        if (!cli.dryRun && !cli.mockRun) {
            System.out.println("[orc-bench] NOTE: live run requires spark-submit / Hive JDBC on a "
                    + "Hadoop cluster node (YARN + HDFS layouts + HS2). "
                    + "Local hosts should use --dry-run or --mock-run.");
        }

        List<RunResult> all = new ArrayList<RunResult>();

        if (wantSpark) {
            Map<String, Boolean> coverage = SparkQueryRunner.section1Coverage(plan);
            System.out.println("[run] §1 Spark lever coverage in plan: " + coverage);
            RunPhaseResult result = SparkQueryRunner.run(
                    cfg,
                    plan,
                    samples,
                    cli.scale,
                    runId,
                    cli.resultsDir,
                    cli.dryRun,
                    cli.mockRun,
                    null);
            printRunResult("spark", result);
            if (result.results != null) {
                all.addAll(result.results);
            }
        } else {
            System.out.println("[run] spark engine not requested — skip SparkQueryRunner");
        }

        if (wantHive) {
            Map<String, Boolean> coverage = HiveJdbcRunner.section1Coverage(plan);
            System.out.println("[run] §1 Hive lever coverage in plan: " + coverage);
            RunPhaseResult result = HiveJdbcRunner.run(
                    cfg,
                    plan,
                    samples,
                    cli.scale,
                    runId,
                    cli.resultsDir,
                    cli.dryRun,
                    cli.mockRun,
                    null);
            printRunResult("hive", result);
            if (result.results != null) {
                all.addAll(result.results);
            }
        } else {
            System.out.println("[run] hive_tez/hive_llap not requested — skip HiveJdbcRunner");
        }

        if (!wantSpark && !wantHive) {
            System.out.println("[run] warning: no engines selected (expected spark and/or hive_tez/hive_llap)");
        }
        return all;
    }

    private static void printReportResult(ReportPhaseResult result) {
        System.out.println("[report] phase=" + result.phase
                + " runId=" + result.runId
                + " scale=" + result.scale
                + " results=" + result.resultRows
                + " aggregates=" + result.aggregateRows
                + " skipped=" + result.skippedRows
                + " finalCells=" + result.finalComparisonCells
                + " slaGateOverall=" + result.slaGateOverall
                + " dryRun=" + result.dryRun);
        if (result.bestOrc != null && result.bestOrc.experimentId != null) {
            System.out.println("[report] BEST_ORC=" + result.bestOrc.experimentId
                    + "/" + result.bestOrc.variantLabel
                    + " layout=" + result.bestOrc.layoutPath
                    + " p50=" + result.bestOrc.meanP50Ms
                    + " slaPassed=" + result.bestOrc.slaPassed
                    + " tie=" + result.bestOrc.tieBreakClass);
        }
        if (result.outputDir != null) {
            System.out.println("[report] artefacts: " + result.outputDir);
        }
        for (String note : result.notes) {
            System.out.println("[report] note: " + note);
        }
    }

    private static void printRunResult(String label, RunPhaseResult result) {
        System.out.println("[run] " + label + " result scale=" + result.scale
                + " planned=" + result.experimentsPlanned
                + " run=" + result.experimentsRun
                + " skipped=" + result.experimentsSkipped
                + " attempts=" + result.attempts
                + " dryRun=" + result.dryRun
                + " mocked=" + result.mocked);
        if (result.metricsJsonlPath != null) {
            System.out.println("[run] " + label + " metrics: " + result.metricsJsonlPath);
        }
        for (String note : result.notes) {
            System.out.println("[run] " + label + " note: " + note);
        }
    }

    private static boolean enginesContain(List<String> engines, String name) {
        if (engines == null || name == null) {
            return false;
        }
        for (String e : engines) {
            if (e != null && name.equalsIgnoreCase(e.trim())) {
                return true;
            }
        }
        return false;
    }

    private static void runMaterialize(
            CliOptions.CliArgs cli, LoadedConfig cfg, ExperimentPlan plan) throws Exception {
        if (!cli.dryRun && !cli.mockMaterialize) {
            System.out.println("[orc-bench] NOTE: live materialize rewrites ORC under "
                    + cfg.benchmark.benchmark.hdfsRoot
                    + " and requires spark-submit on a Hadoop cluster node. "
                    + "Local hosts should use --dry-run or --mock-materialize.");
        }
        MaterializeResult result = LayoutMaterializer.materialize(
                cfg,
                cli.scale,
                plan.physical,
                cli.dryRun,
                cli.mockMaterialize,
                cli.cleanupLayouts,
                cli.keepLayouts,
                null);
        System.out.println("[materialize] result scale=" + result.scale
                + " planned=" + result.plannedWrites
                + " written=" + result.written
                + " skippedMarkers=" + result.skippedExistingMarker
                + " cleaned=" + result.cleaned
                + " dryRun=" + result.dryRun
                + " mocked=" + result.mocked);
        for (String note : result.notes) {
            System.out.println("[materialize] note: " + note);
        }
    }

    private static QuerySampleManifest runGenerate(CliOptions.CliArgs cli, LoadedConfig cfg)
            throws Exception {
        if (!cli.dryRun && !cli.mockGenerate) {
            System.out.println("[orc-bench] NOTE: live generate writes ORC to HDFS under "
                    + cfg.benchmark.benchmark.hdfsRoot
                    + " and requires spark-submit on a Hadoop cluster node. "
                    + "Local hosts should use --dry-run or --mock-generate.");
        }
        GenerateResult result = DatasetGenerator.generate(
                cfg, cli.scale, cli.dryRun, cli.mockGenerate, null);
        System.out.println("[generate] result scale=" + result.scale
                + " rows≈" + result.estimatedRows
                + " basePath=" + result.basePath
                + " dryRun=" + result.dryRun
                + " mocked=" + result.mocked);
        for (String note : result.notes) {
            System.out.println("[generate] note: " + note);
        }
        if (result.samples != null && !result.samples.partitionDates.isEmpty()) {
            System.out.println("[generate] partitionDates=" + result.samples.partitionDates);
        }
        return result.samples;
    }

    private static CapabilityReport runPreflight(
            CliOptions.CliArgs cli, LoadedConfig cfg, ExperimentPlan plan) throws Exception {
        if (!cli.dryRun) {
            System.out.println("[orc-bench] NOTE: live preflight probes require a Hadoop cluster node "
                    + "(HDFS / Kerberos / Hive JDBC). Local machines should use --dry-run or unit tests.");
        }

        CapabilityReport report = PreflightProbe.run(cfg, cli.scale, cli.dryRun);
        PreflightProbe.applySkipToPlan(plan, report,
                cfg.environment.environment.preflight.markUnavailableExperimentsAsSkipped);
        PreflightProbe.enforceFailPolicy(report, cfg.environment.environment.preflight);
        PreflightProbe.printReport(report);

        String runId = Instant.now().toString().replace(':', '-')
                + "_" + UUID.randomUUID().toString().substring(0, 8);
        Path out = PreflightReportWriter.write(cli.resultsDir, runId, report, cli.scale);
        System.out.println("[orc-bench] preflight artefacts: " + out.toAbsolutePath());
        System.out.println("[orc-bench] skip-matrix cells=" + report.skippedExperiments.size());
        return report;
    }
}
