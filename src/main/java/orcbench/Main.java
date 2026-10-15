package orcbench;

import orcbench.config.CliOptions;
import orcbench.config.ConfigLoader;
import orcbench.engine.hive.HiveJdbcRunner;
import orcbench.engine.spark.SparkQueryRunner;
import orcbench.generate.DatasetGenerator;
import orcbench.layout.ExperimentExpander;
import orcbench.layout.LayoutMaterializer;
import orcbench.metrics.MetricsStore;
import orcbench.model.Models.*;
import orcbench.preflight.PreflightProbe;
import orcbench.report.ReportWriter;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class Main {
    private Main() {}

    public static void main(String[] args) {
        CliArgs cli = CliOptions.parse(args);
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

    public static void run(CliArgs cli) throws Exception {
        LoadedConfig cfg = ConfigLoader.load(cli.configDir, cli.envOverride);
        String runId = Instant.now().toString().replace(':', '-') + "_" + UUID.randomUUID().toString().substring(0, 8);
        System.out.println("[orc-bench] runId=" + runId + " scale=" + cli.scale + " phase=" + cli.phase);

        CapabilityReport capability = PreflightProbe.run(cfg, cli.scale, cli.dryRun);
        PreflightProbe.printReport(capability);

        List<String> phases = "all".equals(cli.phase)
                ? Arrays.asList("preflight", "generate", "materialize", "run", "final")
                : Arrays.asList(cli.phase);

        QuerySampleManifest samples = new QuerySampleManifest();
        samples.partitionDates = Arrays.asList(
                cfg.queries.querySuite.defaults.getOrDefault("date_value", "2026-01-15"));

        List<RunResult> allResults = new ArrayList<>();
        String bestOrcPath = null;
        SparkSession spark = null;

        if (phases.contains("preflight") && phases.size() == 1) {
            ReportWriter.writeRun(cli.resultsDir, runId, allResults, new ArrayList<>(),
                    capability.skippedExperiments, capability, null);
            return;
        }

        if (needsSpark(phases)) {
            spark = SparkQueryRunner.createSession(cfg, "orc-bench");
        }

        if (phases.contains("generate")) {
            samples = DatasetGenerator.generate(spark, cfg, cli.scale, cli.dryRun);
        }

        List<ExperimentInstance> physical = ExperimentExpander.expandPhysical(cfg.benchmark, cli.scale, cli.experiments);
        physical = PreflightProbe.applySkip(physical, capability,
                cfg.environment.environment.preflight.markUnavailableExperimentsAsSkipped);

        if (phases.contains("materialize")) {
            LayoutMaterializer.materialize(spark, cfg, cli.scale, physical, cli.dryRun, cli.cleanup, cli.retain);
        }

        if (phases.contains("run")) {
            String baselinePath = cfg.benchmark.benchmark.hdfsRoot + "/" + cli.scale + "/"
                    + cfg.benchmark.physicalBaseline.id;

            for (ExperimentInstance inst : physical) {
                if (!"engine".equals(inst.kind)) {
                    System.out.println("[run] physical " + inst.experimentId + "/" + inst.variantLabel);
                    allResults.addAll(SparkQueryRunner.runExperiment(spark, cfg, inst, samples, runId, null));
                }
            }

            String layoutForEngine = bestOrcPath != null ? bestOrcPath : baselinePath;
            List<ExperimentInstance> engineInst = ExperimentExpander.expandEngine(
                    cfg.benchmark, cli.scale, layoutForEngine, cli.engines, cli.experiments);
            engineInst = PreflightProbe.applySkip(engineInst, capability,
                    cfg.environment.environment.preflight.markUnavailableExperimentsAsSkipped);

            for (ExperimentInstance inst : engineInst) {
                System.out.println("[run] engine " + inst.experimentId + "/" + inst.variantLabel + " (" + inst.engine + ")");
                if ("spark".equals(inst.engine)) {
                    allResults.addAll(SparkQueryRunner.runExperiment(spark, cfg, inst, samples, runId, null));
                } else if ("hive_tez".equals(inst.engine) || "hive_llap".equals(inst.engine)) {
                    allResults.addAll(HiveJdbcRunner.runExperiment(cfg, inst, samples, runId, null, cli.dryRun));
                }
            }

            if (!cli.dryRun) {
                ExperimentInstance sparkBase = null;
                ExperimentInstance llapWarm = null;
                for (ExperimentInstance i : engineInst) {
                    if ("SPARK_BASE".equals(i.experimentId)) sparkBase = i;
                    if ("HIVE_LLAP".equals(i.experimentId) && "warm".equals(i.cacheState)) llapWarm = i;
                }
                for (int c : cfg.benchmark.runs.concurrencyLevels) {
                    if (c <= 1) continue;
                    if (sparkBase != null) {
                        System.out.println("[run] concurrency spark=" + c);
                        allResults.addAll(SparkQueryRunner.runExperiment(spark, cfg, sparkBase, samples, runId, c));
                    }
                    if (llapWarm != null) {
                        System.out.println("[run] concurrency hive_llap=" + c);
                        allResults.addAll(HiveJdbcRunner.runExperiment(cfg, llapWarm, samples, runId, c, cli.dryRun));
                    }
                }
            }
        }

        Set<String> physicalIds = new HashSet<>();
        physicalIds.add(cfg.benchmark.physicalBaseline.id);
        for (PhysicalExperiment pe : cfg.benchmark.physicalExperiments) physicalIds.add(pe.id);

        List<AggregateStats> aggregates = MetricsStore.aggregate(allResults, cfg.benchmark.metrics.slaSuccessThresholdMs);
        String bestId = MetricsStore.rankBestOrc(aggregates, physicalIds);
        if (bestId != null) {
            for (ExperimentInstance i : physical) {
                if (bestId.equals(i.experimentId) && i.layoutPath != null) {
                    bestOrcPath = i.layoutPath;
                    break;
                }
            }
            if (bestOrcPath == null) {
                bestOrcPath = cfg.benchmark.benchmark.hdfsRoot + "/" + cli.scale + "/" + bestId;
            }
            System.out.println("[rank] BEST_ORC candidate experiment=" + bestId + " path=" + bestOrcPath);
        }

        if (phases.contains("final")) {
            String path = bestOrcPath != null ? bestOrcPath
                    : cfg.benchmark.benchmark.hdfsRoot + "/" + cli.scale + "/" + cfg.benchmark.physicalBaseline.id;
            List<ExperimentInstance> finals = ExperimentExpander.finalProfiles(cfg.benchmark, path);
            finals = PreflightProbe.applySkip(finals, capability,
                    cfg.environment.environment.preflight.markUnavailableExperimentsAsSkipped);
            for (ExperimentInstance inst : finals) {
                System.out.println("[final] " + inst.experimentId);
                if ("spark".equals(inst.engine)) {
                    allResults.addAll(SparkQueryRunner.runExperiment(spark, cfg, inst, samples, runId, null));
                } else if ("hive_tez".equals(inst.engine) || "hive_llap".equals(inst.engine)) {
                    allResults.addAll(HiveJdbcRunner.runExperiment(cfg, inst, samples, runId, null, cli.dryRun));
                }
            }
        }

        List<AggregateStats> finalAggregates = MetricsStore.aggregate(allResults, cfg.benchmark.metrics.slaSuccessThresholdMs);
        ReportWriter.writeRun(cli.resultsDir, runId, allResults, finalAggregates,
                capability.skippedExperiments, capability, bestOrcPath);

        if (spark != null) spark.stop();
        System.out.println("[orc-bench] done runId=" + runId + " results=" + allResults.size());
    }

    private static boolean needsSpark(List<String> phases) {
        for (String p : phases) {
            if ("generate".equals(p) || "materialize".equals(p) || "run".equals(p) || "final".equals(p) || "all".equals(p)) {
                return true;
            }
        }
        return false;
    }
}
