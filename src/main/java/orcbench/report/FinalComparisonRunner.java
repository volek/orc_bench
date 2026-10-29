package orcbench.report;

import orcbench.engine.spark.SparkQueryRunner;
import orcbench.io.ResultStore;
import orcbench.layout.ExperimentExpander;
import orcbench.metrics.MetricsStore;
import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.BestOrcChoice;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.RunPhaseResult;
import orcbench.model.Models.RunResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Executes {@code final_comparison} on the selected BEST_ORC layout.
 * {@code --phase final} used to write the path and stop. The measurements
 * are ordinary {@code metrics.jsonl} rows with {@code experimentId=SPARK_COMMON}.
 */
public final class FinalComparisonRunner {
    private FinalComparisonRunner() {}

    /**
     * @return prior rows plus the rows this call measured. Prior rows are loaded
     *         from {@code metrics.jsonl} when {@code prior} is empty.
     */
    public static List<RunResult> execute(
            LoadedConfig cfg,
            ExperimentPlan plan,
            QuerySampleManifest samples,
            String scale,
            String runId,
            String resultsDir,
            String metricsJsonlPath,
            List<RunResult> prior,
            boolean mock) throws Exception {
        List<RunResult> base = new ArrayList<RunResult>();
        if (prior != null) {
            base.addAll(prior);
        }
        String metricsPath = ReportPhase.resolveMetricsPath(resultsDir, runId, metricsJsonlPath);
        if (base.isEmpty() && metricsPath != null && ResultStore.isFile(metricsPath)) {
            base.addAll(MetricsStore.readJsonl(metricsPath));
            System.out.println("[final] loaded " + base.size() + " prior rows from " + metricsPath);
        }

        long slaMs = cfg.benchmark.metrics.slaSuccessThresholdMs;
        if (slaMs <= 0) {
            slaMs = cfg.benchmark.benchmark.slaMeanLatencyMs;
        }
        List<AggregateStats> agg = MetricsStore.aggregate(base, slaMs);
        List<ExperimentInstance> physical = plan == null ? null : plan.physical;
        BestOrcChoice best = BestOrcSelector.select(agg, cfg.benchmark.bestOrcRule, physical);
        String bestPath = best.layoutPath;
        if (bestPath == null || bestPath.trim().isEmpty()) {
            String root = cfg.benchmark.benchmark.hdfsRoot;
            String sc = scale == null ? "S" : scale;
            bestPath = root + "/" + sc + "/" + cfg.benchmark.physicalBaseline.id;
            System.out.println("[final] BEST_ORC layout unknown, using baseline " + bestPath);
        } else {
            System.out.println("[final] BEST_ORC=" + best.experimentId
                    + "/" + best.variantLabel + " layout=" + bestPath);
        }

        List<ExperimentInstance> cells = ExperimentExpander.finalProfiles(cfg.benchmark, bestPath);
        ExperimentPlan only = new ExperimentPlan();
        only.scale = scale;
        only.concurrencySelected = Collections.singletonList(Integer.valueOf(1));
        for (ExperimentInstance cell : cells) {
            if (cell == null || cell.skipReason != null) {
                continue;
            }
            if (SparkQueryRunner.isSparkEngine(cell)) {
                only.engine.add(cell);
            }
        }
        if (only.engine.isEmpty()) {
            System.out.println("[final] no Spark profile to execute");
            return base;
        }
        System.out.println("[final] executing " + only.engine.size()
                + " profile(s) on " + bestPath);
        RunPhaseResult ran = SparkQueryRunner.run(
                cfg, only, samples, scale, runId, resultsDir, false, mock, null);
        if (ran.results != null) {
            base.addAll(ran.results);
        }
        System.out.println("[final] measured attempts=" + (ran.results == null ? 0 : ran.results.size()));
        return base;
    }
}
