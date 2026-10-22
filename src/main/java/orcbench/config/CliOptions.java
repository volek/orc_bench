package orcbench.config;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "orc-bench",
        mixinStandardHelpOptions = true,
        version = "0.1.0-SNAPSHOT",
        description = "ORC/HDFS/Spark/Hive benchmark orchestrator "
                + "(Checkpoint 8: report / BEST_ORC / final_comparison)")
public class CliOptions implements Callable<CliOptions.CliArgs> {

    @Option(names = "--config", description = "Directory with YAML contract (default: conf)")
    String configDir = "conf";

    @Option(names = "--env-override", description = "YAML overlay for null environment fields")
    String envOverride;

    @Option(names = "--scale", description = "Dataset scale: S|M|L (100/500/1024 GB)")
    String scale = "S";

    @Option(names = "--phase",
            description = "Phase: preflight|generate|materialize|run|report|final|all "
                    + "(live generate/materialize/run/preflight/final require a Hadoop cluster node; "
                    + "report works locally from metrics.jsonl / fixtures)")
    String phase = "all";

    @Option(names = "--experiments", description = "Comma-separated experiment IDs filter")
    String experiments;

    @Option(names = "--engines", description = "Comma-separated: spark,hive_tez,hive_llap")
    String engines = "spark,hive_tez,hive_llap";

    @Option(names = "--results-dir",
            description = "Results directory. On a YARN cluster driver the default "
                    + "results is rewritten to hdfs://<namenode>/orc_test/results/<namenode-host>. "
                    + "Pass an explicit hdfs:// path to override.")
    String resultsDir = "results";

    @Option(names = "--run-id",
            description = "Existing results/<runId>/ for --phase report|final "
                    + "(reads metrics.jsonl when present)")
    String runId;

    @Option(names = "--metrics-jsonl",
            description = "Explicit metrics.jsonl path for --phase report|final "
                    + "(local fixtures / cluster dump)")
    String metricsJsonl;

    @Option(names = "--dry-run", description = "Plan without executing Spark/Hive/HDFS writes")
    boolean dryRun;

    @Option(names = "--mock-generate",
            description = "Generate phase uses in-memory FakeGenerateBackend (no HDFS; local/CI)")
    boolean mockGenerate;

    @Option(names = "--mock-materialize",
            description = "Materialize phase uses in-memory FakeMaterializeBackend (no HDFS; local/CI)")
    boolean mockMaterialize;

    @Option(names = "--mock-run",
            description = "Run phase uses FakeSparkQueryBackend / FakeHiveJdbcBackend "
                    + "(no Spark/YARN/HDFS/HS2; local/CI)")
    boolean mockRun;

    @Option(names = "--cleanup-layouts",
            description = "After materialize, delete non-retained rewrite layouts under {hdfs_root}/{scale}")
    boolean cleanupLayouts;

    @Option(names = "--keep-layouts",
            description = "Comma-separated experiment ids / layout dir names retained when cleaning")
    String keepLayouts;

    @Override
    public CliArgs call() {
        CliArgs a = new CliArgs();
        a.configDir = configDir;
        a.envOverride = envOverride;
        a.scale = scale == null ? "S" : scale.toUpperCase();
        a.phase = phase == null ? "all" : phase.toLowerCase();
        a.experiments = split(experiments);
        a.engines = split(engines);
        if (a.engines.isEmpty()) {
            a.engines = new ArrayList<>(Arrays.asList("spark", "hive_tez", "hive_llap"));
        }
        a.resultsDir = resultsDir;
        a.runId = runId;
        a.metricsJsonl = metricsJsonl;
        a.dryRun = dryRun;
        a.mockGenerate = mockGenerate;
        a.mockMaterialize = mockMaterialize;
        a.mockRun = mockRun;
        a.cleanupLayouts = cleanupLayouts;
        a.keepLayouts = split(keepLayouts);
        return a;
    }

    private static List<String> split(String s) {
        List<String> out = new ArrayList<>();
        if (s == null || s.trim().isEmpty()) {
            return out;
        }
        for (String p : s.split(",")) {
            String t = p.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    /**
     * Parse CLI args. Returns null on usage/parse error (caller should exit 2).
     * Exits the JVM when {@code --help} / {@code --version} was requested.
     */
    public static CliArgs parse(String[] args) {
        CliOptions opts = new CliOptions();
        CommandLine cmd = new CommandLine(opts);
        int code = cmd.execute(args);
        if (cmd.isUsageHelpRequested() || cmd.isVersionHelpRequested()) {
            System.exit(code);
        }
        if (code != 0) {
            return null;
        }
        return cmd.getExecutionResult();
    }

    /** Parsed CLI values for Checkpoint 1+. */
    public static final class CliArgs {
        public String configDir;
        public String envOverride;
        public String scale;
        public String phase;
        public List<String> experiments;
        public List<String> engines;
        public String resultsDir;
        /** Existing results run id for report/final phases. */
        public String runId;
        /** Explicit metrics.jsonl for local fixture / cluster dump reporting. */
        public String metricsJsonl;
        public boolean dryRun;
        /** Local/CI: in-memory generate without HDFS. */
        public boolean mockGenerate;
        /** Local/CI: in-memory materialize without HDFS. */
        public boolean mockMaterialize;
        /** Local/CI: FakeSparkQueryBackend / FakeHiveJdbcBackend without Spark/YARN/HDFS/HS2. */
        public boolean mockRun;
        /** Delete non-retained rewrite layouts after materialize. */
        public boolean cleanupLayouts;
        /** Experiment ids / dir names kept when {@link #cleanupLayouts} is set. */
        public List<String> keepLayouts;
    }
}
