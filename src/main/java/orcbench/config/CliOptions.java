package orcbench.config;

import orcbench.model.Models.CliArgs;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "orc-bench", mixinStandardHelpOptions = true, version = "0.1.0",
        description = "ORC/HDFS/Spark/Hive benchmark fat-jar orchestrator")
public class CliOptions implements Callable<CliArgs> {

    @Option(names = "--config", description = "directory with YAML contract (default: conf)")
    String configDir = "conf";

    @Option(names = "--scale", description = "dataset scale S|M|L")
    String scale = "S";

    @Option(names = "--phase", description = "preflight|generate|materialize|run|final|all")
    String phase = "all";

    @Option(names = "--experiments", description = "comma-separated experiment IDs filter")
    String experiments;

    @Option(names = "--engines", description = "spark,hive_tez,hive_llap")
    String engines = "spark,hive_tez,hive_llap";

    @Option(names = "--env-override", description = "YAML overlay for null environment fields")
    String envOverride;

    @Option(names = "--retain", description = "layout IDs to retain during cleanup")
    String retain;

    @Option(names = "--cleanup", description = "remove non-retained layout copies after materialize")
    boolean cleanup;

    @Option(names = "--results-dir", description = "results output directory")
    String resultsDir = "results";

    @Option(names = "--dry-run", description = "plan experiments without executing Spark/Hive jobs")
    boolean dryRun;

    @Override
    public CliArgs call() {
        CliArgs a = new CliArgs();
        a.configDir = configDir;
        a.scale = scale == null ? "S" : scale.toUpperCase();
        a.phase = phase == null ? "all" : phase.toLowerCase();
        a.experiments = split(experiments);
        a.engines = split(engines);
        if (a.engines.isEmpty()) {
            a.engines = new ArrayList<>(Arrays.asList("spark", "hive_tez", "hive_llap"));
        }
        a.envOverride = envOverride;
        a.retain = split(retain);
        a.cleanup = cleanup;
        a.resultsDir = resultsDir;
        a.dryRun = dryRun;
        return a;
    }

    private static List<String> split(String s) {
        List<String> out = new ArrayList<>();
        if (s == null || s.trim().isEmpty()) return out;
        for (String p : s.split(",")) {
            String t = p.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

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
}
