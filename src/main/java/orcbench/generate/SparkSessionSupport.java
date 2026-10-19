package orcbench.generate;

import orcbench.model.Models.LoadedConfig;
import org.apache.spark.sql.SparkSession;

/**
 * Minimal SparkSession bootstrap for Checkpoint 4 generate.
 * Intended for {@code spark-submit} on a cluster node (YARN provides master).
 */
public final class SparkSessionSupport {
    private SparkSessionSupport() {}

    public static SparkSession create(LoadedConfig cfg, String appName) {
        SparkSession.Builder b = SparkSession.builder().appName(appName == null ? "orc-bench" : appName);
        if (cfg != null
                && cfg.environment != null
                && cfg.environment.environment != null
                && cfg.environment.environment.spark != null
                && Boolean.TRUE.equals(cfg.environment.environment.spark.enableHiveSupport)) {
            b = b.enableHiveSupport();
        }
        // When launched via spark-submit, getOrCreate reuses the submit context.
        return b.getOrCreate();
    }

    /** True when Spark classes are present (cluster / spark-submit classpath). */
    public static boolean sparkAvailable() {
        try {
            Class.forName("org.apache.spark.sql.SparkSession");
            return true;
        } catch (ClassNotFoundException ex) {
            return false;
        }
    }
}
