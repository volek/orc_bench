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

    /**
     * YARN cluster mode fails the application if the driver returns without a SparkContext
     * ({@code User did not initialize spark context}). Phases that only read YAML (preflight,
     * report, dry-run) still have to open a session while the process is the cluster driver.
     * Spark 3.2 ApplicationMaster sets {@code spark.yarn.app.id} before calling user main;
     * it does not set {@code SPARK_YARN_MODE}.
     */
    public static void openYarnDriver(LoadedConfig cfg) {
        System.out.println("[orc-bench] driver spark.master=" + System.getProperty("spark.master")
                + " deployMode=" + System.getProperty("spark.submit.deployMode")
                + " yarnAppId=" + System.getProperty("spark.yarn.app.id")
                + " SPARK_YARN_MODE=" + System.getenv("SPARK_YARN_MODE"));
        if (!runningInsideYarnCluster() || held != null) {
            return;
        }
        held = create(cfg, "orc-bench");
        System.out.println("[orc-bench] SparkSession opened for YARN cluster driver");
    }

    public static void closeYarnDriver() {
        SparkSession session = held;
        held = null;
        if (session == null) {
            return;
        }
        try {
            session.stop();
        } catch (Exception ignored) {
            // The query backend may already have stopped the shared session.
        }
    }

    static boolean runningInsideYarnCluster() {
        if ("true".equalsIgnoreCase(System.getenv("SPARK_YARN_MODE"))) {
            return true;
        }
        // Set by ApplicationMaster.run() only in cluster mode, before the user thread starts.
        String appId = System.getProperty("spark.yarn.app.id");
        return appId != null && !appId.trim().isEmpty();
    }

    private static SparkSession held;
}
