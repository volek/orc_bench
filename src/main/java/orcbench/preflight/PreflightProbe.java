package orcbench.preflight;

import orcbench.model.Models.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public final class PreflightProbe {
    private PreflightProbe() {}

    public static CapabilityReport run(LoadedConfig cfg, String scale, boolean dryRun) {
        EnvironmentSection env = cfg.environment.environment;
        CapabilityReport report = new CapabilityReport();
        List<String> notes = report.notes;
        Map<String, String> versions = report.versions;
        Map<String, Boolean> caps = report.capabilities;

        String javaVer = System.getProperty("java.version", "unknown");
        versions.put("java.runtime", javaVer);
        if (env.preflight.requireJava8RuntimeCheck) {
            boolean ok = javaVer.startsWith("1.8") || javaVer.startsWith("8");
            if (!ok) notes.add("Java runtime is " + javaVer + "; cluster expects 1.8 (continuing for local build)");
            caps.put("java8", ok || dryRun);
        }

        for (Map.Entry<String, Optional<String>> e : env.versions.entrySet()) {
            if (e.getValue().isPresent()) versions.put(e.getKey(), e.getValue().get());
            else notes.add("version." + e.getKey() + " is null (fill via --env-override)");
        }

        boolean hdfsOk;
        try {
            Configuration conf = new Configuration();
            if (env.hdfs.namenodeUri != null) conf.set("fs.defaultFS", env.hdfs.namenodeUri);
            FileSystem fs = FileSystem.get(conf);
            long freeGib = fs.getStatus().getRemaining() / (1024L * 1024L * 1024L);
            versions.put("hdfs.free_gib_observed", Long.toString(freeGib));
            long target = cfg.dataset.scales.containsKey(scale)
                    ? cfg.dataset.scales.get(scale).targetLogicalGib : 0L;
            if (env.preflight.requireHdfsCapacityCheck && freeGib < target) {
                notes.add("HDFS free " + freeGib + " GiB < scale " + scale + " target " + target + " GiB");
                hdfsOk = false;
            } else {
                hdfsOk = true;
            }
        } catch (Throwable ex) {
            notes.add("HDFS probe failed: " + ex.getMessage());
            hdfsOk = dryRun;
        }
        caps.put("hdfs_writable", hdfsOk);

        caps.put("hdfs_short_circuit_supported", Boolean.TRUE.equals(env.hdfs.shortCircuitSupported));
        caps.put("client_datanode_colocation", Boolean.TRUE.equals(env.hdfs.dataLocalityVerifiable));
        caps.put("centralized_cache_supported", Boolean.TRUE.equals(env.hdfs.centralizedCacheSupported));
        caps.put("derived_event_date_partition", true);
        caps.put("partition_column_event_date", true);
        caps.put("date_partitioned_table", true);
        caps.put("partition_dimension_join", true);
        caps.put("native_orc_reader", true);
        caps.put("nested_columns", true);
        caps.put("nested_sql_dialect_variant", true);
        caps.put("hive_session_allows_mode_override", true);

        Optional<String> tez = env.versions.get("tez");
        caps.put("tez_version_verified", (tez != null && tez.isPresent()) || env.hive.tezAvailable);
        caps.put("llap_daemons_available",
                Boolean.TRUE.equals(env.hive.llapAvailable) || env.hive.llapConfigured);
        boolean llapRunning = Boolean.TRUE.equals(env.hive.llapDaemonsRunningVerified)
                || Boolean.TRUE.equals(cfg.benchmark.benchmark.llapRunningVerified);
        caps.put("llap_daemons_running", llapRunning);
        caps.put("llap_task_execution_verified", llapRunning);
        caps.put("materialized_views_supported", Boolean.TRUE.equals(env.hive.materializedViewsSupported));

        String jdbcUrl = env.hive.connectionJdbcUrl != null ? env.hive.connectionJdbcUrl : env.hive.jdbcUrl;
        boolean jdbcOk;
        if (jdbcUrl == null) {
            notes.add("Hive JDBC URL is null");
            jdbcOk = env.hive.jdbcConnectionVerified;
        } else if (dryRun) {
            notes.add("dry-run: skip JDBC connect to " + jdbcUrl);
            jdbcOk = true;
        } else {
            try {
                Class.forName("org.apache.hive.jdbc.HiveDriver");
                try (Connection conn = DriverManager.getConnection(jdbcUrl);
                     ResultSet rs = conn.createStatement().executeQuery("SELECT 1")) {
                    rs.next();
                    jdbcOk = true;
                }
            } catch (Exception ex) {
                notes.add("Hive JDBC probe failed: " + ex.getMessage());
                jdbcOk = env.hive.jdbcConnectionVerified;
            }
        }
        caps.put("hive_jdbc", jdbcOk);

        if (!env.operations.permissionToChangeClusterConfig) {
            notes.add("cluster config changes forbidden; SHORT_CIRCUIT/HDFS_CACHE centralized may skip");
        }
        if (!env.operations.permissionToResetCaches) {
            notes.add("cache reset forbidden; warm/cold relies on natural state");
        }
        return report;
    }

    public static List<ExperimentInstance> applySkip(
            List<ExperimentInstance> instances, CapabilityReport report, boolean markSkipped) {
        if (!markSkipped) return instances;
        List<ExperimentInstance> updated = new ArrayList<>();
        for (ExperimentInstance inst : instances) {
            if (inst.skipReason != null) {
                report.skippedExperiments.add(new String[]{inst.experimentId, inst.skipReason});
                updated.add(inst);
                continue;
            }
            List<String> missing = new ArrayList<>();
            for (String r : inst.requires) {
                if (!Boolean.TRUE.equals(report.capabilities.get(r))) missing.add(r);
            }
            if (!missing.isEmpty()) {
                String reason = "missing capabilities: " + String.join(",", missing);
                inst.skipReason = reason;
                report.skippedExperiments.add(new String[]{inst.experimentId + "/" + inst.variantLabel, reason});
            }
            updated.add(inst);
        }
        return updated;
    }

    public static void printReport(CapabilityReport report) {
        System.out.println("=== Preflight ===");
        for (Map.Entry<String, String> e : new TreeMap<>(report.versions).entrySet()) {
            System.out.println("  version." + e.getKey() + " = " + e.getValue());
        }
        for (Map.Entry<String, Boolean> e : new TreeMap<>(report.capabilities).entrySet()) {
            System.out.println("  capability." + e.getKey() + " = " + e.getValue());
        }
        for (String n : report.notes) System.out.println("  note: " + n);
        if (!report.skippedExperiments.isEmpty()) {
            System.out.println("  skipped:");
            for (String[] s : report.skippedExperiments) {
                System.out.println("    - " + s[0] + ": " + s[1]);
            }
        }
    }
}
