package orcbench.preflight;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * Live HDFS / Hive JDBC / Kerberos probes. Intended for execution on a Hadoop
 * cluster node with SDP classpath (spark-submit). Not for local developer machines.
 */
public final class LiveClusterBackend implements PreflightBackend {

    @Override
    public String javaRuntimeVersion() {
        return System.getProperty("java.version", "unknown");
    }

    @Override
    public HdfsProbeResult probeHdfs(String namenodeUri, String workDir) {
        try {
            Configuration conf = new Configuration();
            String dir = workDir == null || workDir.trim().isEmpty()
                    ? "hdfs://orc_test" : workDir.trim();
            Path work = new Path(dir);
            FileSystem fs;
            if (work.toUri().getScheme() != null) {
                fs = work.getFileSystem(conf);
            } else {
                if (namenodeUri != null && !namenodeUri.trim().isEmpty()) {
                    conf.set("fs.defaultFS", namenodeUri);
                }
                fs = FileSystem.get(conf);
            }
            long freeGib = fs.getStatus().getRemaining() / (1024L * 1024L * 1024L);
            if (!fs.exists(work) && !fs.mkdirs(work)) {
                return new HdfsProbeResult(false, freeGib, "cannot create workDir " + work);
            }
            Path probe = new Path(work, ".orc_bench_preflight_" + System.currentTimeMillis());
            FSDataOutputStream out = fs.create(probe, true);
            try {
                out.writeUTF("orc-bench-preflight");
            } finally {
                out.close();
            }
            boolean deleted = fs.delete(probe, false);
            return new HdfsProbeResult(deleted, freeGib,
                    "writable=" + deleted + " freeGib=" + freeGib + " workDir=" + work);
        } catch (Throwable ex) {
            return new HdfsProbeResult(false, -1L, "HDFS probe failed: " + ex.getMessage());
        }
    }

    @Override
    public JdbcProbeResult probeHiveJdbc(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            return new JdbcProbeResult(false, "Hive JDBC URL is null");
        }
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            try (Connection conn = DriverManager.getConnection(jdbcUrl);
                 ResultSet rs = conn.createStatement().executeQuery("SELECT 1")) {
                boolean ok = rs.next();
                return new JdbcProbeResult(ok, ok ? "SELECT 1 ok" : "empty result");
            }
        } catch (Throwable ex) {
            return new JdbcProbeResult(false, "Hive JDBC probe failed: " + ex.getMessage());
        }
    }

    @Override
    public KerberosProbeResult probeKerberos(String principal, String keytabPath) {
        try {
            ProcessBuilder pb = new ProcessBuilder("klist", "-s");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            boolean finished = p.waitFor(15, TimeUnit.SECONDS);
            if (!finished) {
                p.destroyForcibly();
                return new KerberosProbeResult(false, "klist -s timed out");
            }
            int code = p.exitValue();
            if (code == 0) {
                return new KerberosProbeResult(true,
                        "ticket present (principal=" + principal + ", keytab=" + keytabPath + ")");
            }
            return new KerberosProbeResult(false,
                    "klist -s exit=" + code + " (run kinit -kt " + keytabPath + " " + principal + ")");
        } catch (Throwable ex) {
            return new KerberosProbeResult(false, "Kerberos probe failed: " + ex.getMessage());
        }
    }
}
