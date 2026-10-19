package orcbench.preflight;

/**
 * Cluster I/O for preflight probes. Unit tests inject fakes; production uses
 * {@link LiveClusterBackend}. Must only be exercised on a Hadoop cluster node.
 */
public interface PreflightBackend {

    String javaRuntimeVersion();

    HdfsProbeResult probeHdfs(String namenodeUri, String workDir);

    JdbcProbeResult probeHiveJdbc(String jdbcUrl);

    KerberosProbeResult probeKerberos(String principal, String keytabPath);

    final class HdfsProbeResult {
        public final boolean writable;
        public final long freeSpaceGib;
        public final String detail;

        public HdfsProbeResult(boolean writable, long freeSpaceGib, String detail) {
            this.writable = writable;
            this.freeSpaceGib = freeSpaceGib;
            this.detail = detail;
        }
    }

    final class JdbcProbeResult {
        public final boolean ok;
        public final String detail;

        public JdbcProbeResult(boolean ok, String detail) {
            this.ok = ok;
            this.detail = detail;
        }
    }

    final class KerberosProbeResult {
        public final boolean ok;
        public final String detail;

        public KerberosProbeResult(boolean ok, String detail) {
            this.ok = ok;
            this.detail = detail;
        }
    }
}
