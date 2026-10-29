package orcbench.engine.hive;

import orcbench.engine.ResultDigest;
import orcbench.io.HdfsPaths;
import orcbench.model.Models.HiveEnv;
import orcbench.model.Models.LoadedConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HiveServer2 JDBC backend for cluster-node runs.
 * Do not use from the development host against a live HS2 / Kerberos cluster.
 */
public final class LiveHiveJdbcBackend implements HiveJdbcBackend {

    private final Connection connection;
    private final boolean ownsConnection;
    private final LoadedConfig reconnectConfig;

    public LiveHiveJdbcBackend(Connection connection, boolean ownsConnection) {
        this(connection, ownsConnection, null);
    }

    private LiveHiveJdbcBackend(Connection connection, boolean ownsConnection, LoadedConfig reconnectConfig) {
        if (connection == null) {
            throw new IllegalArgumentException("connection is required");
        }
        this.connection = connection;
        this.ownsConnection = ownsConnection;
        this.reconnectConfig = reconnectConfig;
    }

    /**
     * Open a JDBC connection from {@code environment.hive} JDBC URL.
     * Intended for use on a Hadoop cluster node after {@code kinit}.
     */
    public static LiveHiveJdbcBackend connect(LoadedConfig cfg) throws Exception {
        return new LiveHiveJdbcBackend(openConnection(cfg), true, cfg);
    }

    public static Connection openConnection(LoadedConfig cfg) throws Exception {
        String url = resolveJdbcUrl(cfg);
        if (url == null || url.isEmpty()) {
            throw new IllegalStateException(
                    "Hive JDBC URL is null; set environment.hive.jdbc_url "
                            + "(or connection_jdbc_url) via --env-override on the cluster node");
        }
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        return DriverManager.getConnection(url);
    }

    public static String resolveJdbcUrl(LoadedConfig cfg) {
        if (cfg == null || cfg.environment == null || cfg.environment.environment == null) {
            return null;
        }
        HiveEnv env = cfg.environment.environment.hive;
        if (env == null) {
            return null;
        }
        if (env.connectionJdbcUrl != null && !env.connectionJdbcUrl.isEmpty()) {
            return env.connectionJdbcUrl;
        }
        return env.jdbcUrl;
    }

    @Override
    public void applySessionOverrides(Map<String, String> overrides) throws Exception {
        if (overrides == null || overrides.isEmpty()) {
            return;
        }
        Statement st = connection.createStatement();
        try {
            for (Map.Entry<String, String> e : overrides.entrySet()) {
                st.execute("SET " + e.getKey() + "=" + e.getValue());
            }
        } finally {
            st.close();
        }
    }

    @Override
    public void ensureExternalTable(String tableName, String ddl, boolean repairPartitions)
            throws Exception {
        Statement st = connection.createStatement();
        try {
            enableQuotedIdentifiers(st);
            st.execute("DROP TABLE IF EXISTS " + tableName);
            st.execute(ddl);
            if (repairPartitions) {
                st.execute("MSCK REPAIR TABLE " + tableName);
            }
        } finally {
            st.close();
        }
    }

    @Override
    public void ensureRefTables(Map<String, String> nameToLocation) throws Exception {
        if (nameToLocation == null || nameToLocation.isEmpty()) {
            return;
        }
        Statement st = connection.createStatement();
        try {
            enableQuotedIdentifiers(st);
            st.execute("CREATE DATABASE IF NOT EXISTS " + SchemaDdl.HIVE_DATABASE);
            for (Map.Entry<String, String> e : nameToLocation.entrySet()) {
                String qualified = SchemaDdl.qualify(SchemaDdl.HIVE_DATABASE, e.getKey());
                st.execute("DROP TABLE IF EXISTS " + qualified);
                st.execute(SchemaDdl.createReferenceTableSql(qualified, e.getValue()));
                // Fail here, once, if the session cannot see the table.
                st.execute("DESCRIBE " + qualified);
            }
        } finally {
            st.close();
        }
    }

    @Override
    public HiveJdbcBackend openSession() throws Exception {
        if (reconnectConfig == null) {
            throw new IllegalStateException(
                    "parallel Hive session needs environment.hive jdbc url");
        }
        return connect(reconnectConfig);
    }

    private static void enableQuotedIdentifiers(Statement st) throws Exception {
        // Default is "column", but a hive-site override of "none" leaves
        // backticks in the identifier text. Required for `__time` and mixed-case ORC names.
        st.execute("SET hive.support.quoted.identifiers=column");
        // Tez prints HDFS_BYTES_READ into the statement log. A rejected SET must
        // not fail the query: the byte field then stays null.
        trySet(st, "hive.tez.exec.print.summary=true");
        // Container reuse is not a private AM. It stops one slot from handing
        // a dead container to the next statement on the same connection.
        trySet(st, "tez.am.container.reuse.enabled=false");
        trySet(st, "hive.prewarm.enabled=false");
    }

    private static void trySet(Statement st, String assignment) {
        try {
            st.execute("SET " + assignment);
        } catch (Exception ignored) {
            // Server may reject a non-runtime Tez key. Bytes and isolation then
            // fall back to whatever the session already has.
        }
    }

    /**
     * HiveStatement.getQueryLog() is not on the compile classpath of every
     * driver build. Reflect so a missing Hive JDBC class does not break Spark-only runs.
     */
    @SuppressWarnings("unchecked")
    private static List<String> readQueryLog(Statement st) {
        List<String> lines = new ArrayList<String>();
        if (st == null) {
            return lines;
        }
        try {
            java.lang.reflect.Method hasMore = st.getClass().getMethod("hasMoreLogs");
            java.lang.reflect.Method getLog = st.getClass().getMethod("getQueryLog");
            int guard = 0;
            while (Boolean.TRUE.equals(hasMore.invoke(st)) && guard < 20) {
                Object chunk = getLog.invoke(st);
                if (chunk instanceof List) {
                    for (Object line : (List<Object>) chunk) {
                        if (line != null) {
                            lines.add(line.toString());
                        }
                    }
                }
                guard++;
            }
        } catch (Exception ignored) {
            // No operation log: hdfsBytesRead stays null.
        }
        return lines;
    }

    @Override
    public Long estimateOrcBytes(String layoutPath) {
        return HdfsPaths.sumFileBytes(layoutPath);
    }

    /**
     * Stream the result. Buffering every row (Q07 on scale S) exhausts the
     * driver the same way Spark {@code collect()} does. The fingerprint matches
     * {@link orcbench.engine.spark.SparkQueryRunner#hashRows}.
     */
    @Override
    public QueryExecResult execute(String sql) throws Exception {
        Statement st = connection.createStatement();
        try {
            enableQuotedIdentifiers(st);
            ResultSet rs = st.executeQuery(sql);
            try {
                ResultDigest digest = new ResultDigest();
                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                while (rs.next()) {
                    digest.add(canonicalRow(rs, cols));
                }
                Long bytes = TezCounterLog.sumHdfsBytesRead(readQueryLog(st));
                return new QueryExecResult(new String[0], bytes, digest.count(), digest.hashHex());
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }
    }

    @Override
    public String explain(String sql) {
        try {
            Statement st = connection.createStatement();
            try {
                enableQuotedIdentifiers(st);
                ResultSet rs = st.executeQuery("EXPLAIN " + sql);
                try {
                    List<String> lines = readAll(rs);
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < lines.size(); i++) {
                        if (i > 0) {
                            sb.append('\n');
                        }
                        sb.append(lines.get(i));
                    }
                    return sb.toString();
                } finally {
                    rs.close();
                }
            } finally {
                st.close();
            }
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() {
        if (ownsConnection && connection != null) {
            try {
                connection.close();
            } catch (Exception ignored) {
                // ignore
            }
        }
    }

    /**
     * Canonical row encoding for Spark/Hive result equivalence
     * ({@code null} → {@code \N} per {@code result_equivalence.null_encoding}).
     */
    static List<String> readAll(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        List<String> out = new ArrayList<String>();
        while (rs.next()) {
            out.add(canonicalRow(rs, cols));
        }
        return out;
    }

    static String canonicalRow(ResultSet rs, int cols) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= cols; i++) {
            if (i > 1) {
                sb.append('|');
            }
            Object v = rs.getObject(i);
            if (v == null || rs.wasNull()) {
                sb.append("\\N");
            } else {
                sb.append(v.toString());
            }
        }
        return sb.toString();
    }
}
