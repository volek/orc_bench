package orcbench.engine.hive;

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

    public LiveHiveJdbcBackend(Connection connection, boolean ownsConnection) {
        if (connection == null) {
            throw new IllegalArgumentException("connection is required");
        }
        this.connection = connection;
        this.ownsConnection = ownsConnection;
    }

    /**
     * Open a JDBC connection from {@code environment.hive} JDBC URL.
     * Intended for use on a Hadoop cluster node after {@code kinit}.
     */
    public static LiveHiveJdbcBackend connect(LoadedConfig cfg) throws Exception {
        return new LiveHiveJdbcBackend(openConnection(cfg), true);
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
            for (Map.Entry<String, String> e : nameToLocation.entrySet()) {
                try {
                    st.execute("DROP TABLE IF EXISTS " + e.getKey());
                    st.execute("CREATE EXTERNAL TABLE " + e.getKey()
                            + " STORED AS ORC LOCATION '" + e.getValue() + "'");
                } catch (Exception ignored) {
                    // refs are best-effort for JOIN queries
                }
            }
        } finally {
            st.close();
        }
    }

    @Override
    public Long estimateOrcBytes(String layoutPath) {
        // HDFS byte accounting is observational for Hive JDBC path in CP7;
        // full FS stats belong with cluster metrics / CP8 reporting.
        return null;
    }

    @Override
    public QueryExecResult execute(String sql) throws Exception {
        Statement st = connection.createStatement();
        try {
            ResultSet rs = st.executeQuery(sql);
            try {
                List<String> rows = readAll(rs);
                return new QueryExecResult(rows.toArray(new String[0]), null);
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
            out.add(sb.toString());
        }
        return out;
    }
}
