package orcbench.engine;

import orcbench.engine.hive.SchemaDdl;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.QuerySpec;
import orcbench.model.Models.QuerySuiteContract;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Binds query-suite SQL templates to table names and sample parameters
 * (Checkpoint 6). Dialect / representation filters match {@code query-suite.yaml}.
 */
public final class QueryTemplate {
    private static final Pattern PARAM = Pattern.compile("\\$\\{([a-zA-Z0-9_]+)\\}");

    private QueryTemplate() {}

    public static final class BoundQuery {
        public final String queryId;
        public final String sql;

        public BoundQuery(String queryId, String sql) {
            this.queryId = queryId;
            this.sql = sql;
        }
    }

    /** A suite query that cannot be run on this dialect, representation, or layout. */
    public static final class RejectedQuery {
        public final String queryId;
        public final String reason;

        public RejectedQuery(String queryId, String reason) {
            this.queryId = queryId;
            this.reason = reason;
        }
    }

    public static BoundQuery resolveSql(
            QuerySpec query,
            QuerySuiteContract suite,
            String dialect,
            String table,
            QuerySampleManifest samples,
            String representation) {
        if (query == null) {
            return null;
        }
        if (!query.representations.isEmpty() && representation != null
                && !query.representations.contains(representation)) {
            return null;
        }
        if (!query.dialects.isEmpty() && dialect != null && !query.dialects.contains(dialect)) {
            return null;
        }

        String sql = query.sql;
        if (sql == null && "NP_RAW_ARRAY".equals(query.id)) {
            for (QuerySpec q : suite.queries) {
                if (("spark_3_2".equals(dialect) && "NP_ARRAY_EPK_SPARK".equals(q.id))
                        || ("hive_tez".equals(dialect) && "NP_ARRAY_EPK_HIVE".equals(q.id))) {
                    sql = q.sql;
                    break;
                }
            }
        }
        if (sql == null) {
            return null;
        }

        Map<String, String> params = new HashMap<String, String>();
        if (suite.querySuite != null && suite.querySuite.defaults != null) {
            params.putAll(suite.querySuite.defaults);
        }
        params.put("table", table);
        params.put("module_dictionary_table", "module_dictionary");
        params.put("epk_dictionary_table", "epk_dictionary");
        params.put("date_dimension_table", "partition_dates");
        params.put("type_value", "A");
        params.put("date_category", "peak");
        params.put("device_system_name", first(pick(samples, "DeviceSystemName", "present"), "iOS"));
        params.put("user_login", first(pick(samples, "userLogin", "present"), "userLogin0"));
        params.put("user_login_a", at(pick(samples, "userLogin", "present"), 0, "userLogin0"));
        params.put("user_login_b", at(pick(samples, "userLogin", "present"), 1, "userLogin1"));
        params.put("user_login_c", at(pick(samples, "userLogin", "present"), 2, "userLogin2"));
        params.put("session_id_outer", first(pick(samples, "session", "present"), "copy2_0"));
        params.put("nested_session_id", first(pick(samples, "SESSION_ID", "present"), "copy2_0"));
        params.put("epk_id", first(pick(samples, "EPK_ID", "present"), "epk_0"));
        params.put("ip_address", first(pick(samples, "IP_ADDRESS", "present"), "203.0.113.1"));
        params.put("user_node", first(pick(samples, "userNode", "present"), "203.0.113.10"));
        params.put("operation_uid", first(pick(samples, "OPERATION_UID", "present"), "op_0"));
        params.put("fulltext_pattern", "SYSTEM_EVENTS");
        params.put("absent_param_value", "ABSENT_STAFF");
        params.put("epk_id_list", quoteList(pick(samples, "epk_id_list", "present"),
                first(pick(samples, "EPK_ID", "present"), "epk_0")));

        String bound = substitute(sql, params);
        if ("hive_tez".equals(dialect)) {
            bound = SchemaDdl.quoteHiveSqlIdentifiers(bound);
        }
        return new BoundQuery(query.id, bound);
    }

    /**
     * Queries listed by the experiment that {@link #resolveSql} will not bind.
     * Callers record these as {@code skipped} instead of dropping them.
     */
    public static List<RejectedQuery> unboundQueries(
            QuerySuiteContract suite,
            List<String> experimentQueryIds,
            String dialect,
            String representation) {
        List<RejectedQuery> out = new ArrayList<RejectedQuery>();
        if (experimentQueryIds != null && suite != null && suite.queries != null) {
            java.util.Set<String> known = new java.util.HashSet<String>();
            for (QuerySpec q : suite.queries) {
                known.add(q.id);
            }
            for (String id : experimentQueryIds) {
                if (!known.contains(id)) {
                    out.add(new RejectedQuery(id, "query is not in the suite"));
                }
            }
        }
        for (QuerySpec q : selectQueries(suite, experimentQueryIds)) {
            if (resolveSql(q, suite, dialect, "t", new QuerySampleManifest(), representation) != null) {
                continue;
            }
            out.add(new RejectedQuery(q.id, unboundReason(q, dialect, representation)));
        }
        return out;
    }

    /** Drop statements the layout cannot answer, recording why. */
    public static List<BoundQuery> runnableQueries(
            List<BoundQuery> bound,
            List<String> partitionColumns,
            List<RejectedQuery> rejected) {
        List<BoundQuery> out = new ArrayList<BoundQuery>();
        if (bound == null) {
            return out;
        }
        for (BoundQuery b : bound) {
            String reason = layoutRejectReason(b.sql, partitionColumns);
            if (reason != null) {
                if (rejected != null) {
                    rejected.add(new RejectedQuery(b.queryId, reason));
                }
            } else {
                out.add(b);
            }
        }
        return out;
    }

    /**
     * Skip a bound statement that names {@code event_date} on a layout which
     * does not have that partition column. The flat control used to fail every
     * repetition with {@code cannot resolve 'event_date'}.
     */
    public static String layoutRejectReason(String sql, List<String> partitionColumns) {
        if (sql == null || !containsBareIdent(sql, "event_date")) {
            return null;
        }
        if (partitionColumns != null && partitionColumns.contains("event_date")) {
            return null;
        }
        return "query references event_date but this layout has no event_date column";
    }

    public static List<BoundQuery> queriesForExperiment(
            QuerySuiteContract suite,
            List<String> experimentQueryIds,
            String dialect,
            String table,
            QuerySampleManifest samples,
            String representation) {
        List<BoundQuery> out = new ArrayList<BoundQuery>();
        for (QuerySpec q : selectQueries(suite, experimentQueryIds)) {
            BoundQuery b = resolveSql(q, suite, dialect, table, samples, representation);
            if (b != null) {
                out.add(b);
            }
        }
        return out;
    }

    private static List<QuerySpec> selectQueries(
            QuerySuiteContract suite, List<String> experimentQueryIds) {
        List<QuerySpec> selected = new ArrayList<QuerySpec>();
        if (suite == null || suite.queries == null) {
            return selected;
        }
        if (experimentQueryIds == null || experimentQueryIds.isEmpty()) {
            selected.addAll(suite.queries);
        } else {
            for (QuerySpec q : suite.queries) {
                if (experimentQueryIds.contains(q.id)) {
                    selected.add(q);
                }
            }
        }
        return selected;
    }

    private static String unboundReason(QuerySpec query, String dialect, String representation) {
        if (query == null) {
            return "missing query";
        }
        if (!query.representations.isEmpty() && representation != null
                && !query.representations.contains(representation)) {
            return "representation " + representation + " is not in " + query.representations;
        }
        if (!query.dialects.isEmpty() && dialect != null && !query.dialects.contains(dialect)) {
            return "dialect " + dialect + " is not in " + query.dialects;
        }
        if (query.sql == null) {
            return "no sql";
        }
        return "query was not bound";
    }

    static boolean containsBareIdent(String sql, String ident) {
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i = scanQuote(sql, i, '\'');
                continue;
            }
            if (c == '`') {
                i = scanQuote(sql, i, '`');
                continue;
            }
            if (isIdentStart(c)) {
                int j = i + 1;
                while (j < sql.length() && isIdentPart(sql.charAt(j))) {
                    j++;
                }
                if (sql.substring(i, j).equals(ident)) {
                    return true;
                }
                i = j;
                continue;
            }
            i++;
        }
        return false;
    }

    private static int scanQuote(String sql, int start, char quote) {
        int j = start + 1;
        while (j < sql.length()) {
            if (sql.charAt(j) == quote) {
                if (j + 1 < sql.length() && sql.charAt(j + 1) == quote) {
                    j += 2;
                    continue;
                }
                return j + 1;
            }
            j++;
        }
        return sql.length();
    }

    private static boolean isIdentStart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    private static boolean isIdentPart(char c) {
        return isIdentStart(c) || (c >= '0' && c <= '9');
    }

    private static List<String> pick(QuerySampleManifest samples, String key, String caseName) {
        if (samples != null
                && samples.samples != null
                && samples.samples.containsKey(key)
                && samples.samples.get(key).containsKey(caseName)) {
            return samples.samples.get(key).get(caseName);
        }
        return new ArrayList<String>();
    }

    private static String first(List<String> list, String def) {
        return list.isEmpty() ? def : list.get(0);
    }

    private static String quoteList(List<String> values, String fallback) {
        List<String> src = values == null || values.isEmpty()
                ? java.util.Collections.singletonList(fallback) : values;
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (String v : src) {
            if (v == null || v.isEmpty()) {
                continue;
            }
            if (n > 0) {
                sb.append(',');
            }
            sb.append('\'').append(v.replace("'", "''")).append('\'');
            n++;
            if (n >= 10800) {
                break;
            }
        }
        if (n == 0) {
            return "'" + fallback.replace("'", "''") + "'";
        }
        return sb.toString();
    }

    private static String at(List<String> list, int idx, String def) {
        return idx < list.size() ? list.get(idx) : def;
    }

    private static String substitute(String template, Map<String, String> params) {
        Matcher m = PARAM.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            String raw = params.containsKey(key) ? params.get(key) : m.group(0);
            String bound = "epk_id_list".equals(key) ? raw : escape(raw);
            m.appendReplacement(sb, Matcher.quoteReplacement(bound));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String escape(String value) {
        return value.replace("'", "''");
    }
}
