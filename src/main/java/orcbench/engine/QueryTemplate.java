package orcbench.engine;

import orcbench.model.Models.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static BoundQuery resolveSql(QuerySpec query, QuerySuiteContract suite, String dialect,
                                        String table, QuerySampleManifest samples, String representation) {
        if (!query.representations.isEmpty() && !query.representations.contains(representation)) {
            return null;
        }
        if (!query.dialects.isEmpty() && !query.dialects.contains(dialect)) {
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
        if (sql == null) return null;

        Map<String, String> params = new HashMap<>(suite.querySuite.defaults);
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

        return new BoundQuery(query.id, substitute(sql, params));
    }

    public static List<BoundQuery> queriesForExperiment(
            QuerySuiteContract suite, List<String> experimentQueryIds, String dialect,
            String table, QuerySampleManifest samples, String representation) {
        List<QuerySpec> selected = new ArrayList<>();
        if (experimentQueryIds == null || experimentQueryIds.isEmpty()) {
            selected.addAll(suite.queries);
        } else {
            for (QuerySpec q : suite.queries) {
                if (experimentQueryIds.contains(q.id)) selected.add(q);
            }
        }
        List<BoundQuery> out = new ArrayList<>();
        for (QuerySpec q : selected) {
            BoundQuery b = resolveSql(q, suite, dialect, table, samples, representation);
            if (b != null) out.add(b);
        }
        return out;
    }

    private static List<String> pick(QuerySampleManifest samples, String key, String caseName) {
        if (samples.samples.containsKey(key) && samples.samples.get(key).containsKey(caseName)) {
            return samples.samples.get(key).get(caseName);
        }
        return new ArrayList<>();
    }

    private static String first(List<String> list, String def) {
        return list.isEmpty() ? def : list.get(0);
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
            m.appendReplacement(sb, Matcher.quoteReplacement(escape(raw)));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String escape(String value) {
        return value.replace("'", "''");
    }
}
