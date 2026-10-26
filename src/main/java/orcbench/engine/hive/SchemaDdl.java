package orcbench.engine.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Hive external-table DDL helpers for Checkpoint 7.
 * Converts {@code schema.orc.txt} field types to Hive types and builds
 * {@code CREATE EXTERNAL TABLE … STORED AS ORC} statements.
 */
public final class SchemaDdl {
    /**
     * ORC field names Hive would fold to lowercase if left unquoted.
     * The fold breaks reads: the file column is {@code userLogin}, the
     * unquoted Hive name becomes {@code userlogin}.
     */
    private static final Set<String> CASE_SENSITIVE_COLUMNS = Collections.unmodifiableSet(
            new HashSet<String>(Arrays.asList(
                    "userLogin", "createdAt", "userNode", "userName", "metamodelVersion",
                    "paramOldValue", "paramName", "paramValue", "archiveInternalId",
                    "archiveInsertTime", "__time", "sourceSystem", "metamodelHash",
                    "nodeId", "requestId")));

    private SchemaDdl() {}

    /**
     * Parse {@code struct<field:type,…>} into Hive column DDL lines
     * ({@code name TYPE} joined by comma+newline). Nested angle-bracket types
     * with commas are preserved via depth-aware splitting.
     */
    public static String hiveColumnsFromOrcSchema(String schemaText) {
        if (schemaText == null) {
            return "";
        }
        String body = schemaText.trim();
        if (body.toLowerCase(Locale.ROOT).startsWith("struct<")) {
            body = body.substring("struct<".length());
            if (body.endsWith(">")) {
                body = body.substring(0, body.length() - 1);
            }
        }
        List<String> cols = new ArrayList<String>();
        for (String field : splitTopLevel(body)) {
            String trimmed = field.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int colon = indexOfTopLevel(trimmed, ':');
            if (colon < 0) {
                cols.add(quoteHiveIdent(trimmed));
            } else {
                String name = trimmed.substring(0, colon).trim();
                String type = trimmed.substring(colon + 1).trim();
                cols.add(quoteHiveIdent(name) + " " + toHiveType(type));
            }
        }
        return joinColumns(cols);
    }

    public static String createExternalTableSql(
            String table,
            String schemaText,
            String location,
            List<String> partitions,
            List<String[]> extraColumns) {
        StringBuilder cols = new StringBuilder(hiveColumnsFromOrcSchema(schemaText));
        if (extraColumns != null) {
            for (String[] c : extraColumns) {
                if (c == null || c.length < 2) {
                    continue;
                }
                if (cols.length() > 0) {
                    cols.append(",\n  ");
                }
                cols.append(quoteHiveIdent(c[0])).append(' ').append(c[1]);
            }
        }
        String partClause = "";
        if (partitions != null && !partitions.isEmpty()) {
            List<String> parts = new ArrayList<String>();
            for (String p : partitions) {
                parts.add(quoteHiveIdent(p) + " STRING");
            }
            partClause = "\nPARTITIONED BY (" + joinPlain(parts) + ")";
        }
        return "CREATE EXTERNAL TABLE " + table + " (\n  " + cols + "\n)"
                + partClause + "\nSTORED AS ORC\nLOCATION '" + location + "'\n";
    }

    /**
     * Dictionary ORC written by generate ({@code id}, {@code type}).
     * An explicit schema is required: {@code CREATE TABLE … STORED AS ORC LOCATION}
     * without columns fails when the path cannot be inferred, and the caller
     * used to swallow that failure, so JOIN queries then reported
     * {@code Table not found}.
     */
    public static String createReferenceTableSql(String table, String location) {
        String safeLocation = location == null ? "" : location.replace("'", "");
        return "CREATE EXTERNAL TABLE " + quoteHiveIdent(table)
                + " (id STRING, type STRING)\nSTORED AS ORC\nLOCATION '"
                + safeLocation + "'\n";
    }

    /**
     * Quote mixed-case ORC columns in a Hive statement. Keywords stay bare:
     * only names in {@link #CASE_SENSITIVE_COLUMNS} are wrapped. Regions
     * already inside quotes or backticks are left unchanged, so a template
     * that already quotes {@code `__time`} is not quoted twice.
     */
    public static String quoteHiveSqlIdentifiers(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        StringBuilder out = new StringBuilder(sql.length() + 16);
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'') {
                int j = scanSingleQuoted(sql, i);
                out.append(sql, i, j);
                i = j;
                continue;
            }
            if (c == '`') {
                int j = scanBacktick(sql, i);
                out.append(sql, i, j);
                i = j;
                continue;
            }
            if (isIdentStart(c)) {
                int j = i + 1;
                while (j < sql.length() && isIdentPart(sql.charAt(j))) {
                    j++;
                }
                String ident = sql.substring(i, j);
                if (CASE_SENSITIVE_COLUMNS.contains(ident)) {
                    out.append(quoteHiveIdent(ident));
                } else {
                    out.append(ident);
                }
                i = j;
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    /**
     * Extra columns for representation rewrites (FLAT / MAP / PAIRS / HYBRID).
     * RAW_ARRAY uses the canonical ORC schema only.
     */
    public static List<String[]> representationExtras(String representation) {
        List<String[]> extras = new ArrayList<String[]>();
        if (representation == null) {
            return extras;
        }
        if ("FLAT".equals(representation) || "HYBRID".equals(representation)) {
            String[] names = {
                    "epk_id", "operation_uid", "card_number", "ip_address", "device_id",
                    "tb_code", "osb_code", "vsp_code"
            };
            for (String n : names) {
                extras.add(new String[]{n, "STRING"});
            }
        } else if ("MAP".equals(representation)) {
            extras.add(new String[]{"params", "MAP<STRING,STRING>"});
        } else if ("PAIRS".equals(representation)) {
            extras.add(new String[]{"params", "ARRAY<STRUCT<name:STRING,value:STRING>>"});
        }
        return extras;
    }

    /**
     * Hive folds unquoted identifiers to lowercase and rejects a leading
     * underscore ({@code __time} is lexed as a charset introducer). Quote
     * both cases so the metastore name matches the ORC field.
     */
    static String quoteHiveIdent(String name) {
        if (name == null || name.isEmpty() || isLowercaseHiveIdent(name)) {
            return name;
        }
        return "`" + name.replace("`", "``") + "`";
    }

    private static boolean isLowercaseHiveIdent(String name) {
        char first = name.charAt(0);
        if (first < 'a' || first > 'z') {
            return false;
        }
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
                return false;
            }
        }
        return true;
    }

    private static int scanSingleQuoted(String sql, int start) {
        int j = start + 1;
        while (j < sql.length()) {
            if (sql.charAt(j) == '\'') {
                if (j + 1 < sql.length() && sql.charAt(j + 1) == '\'') {
                    j += 2;
                    continue;
                }
                return j + 1;
            }
            j++;
        }
        return sql.length();
    }

    private static int scanBacktick(String sql, int start) {
        int j = start + 1;
        while (j < sql.length()) {
            if (sql.charAt(j) == '`') {
                if (j + 1 < sql.length() && sql.charAt(j + 1) == '`') {
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

    static String toHiveType(String t) {
        if (t == null) {
            return "STRING";
        }
        String lower = t.toLowerCase(Locale.ROOT);
        if ("string".equals(lower)) {
            return "STRING";
        }
        if ("timestamp".equals(lower)) {
            return "TIMESTAMP";
        }
        if ("bigint".equals(lower)) {
            return "BIGINT";
        }
        if ("int".equals(lower)) {
            return "INT";
        }
        if ("double".equals(lower)) {
            return "DOUBLE";
        }
        if ("boolean".equals(lower)) {
            return "BOOLEAN";
        }
        if ("date".equals(lower)) {
            return "DATE";
        }
        if (lower.startsWith("array<") && lower.endsWith(">")) {
            String inner = t.substring("array<".length(), t.length() - 1);
            return "ARRAY<" + toHiveType(inner) + ">";
        }
        if (lower.startsWith("map<") && lower.endsWith(">")) {
            String inner = t.substring("map<".length(), t.length() - 1);
            List<String> kv = splitTopLevel(inner);
            if (kv.size() == 2) {
                return "MAP<" + toHiveType(kv.get(0).trim()) + "," + toHiveType(kv.get(1).trim()) + ">";
            }
            return "MAP<STRING,STRING>";
        }
        if (lower.startsWith("struct<") && lower.endsWith(">")) {
            // Keep nested struct field names; upper-case primitive tokens via recursive parse.
            String inner = t.substring("struct<".length(), t.length() - 1);
            List<String> fields = splitTopLevel(inner);
            List<String> out = new ArrayList<String>();
            for (String f : fields) {
                int colon = indexOfTopLevel(f, ':');
                if (colon < 0) {
                    out.add(f.trim());
                } else {
                    out.add(quoteHiveIdent(f.substring(0, colon).trim())
                            + ":" + toHiveType(f.substring(colon + 1).trim()));
                }
            }
            return "STRUCT<" + joinPlain(out) + ">";
        }
        return t.toUpperCase(Locale.ROOT);
    }

    private static List<String> splitTopLevel(String body) {
        List<String> out = new ArrayList<String>();
        if (body == null || body.isEmpty()) {
            return out;
        }
        StringBuilder cur = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '<') {
                depth++;
                cur.append(c);
            } else if (c == '>') {
                depth--;
                cur.append(c);
            } else if (c == ',' && depth == 0) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (cur.length() > 0) {
            out.add(cur.toString());
        }
        return out;
    }

    private static int indexOfTopLevel(String s, char needle) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '<') {
                depth++;
            } else if (c == '>') {
                depth--;
            } else if (c == needle && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private static String joinColumns(List<String> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) {
                sb.append(",\n  ");
            }
            sb.append(cols.get(i));
        }
        return sb.toString();
    }

    private static String joinPlain(List<String> parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(parts.get(i));
        }
        return sb.toString();
    }
}
