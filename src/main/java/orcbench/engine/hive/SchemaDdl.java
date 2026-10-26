package orcbench.engine.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Hive external-table DDL helpers for Checkpoint 7.
 * Converts {@code schema.orc.txt} field types to Hive types and builds
 * {@code CREATE EXTERNAL TABLE … STORED AS ORC} statements.
 */
public final class SchemaDdl {
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
                    "epk_id", "session_id", "operation_uid", "channel_type", "vsp_code",
                    "osb_code", "tb_code", "ip_address", "device_system_name"
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
     * HiveLexer accepts an unquoted identifier only as
     * {@code (Letter|Digit)(Letter|Digit|'_')*}. A leading underscore is
     * {@code CharSetName} ({@code _UTF8'...'}), so {@code __time TIMESTAMP}
     * fails with {@code cannot recognize input near '__time'}.
     */
    static String quoteHiveIdent(String name) {
        if (name == null || name.isEmpty() || isUnquotedHiveIdent(name)) {
            return name;
        }
        return "`" + name.replace("`", "``") + "`";
    }

    private static boolean isUnquotedHiveIdent(String name) {
        char first = name.charAt(0);
        if (!isAsciiLetterOrDigit(first)) {
            return false;
        }
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!isAsciiLetterOrDigit(c) && c != '_') {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiLetterOrDigit(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
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
