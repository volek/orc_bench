package orcbench.engine.hive;

import java.util.ArrayList;
import java.util.List;

public final class SchemaDdl {
    private SchemaDdl() {}

    public static String hiveColumnsFromOrcSchema(String schemaText) {
        String body = schemaText.replace("struct<", "").replaceAll(">\\s*$", "");
        String[] fields = body.split(",");
        List<String> cols = new ArrayList<>();
        for (String field : fields) {
            String trimmed = field.trim();
            if (trimmed.isEmpty()) continue;
            String[] parts = trimmed.split(":", 2);
            if (parts.length != 2) cols.add(trimmed);
            else cols.add(parts[0].trim() + " " + toHiveType(parts[1].trim()));
        }
        return String.join(",\n  ", cols);
    }

    private static String toHiveType(String t) {
        switch (t) {
            case "string": return "STRING";
            case "timestamp": return "TIMESTAMP";
            case "bigint": return "BIGINT";
            case "int": return "INT";
            case "double": return "DOUBLE";
            case "boolean": return "BOOLEAN";
            case "date": return "DATE";
            default:
                if (t.startsWith("array<")) {
                    String inner = t.substring("array<".length(), t.length() - 1);
                    return "ARRAY<" + toHiveType(inner) + ">";
                }
                if (t.startsWith("map<")) {
                    String inner = t.substring("map<".length(), t.length() - 1);
                    String[] kv = inner.split(",", 2);
                    if (kv.length == 2) return "MAP<" + toHiveType(kv[0].trim()) + "," + toHiveType(kv[1].trim()) + ">";
                    return "MAP<STRING,STRING>";
                }
                return t.toUpperCase();
        }
    }

    public static String createExternalTableSql(
            String table, String schemaText, String location,
            List<String> partitions, List<String[]> extraColumns) {
        StringBuilder cols = new StringBuilder(hiveColumnsFromOrcSchema(schemaText));
        if (extraColumns != null) {
            for (String[] c : extraColumns) {
                cols.append(",\n  ").append(c[0]).append(' ').append(c[1]);
            }
        }
        String partClause = "";
        if (partitions != null && !partitions.isEmpty()) {
            List<String> parts = new ArrayList<>();
            for (String p : partitions) parts.add(p + " STRING");
            partClause = "\nPARTITIONED BY (" + String.join(", ", parts) + ")";
        }
        return "CREATE EXTERNAL TABLE " + table + " (\n  " + cols + "\n)"
                + partClause + "\nSTORED AS ORC\nLOCATION '" + location + "'\n";
    }
}
