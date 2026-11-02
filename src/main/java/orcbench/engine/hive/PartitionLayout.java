package orcbench.engine.hive;

import java.util.Collections;
import java.util.List;

/**
 * Hive 3.1 {@code MSCK REPAIR} with the default
 * {@code hive.msck.path.validation=throw} fails the DDL task when ORC files
 * sit in the table root of a {@code PARTITIONED BY} table. Generate writes
 * {@code RAW_ARRAY_BASE} that way: files, not {@code key=} directories.
 * The metastore error stays on HiveServer2; JDBC only reports return code 1.
 */
public final class PartitionLayout {
    private PartitionLayout() {}

    /**
     * Partition columns to put in {@code CREATE EXTERNAL TABLE}.
     *
     * @param declared columns from the experiment contract; may be empty
     * @param childNames immediate children of the layout directory, or null
     *                    when the listing failed
     */
    public static List<String> effectivePartitions(List<String> declared, List<String> childNames) {
        if (declared == null || declared.isEmpty()) {
            return Collections.emptyList();
        }
        if (childNames == null || hasHivePartitionDirs(childNames)) {
            return declared;
        }
        return Collections.emptyList();
    }

    /** True when a visible child is a Hive partition directory ({@code col=value}). */
    public static boolean hasHivePartitionDirs(List<String> childNames) {
        if (childNames == null) {
            return false;
        }
        for (String name : childNames) {
            if (name == null || name.isEmpty()) {
                continue;
            }
            char first = name.charAt(0);
            if (first == '_' || first == '.') {
                continue;
            }
            int eq = name.indexOf('=');
            if (eq > 0 && eq < name.length() - 1) {
                return true;
            }
        }
        return false;
    }
}
