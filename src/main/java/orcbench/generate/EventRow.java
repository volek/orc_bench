package orcbench.generate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One synthetic RAW_ARRAY event matching {@code schema.orc.txt}.
 * Pure Java — no Spark dependency (unit-testable on the developer host).
 */
public final class EventRow {
    public final String userLogin;
    public final long createdAtMs;
    public final String session;
    public final String userNode;
    public final String module;
    public final String name;
    public final String userName;
    public final String metamodelVersion;
    /** Always empty list (never null) — {@code dataset.null_semantics.tags}. */
    public final List<String> tags;
    /**
     * Parallel to {@code paramName}: elements are null (not empty string) —
     * {@code dataset.null_semantics.paramOldValue}.
     */
    public final List<String> paramOldValue;
    public final List<String> paramName;
    public final List<String> paramValue;
    public final String archiveInternalId;
    public final long archiveInsertTimeMs;
    public final long timeMs;

    public EventRow(
            String userLogin,
            long createdAtMs,
            String session,
            String userNode,
            String module,
            String name,
            String userName,
            String metamodelVersion,
            List<String> tags,
            List<String> paramOldValue,
            List<String> paramName,
            List<String> paramValue,
            String archiveInternalId,
            long archiveInsertTimeMs,
            long timeMs) {
        this.userLogin = userLogin;
        this.createdAtMs = createdAtMs;
        this.session = session;
        this.userNode = userNode;
        this.module = module;
        this.name = name;
        this.userName = userName;
        this.metamodelVersion = metamodelVersion;
        this.tags = immutableCopy(tags);
        this.paramOldValue = immutableCopyNullable(paramOldValue);
        this.paramName = immutableCopy(paramName);
        this.paramValue = immutableCopy(paramValue);
        this.archiveInternalId = archiveInternalId;
        this.archiveInsertTimeMs = archiveInsertTimeMs;
        this.timeMs = timeMs;
    }

    private static List<String> immutableCopy(List<String> in) {
        if (in == null || in.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<String>(in));
    }

    private static List<String> immutableCopyNullable(List<String> in) {
        if (in == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<String>(in));
    }
}
