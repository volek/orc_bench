package orcbench.layout;

import orcbench.model.Models.LayoutWriteRequest;

import java.util.List;

/**
 * Filesystem / writer sink for Checkpoint 5 materialize.
 * Unit tests and local {@code --mock-materialize} use {@link FakeMaterializeBackend};
 * live HDFS/Spark writes go through {@link SparkMaterializeBackend} on a cluster node only.
 */
public interface MaterializeBackend {

    /** True when {@code path} exists (file or directory). */
    boolean pathExists(String path) throws Exception;

    /** True when an idempotency {@code *.done} marker already exists. */
    boolean markerExists(String markerPath) throws Exception;

    /** Write idempotency marker content (UTF-8). Never overwrite if already present. */
    void writeMarker(String markerPath, String content) throws Exception;

    /** Approximate byte size of source layout (ORC under {@code basePath}). */
    long estimateBytes(String basePath) throws Exception;

    /**
     * Materialize one OVAT layout from {@link LayoutWriteRequest#sourceBasePath}
     * into {@link LayoutWriteRequest#layoutPath}.
     */
    void writeLayout(LayoutWriteRequest request) throws Exception;

    /** List immediate child names under {@code directory} (empty if missing). */
    List<String> listChildren(String directory) throws Exception;

    /** Recursive delete; no-op if path missing. */
    void deleteRecursive(String path) throws Exception;

    /** Optional local DDL stub write (cluster may also regenerate later). */
    void writeLocalText(String relativePath, String content) throws Exception;
}
