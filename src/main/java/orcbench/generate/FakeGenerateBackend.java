package orcbench.generate;

import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.ReferenceDataset;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory generate sink for unit tests and local mock paths.
 * Caps materialised rows so developer hosts never attempt full scale-S HDFS writes.
 */
public final class FakeGenerateBackend implements GenerateBackend {

    public static final int DEFAULT_MAX_ROWS = 2048;

    private final int maxRows;
    public final List<EventRow> events = new ArrayList<>();
    public final Map<String, Integer> refCounts = new LinkedHashMap<>();
    public QuerySampleManifest lastManifest;
    public String lastBasePath;
    public String lastRefsPath;
    public String lastManifestPath;
    public OrcWriteSettings lastOrc;
    public long requestedRowCount;
    public int writeRawCalls;
    public int writeRefsCalls;
    public int writeManifestCalls;

    public FakeGenerateBackend() {
        this(DEFAULT_MAX_ROWS);
    }

    public FakeGenerateBackend(int maxRows) {
        this.maxRows = Math.max(1, maxRows);
    }

    @Override
    public void writeRawArray(
            String basePath,
            long rowCount,
            int parallelismHint,
            EventRowFactory factory,
            OrcWriteSettings orc) {
        writeRawCalls++;
        lastBasePath = basePath;
        lastOrc = orc;
        requestedRowCount = rowCount;
        events.clear();
        long limit = Math.min(rowCount, (long) maxRows);
        for (long i = 0; i < limit; i++) {
            events.add(factory.create(i));
        }
    }

    @Override
    public void writeReferences(
            String refsPath,
            Map<String, ReferenceDataset> datasets,
            long seed,
            String algo) {
        writeRefsCalls++;
        lastRefsPath = refsPath;
        refCounts.clear();
        if (datasets == null) {
            return;
        }
        ReferenceDataset module = datasets.get("module_dictionary");
        if (module != null && module.enabled) {
            int n = module.rowCount == null ? 500 : module.rowCount;
            refCounts.put("module_dictionary", Math.min(n, maxRows));
        }
        ReferenceDataset epk = datasets.get("epk_dictionary");
        if (epk != null && epk.enabled) {
            int n = epk.rowCount == null ? 100_000 : epk.rowCount;
            // Cap EPKs in fake mode — full 100k is fine in memory but keep tests light
            refCounts.put("epk_dictionary", Math.min(n, maxRows));
        }
        ReferenceDataset dates = datasets.get("partition_dates");
        if (dates != null && dates.enabled) {
            refCounts.put("partition_dates", 31);
        }
    }

    @Override
    public void writeManifest(String manifestPath, QuerySampleManifest manifest) {
        writeManifestCalls++;
        lastManifestPath = manifestPath;
        lastManifest = manifest;
    }

    @Override
    public long countRows(String basePath) {
        return events.size();
    }
}
