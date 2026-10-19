package orcbench.generate;

import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.ReferenceDataset;

import java.util.List;
import java.util.Map;

/**
 * Materialization sink for the generate phase. Unit tests inject
 * {@link FakeGenerateBackend}; cluster live path uses {@link SparkOrcGenerateBackend}.
 * Never run a live backend against HDFS from a developer laptop (plan §7.0).
 */
public interface GenerateBackend {

    /**
     * Write RAW_ARRAY_BASE ORC for row ids {@code [0, rowCount)}.
     *
     * @param factory builds one {@link EventRow} per row id (deterministic)
     */
    void writeRawArray(
            String basePath,
            long rowCount,
            int parallelismHint,
            EventRowFactory factory,
            OrcWriteSettings orc) throws Exception;

    /** Write reference ORC dictionaries under {@code refsPath/<name>}. */
    void writeReferences(
            String refsPath,
            Map<String, ReferenceDataset> datasets,
            long seed,
            String algo) throws Exception;

    /** Persist query-sample manifest (CSV/JSON under {@code manifestPath}). */
    void writeManifest(String manifestPath, QuerySampleManifest manifest) throws Exception;

    /** Optional post-write row-count check; return observed count or -1 if skipped. */
    long countRows(String basePath) throws Exception;

    @FunctionalInterface
    interface EventRowFactory {
        EventRow create(long rowId);
    }

    /** Simple id/type reference row for dictionaries. */
    final class RefRow {
        public final String id;
        public final String type;

        public RefRow(String id, String type) {
            this.id = id;
            this.type = type;
        }
    }

    /** Date/category reference row. */
    final class DateRefRow {
        public final String eventDate;
        public final String category;

        public DateRefRow(String eventDate, String category) {
            this.eventDate = eventDate;
            this.category = category;
        }
    }

    /** Helpers shared by backends when materializing refs from config. */
    final class RefBuilders {
        private RefBuilders() {}

        public static List<RefRow> moduleDictionary(int n) {
            List<RefRow> rows = new java.util.ArrayList<RefRow>(n);
            for (int i = 0; i < n; i++) {
                String t = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C");
                rows.add(new RefRow("module_" + i, t));
            }
            return rows;
        }

        public static List<RefRow> epkDictionary(int n) {
            List<RefRow> rows = new java.util.ArrayList<RefRow>(n);
            for (int i = 0; i < n; i++) {
                rows.add(new RefRow("epk_" + i, i % 2 == 0 ? "A" : "B"));
            }
            return rows;
        }

        public static List<DateRefRow> partitionDates(int days, int categoryCardinality) {
            int cats = Math.max(1, categoryCardinality);
            List<DateRefRow> rows = new java.util.ArrayList<DateRefRow>(days);
            for (int d = 0; d < days; d++) {
                String day = String.format("2026-01-%02d", d + 1);
                String cat = d % 2 == 0 ? "peak" : "offpeak";
                if (cats > 2) {
                    cat = "cat_" + (d % cats);
                }
                rows.add(new DateRefRow(day, cat));
            }
            return rows;
        }
    }
}
