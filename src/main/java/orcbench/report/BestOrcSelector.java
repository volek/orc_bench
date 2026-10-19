package orcbench.report;

import orcbench.engine.hive.HiveJdbcRunner;
import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.BestOrcChoice;
import orcbench.model.Models.BestOrcRule;
import orcbench.model.Models.ExperimentInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Selects {@code BEST_ORC} from measured aggregates per {@code best_orc_rule}.
 * Hive-shared recommendations never pick ZSTD (ORC 1.5.6 codec gap).
 */
public final class BestOrcSelector {
    private BestOrcSelector() {}

    public static BestOrcChoice select(
            List<AggregateStats> aggregates,
            BestOrcRule rule,
            List<ExperimentInstance> physicalLayouts) {
        BestOrcRule effective = rule == null ? new BestOrcRule() : rule;
        BestOrcChoice none = new BestOrcChoice();
        none.notes.add("no BEST_ORC candidate matched objective queries + SLA gate");

        Map<String, ExperimentInstance> layouts = indexLayouts(physicalLayouts);
        Set<String> objective = new HashSet<String>(effective.objectiveQueries);
        if (objective.isEmpty()) {
            objective.add("Q02");
            objective.add("Q03");
            objective.add("Q07_NARROW");
        }

        Map<String, List<AggregateStats>> byCandidate = new HashMap<String, List<AggregateStats>>();
        if (aggregates != null) {
            for (AggregateStats a : aggregates) {
                if (a == null || a.concurrency != 1) {
                    continue;
                }
                if (!objective.contains(a.queryId)) {
                    continue;
                }
                String key = candidateKey(a.experimentId, a.variantLabel);
                ExperimentInstance layout = layouts.get(key);
                if (!layouts.isEmpty() && layout == null) {
                    // Prefer explicit physical rewrite/baseline layouts when provided.
                    continue;
                }
                if (layout != null && !isLayoutCandidate(layout)) {
                    continue;
                }
                if (effective.excludeZstd && isZstdExcluded(layout, a)) {
                    continue;
                }
                List<AggregateStats> bucket = byCandidate.get(key);
                if (bucket == null) {
                    bucket = new ArrayList<AggregateStats>();
                    byCandidate.put(key, bucket);
                }
                bucket.add(a);
            }
        }

        BestOrcChoice best = null;
        for (Map.Entry<String, List<AggregateStats>> e : byCandidate.entrySet()) {
            List<AggregateStats> rows = e.getValue();
            if (rows.isEmpty()) {
                continue;
            }
            double sumStat = 0;
            double sumSla = 0;
            for (AggregateStats a : rows) {
                sumStat += "avg".equals(effective.objectiveStat) ? a.avgMs : a.p50Ms;
                sumSla += a.slaSuccessRate;
            }
            int n = rows.size();
            double meanStat = sumStat / n;
            double meanSla = sumSla / n;
            boolean slaOk = meanSla + 1e-12 >= effective.slaSuccessRateMin;

            AggregateStats head = rows.get(0);
            ExperimentInstance layout = layouts.get(e.getKey());
            String tieClass = tieBreakClass(layout, head);
            int tieRank = tieRank(tieClass);

            BestOrcChoice cand = new BestOrcChoice();
            cand.experimentId = head.experimentId;
            cand.variantLabel = head.variantLabel;
            cand.layoutPath = layout != null ? layout.layoutPath : null;
            cand.meanP50Ms = meanStat;
            cand.meanSlaSuccessRate = meanSla;
            cand.slaPassed = slaOk;
            cand.tieBreakClass = tieClass;
            cand.notes.add("objectiveQueries=" + effective.objectiveQueries
                    + " n=" + n
                    + " meanStat=" + String.format(Locale.ROOT, "%.2f", meanStat)
                    + " meanSla=" + String.format(Locale.ROOT, "%.4f", meanSla)
                    + " tie=" + tieClass);

            if (!slaOk) {
                continue;
            }
            if (best == null
                    || meanStat < best.meanP50Ms - 1e-9
                    || (Math.abs(meanStat - best.meanP50Ms) < 1e-9
                    && tieRank < tieRank(best.tieBreakClass))) {
                best = cand;
            }
        }

        if (best == null) {
            // Fallback: ignore SLA gate but still exclude ZSTD — useful for sparse fixtures.
            for (Map.Entry<String, List<AggregateStats>> e : byCandidate.entrySet()) {
                List<AggregateStats> rows = e.getValue();
                if (rows.isEmpty()) {
                    continue;
                }
                double sumStat = 0;
                double sumSla = 0;
                for (AggregateStats a : rows) {
                    sumStat += "avg".equals(effective.objectiveStat) ? a.avgMs : a.p50Ms;
                    sumSla += a.slaSuccessRate;
                }
                int n = rows.size();
                AggregateStats head = rows.get(0);
                ExperimentInstance layout = layouts.get(e.getKey());
                BestOrcChoice cand = new BestOrcChoice();
                cand.experimentId = head.experimentId;
                cand.variantLabel = head.variantLabel;
                cand.layoutPath = layout != null ? layout.layoutPath : null;
                cand.meanP50Ms = sumStat / n;
                cand.meanSlaSuccessRate = sumSla / n;
                cand.slaPassed = cand.meanSlaSuccessRate + 1e-12 >= effective.slaSuccessRateMin;
                cand.tieBreakClass = tieBreakClass(layout, head);
                cand.notes.add("selected without SLA gate (no candidate passed "
                        + effective.slaSuccessRateMin + ")");
                if (best == null
                        || cand.meanP50Ms < best.meanP50Ms - 1e-9
                        || (Math.abs(cand.meanP50Ms - best.meanP50Ms) < 1e-9
                        && tieRank(cand.tieBreakClass) < tieRank(best.tieBreakClass))) {
                    best = cand;
                }
            }
        }

        if (best == null) {
            return none;
        }
        if (best.layoutPath == null && physicalLayouts != null) {
            for (ExperimentInstance i : physicalLayouts) {
                if (i != null
                        && safeEq(i.experimentId, best.experimentId)
                        && safeEq(i.variantLabel, best.variantLabel)) {
                    best.layoutPath = i.layoutPath;
                    break;
                }
            }
        }
        if (effective.excludeZstd) {
            best.notes.add("ZSTD excluded from BEST_ORC (Hive ORC 1.5.6 codec gap)");
        }
        return best;
    }

    public static boolean isZstdExcluded(ExperimentInstance layout, AggregateStats a) {
        if (layout != null && HiveJdbcRunner.isHiveZstdExcluded(layout)) {
            return true;
        }
        if (a != null) {
            if (a.variantLabel != null
                    && a.variantLabel.toUpperCase(Locale.ROOT).contains("ZSTD")) {
                return true;
            }
            if ("COMPRESSION".equals(a.experimentId)
                    && a.variantLabel != null
                    && a.variantLabel.toUpperCase(Locale.ROOT).contains("ZSTD")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLayoutCandidate(ExperimentInstance i) {
        if (i == null) {
            return false;
        }
        String kind = i.kind == null ? "" : i.kind;
        if ("query_only".equals(kind) || "reader_settings".equals(kind)
                || "cluster_read".equals(kind)) {
            return false;
        }
        return i.skipReason == null;
    }

    private static Map<String, ExperimentInstance> indexLayouts(List<ExperimentInstance> physical) {
        Map<String, ExperimentInstance> map = new HashMap<String, ExperimentInstance>();
        if (physical == null) {
            return map;
        }
        for (ExperimentInstance i : physical) {
            if (i == null) {
                continue;
            }
            map.put(candidateKey(i.experimentId, i.variantLabel), i);
        }
        return map;
    }

    private static String candidateKey(String experimentId, String variantLabel) {
        return nullSafe(experimentId) + "|" + nullSafe(variantLabel);
    }

    private static String tieBreakClass(ExperimentInstance layout, AggregateStats head) {
        if (layout != null) {
            if (layout.partitionColumns != null && !layout.partitionColumns.isEmpty()) {
                return "partitioned";
            }
            if (layout.sorting != null && !layout.sorting.isEmpty()) {
                return "sorted";
            }
            if ("RAW_ARRAY_BASE".equals(layout.experimentId)
                    || "baseline".equals(layout.kind)) {
                return "baseline";
            }
        }
        String id = head == null ? "" : nullSafe(head.experimentId).toUpperCase(Locale.ROOT);
        String var = head == null ? "" : nullSafe(head.variantLabel).toUpperCase(Locale.ROOT);
        if (id.contains("PARTITION") || var.contains("PARTITION") || var.contains("EVENT_DATE")) {
            return "partitioned";
        }
        if (id.contains("SORT") || var.contains("SORT")) {
            return "sorted";
        }
        if (id.contains("RAW_ARRAY_BASE") || "BASELINE".equals(id)) {
            return "baseline";
        }
        return "other";
    }

    /** Lower is better: partitioned(0) > sorted(1) > baseline(2) > other(3). */
    private static int tieRank(String tieClass) {
        if ("partitioned".equals(tieClass)) {
            return 0;
        }
        if ("sorted".equals(tieClass)) {
            return 1;
        }
        if ("baseline".equals(tieClass)) {
            return 2;
        }
        return 3;
    }

    private static boolean safeEq(String a, String b) {
        return nullSafe(a).equals(nullSafe(b));
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }
}
