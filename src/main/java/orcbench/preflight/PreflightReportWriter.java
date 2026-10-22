package orcbench.preflight;

import orcbench.io.ResultStore;
import orcbench.model.Models.CapabilityReport;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Preflight artefacts under {@code <resultsDir>/<runId>/}.
 * On a YARN driver {@code resultsDir} is an HDFS directory.
 */
public final class PreflightReportWriter {
    private PreflightReportWriter() {}

    public static String write(String resultsDir, String runId, CapabilityReport report, String scale)
            throws IOException {
        String dir = ResultStore.child(resultsDir == null ? "results" : resultsDir, runId);
        ResultStore.mkdirs(dir);

        StringBuilder summary = new StringBuilder();
        summary.append("runId=").append(runId).append('\n');
        summary.append("phase=preflight\n");
        summary.append("scale=").append(scale == null ? "" : scale).append('\n');
        summary.append("skipped=").append(report.skippedExperiments.size()).append('\n');
        summary.append("capabilities=").append(report.capabilities.size()).append('\n');
        summary.append("notes=").append(report.notes.size()).append('\n');
        ResultStore.writeUtf8(ResultStore.child(dir, "SUMMARY.txt"), summary.toString());

        StringBuilder caps = new StringBuilder();
        caps.append("capability\tvalue\n");
        for (Map.Entry<String, Boolean> e : new TreeMap<String, Boolean>(report.capabilities).entrySet()) {
            caps.append(e.getKey()).append('\t').append(String.valueOf(e.getValue())).append('\n');
        }
        ResultStore.writeUtf8(ResultStore.child(dir, "capabilities.tsv"), caps.toString());

        StringBuilder skipped = new StringBuilder();
        skipped.append("experiment\treason\n");
        for (String[] row : report.skippedExperiments) {
            skipped.append(row[0] == null ? "" : row[0]).append('\t');
            skipped.append(row.length > 1 && row[1] != null ? row[1] : "").append('\n');
        }
        ResultStore.writeUtf8(ResultStore.child(dir, "skipped.tsv"), skipped.toString());

        StringBuilder notes = new StringBuilder();
        for (String n : report.notes) {
            notes.append(n).append('\n');
        }
        for (Map.Entry<String, String> e : new TreeMap<String, String>(report.versions).entrySet()) {
            notes.append("version.").append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        ResultStore.writeUtf8(ResultStore.child(dir, "preflight-notes.txt"), notes.toString());
        return dir;
    }
}
