package orcbench.preflight;

import orcbench.model.Models.CapabilityReport;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;

/**
 * Minimal preflight artefacts under {@code results/<runId>/} (full reporting is Checkpoint 8).
 */
public final class PreflightReportWriter {
    private PreflightReportWriter() {}

    public static Path write(String resultsDir, String runId, CapabilityReport report, String scale)
            throws IOException {
        Path dir = Paths.get(resultsDir == null ? "results" : resultsDir, runId);
        Files.createDirectories(dir);

        try (BufferedWriter w = Files.newBufferedWriter(dir.resolve("SUMMARY.txt"), StandardCharsets.UTF_8)) {
            w.write("runId=" + runId);
            w.newLine();
            w.write("phase=preflight");
            w.newLine();
            w.write("scale=" + (scale == null ? "" : scale));
            w.newLine();
            w.write("skipped=" + report.skippedExperiments.size());
            w.newLine();
            w.write("capabilities=" + report.capabilities.size());
            w.newLine();
            w.write("notes=" + report.notes.size());
            w.newLine();
        }

        try (BufferedWriter w = Files.newBufferedWriter(dir.resolve("capabilities.tsv"), StandardCharsets.UTF_8)) {
            w.write("capability\tvalue");
            w.newLine();
            for (Map.Entry<String, Boolean> e : new TreeMap<>(report.capabilities).entrySet()) {
                w.write(e.getKey());
                w.write('\t');
                w.write(String.valueOf(e.getValue()));
                w.newLine();
            }
        }

        try (BufferedWriter w = Files.newBufferedWriter(dir.resolve("skipped.tsv"), StandardCharsets.UTF_8)) {
            w.write("experiment\treason");
            w.newLine();
            for (String[] row : report.skippedExperiments) {
                w.write(row[0] == null ? "" : row[0]);
                w.write('\t');
                w.write(row.length > 1 && row[1] != null ? row[1] : "");
                w.newLine();
            }
        }

        try (BufferedWriter w = Files.newBufferedWriter(dir.resolve("preflight-notes.txt"), StandardCharsets.UTF_8)) {
            for (String n : report.notes) {
                w.write(n);
                w.newLine();
            }
            for (Map.Entry<String, String> e : new TreeMap<>(report.versions).entrySet()) {
                w.write("version." + e.getKey() + "=" + e.getValue());
                w.newLine();
            }
        }
        return dir;
    }
}
