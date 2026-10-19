package orcbench;

import orcbench.config.CliOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    @Test
    void parsesHelpfulDefaults() {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "preflight",
                "--dry-run"
        });
        assertNotNull(a);
        assertEquals("conf", a.configDir);
        assertEquals("S", a.scale);
        assertEquals("preflight", a.phase);
        assertTrue(a.dryRun);
        assertFalse(a.mockGenerate);
        assertFalse(a.mockMaterialize);
        assertFalse(a.mockRun);
    }

    @Test
    void preflightDryRunWritesSkipMatrix(@TempDir Path tmp) throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "preflight",
                "--engines", "spark,hive_tez,hive_llap",
                "--results-dir", tmp.toString(),
                "--dry-run"
        });
        assertNotNull(a);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("Checkpoint 8") || out.contains("Checkpoint 7"));
        assertTrue(out.contains("=== Preflight ==="));
        assertTrue(out.contains("capability."));
        assertTrue(out.contains("preflight artefacts"));
        assertTrue(out.contains("OK — preflight complete"));

        assertTrue(Files.list(tmp).findAny().isPresent());
    }

    @Test
    void generateDryRunUsableLocally() throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "generate",
                "--dry-run"
        });
        assertNotNull(a);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("[generate]"));
        assertTrue(out.contains("dry-run") || out.contains("dryRun=true"));
        assertTrue(out.contains("RAW_ARRAY_BASE"));
        assertTrue(out.contains("OK — generate complete"));
    }

    @Test
    void generateMockUsableLocally() throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "generate",
                "--mock-generate"
        });
        assertNotNull(a);
        assertTrue(a.mockGenerate);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("mocked=true") || out.contains("mock"));
        assertTrue(out.contains("OK — generate complete"));
    }

    @Test
    void materializeDryRunUsableLocally() throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "materialize",
                "--experiments", "COMPRESSION,STRIPE_SIZE",
                "--dry-run"
        });
        assertNotNull(a);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("[materialize]"));
        assertTrue(out.contains("dry-run") || out.contains("dryRun=true"));
        assertTrue(out.contains("COMPRESSION") || out.contains("STRIPE_SIZE"));
        assertTrue(out.contains("OK — materialize complete"));
    }

    @Test
    void materializeMockUsableLocally() throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "materialize",
                "--experiments", "COMPRESSION",
                "--mock-materialize"
        });
        assertNotNull(a);
        assertTrue(a.mockMaterialize);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("mocked=true") || out.contains("mock"));
        assertTrue(out.contains("OK — materialize complete"));
        assertTrue(out.contains("written="));
    }

    @Test
    void runDryRunUsableLocally() throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "run",
                "--engines", "spark",
                "--experiments", "SPARK_VECTORIZATION,ORC_COLUMN_PRUNING",
                "--dry-run"
        });
        assertNotNull(a);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("[run]"));
        assertTrue(out.contains("dry-run") || out.contains("dryRun=true"));
        assertTrue(out.contains("OK — run complete") || out.contains("Spark run complete"));
    }

    @Test
    void runMockUsableLocally(@TempDir Path tmp) throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "run",
                "--engines", "spark",
                "--experiments", "SPARK_BASE",
                "--results-dir", tmp.toString(),
                "--mock-run"
        });
        assertNotNull(a);
        assertTrue(a.mockRun);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("mocked=true") || out.contains("mock"));
        assertTrue(out.contains("OK — run complete") || out.contains("Spark run complete"));
        assertTrue(out.contains("attempts="));
        assertTrue(Files.list(tmp).findAny().isPresent());
    }

    @Test
    void hiveTezDryRunUsableLocally() throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "run",
                "--engines", "hive_tez",
                "--experiments", "HIVE_TEZ_BASE,HIVE_VECTORIZATION,HIVE_MATERIALIZED_VIEW",
                "--dry-run"
        });
        assertNotNull(a);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("[run]"));
        assertTrue(out.contains("hive") || out.contains("HIVE_"));
        assertTrue(out.contains("dry-run") || out.contains("dryRun=true"));
        assertTrue(out.contains("OK — run complete"));
    }

    @Test
    void hiveLlapMockUsableLocally(@TempDir Path tmp) throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "run",
                "--engines", "hive_llap",
                "--experiments", "HIVE_LLAP",
                "--results-dir", tmp.toString(),
                "--mock-run"
        });
        assertNotNull(a);
        assertTrue(a.mockRun);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("hive") || out.contains("HIVE_LLAP") || out.contains("mock"));
        assertTrue(out.contains("OK — run complete"));
        assertTrue(out.contains("attempts="));
        assertTrue(Files.list(tmp).findAny().isPresent());
    }

    @Test
    void dryRunAllIncludesFinalReport(@TempDir Path tmp) throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "all",
                "--engines", "spark,hive_tez",
                "--experiments", "ORC_COLUMN_PRUNING,SPARK_VECTORIZATION,HIVE_TEZ_BASE",
                "--results-dir", tmp.toString(),
                "--dry-run"
        });
        assertNotNull(a);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("[run]") || out.contains("[generate]") || out.contains("[materialize]"));
        assertTrue(out.contains("[report]") || out.contains("final"));
        assertTrue(out.contains("Checkpoint 8"));
        assertTrue(out.contains("OK — final complete") || out.contains("final complete"));
    }

    @Test
    void reportPhaseFromFixtureMetrics(@TempDir Path tmp) throws Exception {
        Path fixture = Paths.get("src/test/resources/fixtures/cp8/metrics.jsonl");
        assertTrue(Files.isRegularFile(fixture));

        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "report",
                "--results-dir", tmp.toString(),
                "--run-id", "fixture-cp8",
                "--metrics-jsonl", fixture.toString()
        });
        assertNotNull(a);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("[report]"));
        assertTrue(out.contains("BEST_ORC") || out.contains("bestOrc") || out.contains("artefacts"));
        assertTrue(out.contains("OK — report complete"));
        assertTrue(Files.exists(tmp.resolve("fixture-cp8").resolve("SUMMARY.txt")));
        assertTrue(Files.exists(tmp.resolve("fixture-cp8").resolve("summary.csv")));
        assertTrue(Files.exists(tmp.resolve("fixture-cp8").resolve("skipped.tsv")));
    }

    @Test
    void finalPhaseDryRunUsableLocally(@TempDir Path tmp) throws Exception {
        CliOptions.CliArgs a = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "final",
                "--results-dir", tmp.toString(),
                "--dry-run"
        });
        assertNotNull(a);

        PrintStream previous = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buf, true, "UTF-8"));
        try {
            Main.run(a);
        } finally {
            System.setOut(previous);
        }
        String out = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(out.contains("[report]") || out.contains("final"));
        assertTrue(out.contains("final_comparison") || out.contains("finalCells")
                || out.contains("BEST_ORC") || out.contains("dry-run"));
        assertTrue(out.contains("OK — final complete"));
    }
}
