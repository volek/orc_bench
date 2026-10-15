package orcbench.engine.spark;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SparkQueryRunnerTest {

    @TempDir
    Path tmp;

    @Test
    void hashRowsIsOrderIndependent() {
        String h1 = SparkQueryRunner.hashRows(new String[]{"a|1", "b|2"});
        String h2 = SparkQueryRunner.hashRows(new String[]{"b|2", "a|1"});
        assertEquals(h1, h2);
        assertEquals(16, h1.length());
    }

    @Test
    void skippedExperimentReturnsSkippedStatus() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "X";
        inst.variantLabel = "v";
        inst.skipReason = "nope";
        inst.layoutPath = tmp.toString();
        List<RunResult> results = SparkQueryRunner.runExperiment(
                null, cfg, inst, new QuerySampleManifest(), "run", null);
        assertEquals(1, results.size());
        assertEquals("skipped", results.get(0).status);
        assertEquals("nope", results.get(0).errorMessage);
    }
}
