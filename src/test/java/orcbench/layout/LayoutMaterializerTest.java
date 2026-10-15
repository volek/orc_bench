package orcbench.layout;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.*;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LayoutMaterializerTest {

    @Test
    void dryRunDoesNotWriteLayouts() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> instances = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("COMPRESSION"));
        List<ExperimentInstance> out = LayoutMaterializer.materialize(
                null, cfg, "S", instances, true, false, Collections.emptyList());
        assertEquals(instances.size(), out.size());
        assertEquals(instances.get(0).layoutPath, out.get(0).layoutPath);
    }
}
