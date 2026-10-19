package orcbench.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class YamlSupportTest {

    @TempDir
    Path tmp;

    @Test
    void loadMapAndTextFromFile() throws Exception {
        Path yml = tmp.resolve("sample.yaml");
        Files.write(yml, "a:\n  b: 1\n  c: true\nlist:\n  - x\n  - y\n".getBytes(StandardCharsets.UTF_8));
        Map<String, Object> m = YamlSupport.loadMap(yml.toString());
        Map<String, Object> a = YamlSupport.nestedMap(m, "a");
        assertEquals(1, YamlSupport.intVal(a, "b", 0));
        assertTrue(YamlSupport.bool(a, "c", false));
        assertEquals(Arrays.asList("x", "y"), YamlSupport.stringList(m, "list"));

        Path txt = tmp.resolve("t.txt");
        Files.write(txt, "hello".getBytes(StandardCharsets.UTF_8));
        assertEquals("hello", YamlSupport.loadText(txt.toString()));
    }

    @Test
    void deepMergeOverlaysNested() {
        Map<String, Object> base = new HashMap<>();
        Map<String, Object> inner = new HashMap<>();
        inner.put("x", 1);
        inner.put("y", 2);
        base.put("n", inner);
        base.put("keep", "a");

        Map<String, Object> overlay = new HashMap<>();
        Map<String, Object> oi = new HashMap<>();
        oi.put("y", 9);
        overlay.put("n", oi);

        Map<String, Object> merged = YamlSupport.deepMerge(base, overlay);
        assertEquals("a", merged.get("keep"));
        @SuppressWarnings("unchecked")
        Map<String, Object> n = (Map<String, Object>) merged.get("n");
        assertEquals(1, n.get("x"));
        assertEquals(9, n.get("y"));
    }
}
