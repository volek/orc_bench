package orcbench.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class YamlSupportTest {

    @TempDir
    Path tmp;

    @Test
    void loadMapAndTextFromFile() throws Exception {
        Path yml = tmp.resolve("sample.yaml");
        Files.write(yml, "a:\n  b: 1\n  c: true\n  d: null\nlist:\n  - x\n  - y\n".getBytes(StandardCharsets.UTF_8));
        Map<String, Object> m = YamlSupport.loadMap(yml.toString());
        assertTrue(m.containsKey("a"));
        Map<String, Object> a = YamlSupport.nestedMap(m, "a");
        assertEquals(1, YamlSupport.intVal(a, "b", 0));
        assertTrue(YamlSupport.bool(a, "c", false));
        assertNull(YamlSupport.optStr(a, "d"));
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

    @Test
    void typedReaders() {
        Map<String, Object> m = new HashMap<>();
        m.put("i", 3);
        m.put("l", 9L);
        m.put("d", 1.5);
        m.put("b", "true");
        m.put("s", "ok");
        assertEquals(3, YamlSupport.intVal(m, "i", 0));
        assertEquals(9L, YamlSupport.longVal(m, "l", 0));
        assertEquals(1.5, YamlSupport.doubleVal(m, "d", 0), 1e-9);
        assertTrue(YamlSupport.bool(m, "b", false));
        assertEquals("ok", YamlSupport.str(m, "s", ""));
        assertNull(YamlSupport.optInt(m, "missing"));
        assertNull(YamlSupport.optLong(m, "missing"));
        assertNull(YamlSupport.optDouble(m, "missing"));
        assertNull(YamlSupport.optBool(m, "missing"));
    }

    @Test
    void resolvePathPrefersExisting() throws Exception {
        Path f = tmp.resolve("environment.yaml");
        Files.write(f, "environment: {}".getBytes(StandardCharsets.UTF_8));
        assertEquals(f.toString(), YamlSupport.resolvePath(tmp.toString(), "environment.yaml"));
        assertEquals("missing.yaml", YamlSupport.resolvePath(tmp.toString(), "missing.yaml"));
    }

    @Test
    void anyListAndStringMap() {
        Map<String, Object> m = new HashMap<>();
        m.put("vals", Arrays.asList(1, 2));
        Map<String, Object> nested = new HashMap<>();
        nested.put("k", "v");
        m.put("map", nested);
        List<Object> list = YamlSupport.anyList(m, "vals");
        assertEquals(2, list.size());
        assertEquals("v", YamlSupport.stringMap(m, "map").get("k"));
    }
}
