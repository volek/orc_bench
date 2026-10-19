package orcbench.config;

import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** SnakeYAML helpers for raw maps and typed ConfigLoader parsing. */
public final class YamlSupport {
    private static final Yaml YAML = new Yaml();

    private YamlSupport() {}

    @SuppressWarnings("unchecked")
    public static Map<String, Object> loadMap(String path) throws Exception {
        File file = new File(path);
        InputStream in;
        if (file.exists()) {
            in = new FileInputStream(file);
        } else {
            in = YamlSupport.class.getClassLoader().getResourceAsStream(file.getName());
            if (in == null) {
                throw new IllegalArgumentException("Config not found: " + path);
            }
        }
        try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            Object raw = YAML.load(reader);
            return asMap(raw);
        }
    }

    public static String loadText(String path) throws Exception {
        File file = new File(path);
        if (file.exists()) {
            byte[] bytes = Files.readAllBytes(file.toPath());
            return new String(bytes, StandardCharsets.UTF_8);
        }
        InputStream in = YamlSupport.class.getClassLoader().getResourceAsStream(file.getName());
        if (in == null) {
            throw new IllegalArgumentException("Resource not found: " + path);
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) >= 0) {
                baos.write(buf, 0, n);
            }
            return new String(baos.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            in.close();
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asMap(Object v) {
        if (v == null) {
            return new LinkedHashMap<>();
        }
        Map<Object, Object> m = (Map<Object, Object>) v;
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> e : m.entrySet()) {
            out.put(String.valueOf(e.getKey()), normalize(e.getValue()));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    public static Object normalize(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Map) {
            return asMap(v);
        }
        if (v instanceof List) {
            List<Object> src = (List<Object>) v;
            List<Object> out = new ArrayList<>(src.size());
            for (Object o : src) {
                out.add(normalize(o));
            }
            return out;
        }
        return v;
    }

    public static String str(Map<String, Object> m, String key, String def) {
        Object v = m.get(key);
        return v == null ? def : v.toString();
    }

    public static String optStr(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v == null ? null : v.toString();
    }

    public static boolean bool(Map<String, Object> m, String key, boolean def) {
        Object v = m.get(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        return Boolean.parseBoolean(v.toString());
    }

    public static Boolean optBool(Map<String, Object> m, String key) {
        if (!m.containsKey(key) || m.get(key) == null) {
            return null;
        }
        return bool(m, key, false);
    }

    public static int intVal(Map<String, Object> m, String key, int def) {
        Object v = m.get(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        try {
            return Integer.parseInt(v.toString());
        } catch (Exception e) {
            return def;
        }
    }

    public static Integer optInt(Map<String, Object> m, String key) {
        if (!m.containsKey(key) || m.get(key) == null) {
            return null;
        }
        return intVal(m, key, 0);
    }

    public static long longVal(Map<String, Object> m, String key, long def) {
        Object v = m.get(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        try {
            return Long.parseLong(v.toString());
        } catch (Exception e) {
            return def;
        }
    }

    public static Long optLong(Map<String, Object> m, String key) {
        if (!m.containsKey(key) || m.get(key) == null) {
            return null;
        }
        return longVal(m, key, 0L);
    }

    public static double doubleVal(Map<String, Object> m, String key, double def) {
        Object v = m.get(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }
        try {
            return Double.parseDouble(v.toString());
        } catch (Exception e) {
            return def;
        }
    }

    public static Double optDouble(Map<String, Object> m, String key) {
        if (!m.containsKey(key) || m.get(key) == null) {
            return null;
        }
        return doubleVal(m, key, 0.0);
    }

    @SuppressWarnings("unchecked")
    public static List<String> stringList(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (!(v instanceof List)) {
            return new ArrayList<>();
        }
        List<String> out = new ArrayList<>();
        for (Object o : (List<Object>) v) {
            if (o != null) {
                out.add(o.toString());
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> anyList(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (!(v instanceof List)) {
            return new ArrayList<>();
        }
        return new ArrayList<>((List<Object>) v);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> stringMap(Map<String, Object> m, String key) {
        Object v = m.get(key);
        Map<String, String> out = new LinkedHashMap<>();
        if (!(v instanceof Map)) {
            return out;
        }
        for (Map.Entry<String, Object> e : ((Map<String, Object>) v).entrySet()) {
            if (e.getValue() != null) {
                out.put(String.valueOf(e.getKey()), e.getValue().toString());
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> nestedMap(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v instanceof Map) {
            return (Map<String, Object>) v;
        }
        return new LinkedHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> deepMerge(Map<String, Object> base, Map<String, Object> overlay) {
        Map<String, Object> out = new LinkedHashMap<>(base);
        for (Map.Entry<String, Object> e : overlay.entrySet()) {
            Object bv = out.get(e.getKey());
            Object ov = e.getValue();
            if (bv instanceof Map && ov instanceof Map) {
                out.put(e.getKey(), deepMerge((Map<String, Object>) bv, (Map<String, Object>) ov));
            } else {
                out.put(e.getKey(), ov);
            }
        }
        return out;
    }

    public static String resolvePath(String configDir, String name) {
        java.nio.file.Path p = Paths.get(configDir, name);
        if (Files.exists(p)) {
            return p.toString();
        }
        return name;
    }
}
