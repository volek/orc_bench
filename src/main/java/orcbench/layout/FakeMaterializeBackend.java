package orcbench.layout;

import orcbench.model.Models.LayoutWriteRequest;
import orcbench.model.Models.OrcWriteSettings;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * In-memory materialize sink for unit tests and local {@code --mock-materialize}.
 * Never touches HDFS / YARN.
 */
public final class FakeMaterializeBackend implements MaterializeBackend {

    /** Paths that exist (directories or files). Markers stored under {@link #markers}. */
    public final Set<String> paths = new LinkedHashSet<>();
    public final Map<String, String> markers = new LinkedHashMap<>();
    public final Map<String, Long> pathBytes = new LinkedHashMap<>();
    public final List<LayoutWriteRequest> writes = new ArrayList<>();
    public final List<String> deleted = new ArrayList<>();
    public final Map<String, String> localTexts = new LinkedHashMap<>();

    public int writeLayoutCalls;
    public int writeMarkerCalls;
    public int deleteCalls;

    /** Seed a RAW_ARRAY_BASE path so materialize can "read" a source. */
    public void seedBaseline(String basePath, long bytes) {
        paths.add(normalize(basePath));
        pathBytes.put(normalize(basePath), Math.max(1L, bytes));
        // parent scale dir
        int slash = basePath.lastIndexOf('/');
        if (slash > 0) {
            paths.add(normalize(basePath.substring(0, slash)));
        }
    }

    @Override
    public boolean pathExists(String path) {
        String n = normalize(path);
        if (paths.contains(n) || markers.containsKey(n)) {
            return true;
        }
        for (String p : paths) {
            if (p.startsWith(n + "/")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean markerExists(String markerPath) {
        return markers.containsKey(normalize(markerPath));
    }

    @Override
    public void writeMarker(String markerPath, String content) {
        writeMarkerCalls++;
        String n = normalize(markerPath);
        if (markers.containsKey(n)) {
            return; // idempotency: never overwrite
        }
        markers.put(n, content == null ? "" : content);
        paths.add(n);
        // ensure parent state dir exists
        int slash = n.lastIndexOf('/');
        if (slash > 0) {
            paths.add(n.substring(0, slash));
        }
    }

    @Override
    public long estimateBytes(String basePath) {
        Long v = pathBytes.get(normalize(basePath));
        return v == null ? 1L : Math.max(1L, v);
    }

    @Override
    public void writeLayout(LayoutWriteRequest request) {
        writeLayoutCalls++;
        if (request == null || request.layoutPath == null) {
            return;
        }
        // Defensive copy of settings for assertions
        LayoutWriteRequest copy = shallowCopy(request);
        writes.add(copy);
        String n = normalize(request.layoutPath);
        paths.add(n);
        long approx = Math.max(1L, request.sourceBytesEstimate / Math.max(1, request.targetFileSizeMib));
        pathBytes.put(n, Math.max(1L, approx) * 1024L * 1024L);
    }

    @Override
    public List<String> listChildren(String directory) {
        String prefix = normalize(directory) + "/";
        Set<String> kids = new LinkedHashSet<>();
        for (String p : paths) {
            if (!p.startsWith(prefix)) {
                continue;
            }
            String rest = p.substring(prefix.length());
            int slash = rest.indexOf('/');
            String name = slash < 0 ? rest : rest.substring(0, slash);
            if (!name.isEmpty()) {
                kids.add(name);
            }
        }
        return new ArrayList<>(kids);
    }

    @Override
    public void deleteRecursive(String path) {
        deleteCalls++;
        String n = normalize(path);
        deleted.add(n);
        List<String> remove = new ArrayList<>();
        for (String p : paths) {
            if (p.equals(n) || p.startsWith(n + "/")) {
                remove.add(p);
            }
        }
        paths.removeAll(remove);
        for (String r : remove) {
            pathBytes.remove(r);
            markers.remove(r);
        }
    }

    @Override
    public void writeLocalText(String relativePath, String content) throws Exception {
        localTexts.put(relativePath, content == null ? "" : content);
        // Also mirror to local FS under CWD for smoke parity with live path
        Path out = Paths.get(relativePath);
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        Files.write(out, (content == null ? "" : content).getBytes(StandardCharsets.UTF_8));
    }

    private static LayoutWriteRequest shallowCopy(LayoutWriteRequest src) {
        LayoutWriteRequest c = new LayoutWriteRequest();
        c.experimentId = src.experimentId;
        c.variantLabel = src.variantLabel;
        c.kind = src.kind;
        c.sourceBasePath = src.sourceBasePath;
        c.layoutPath = src.layoutPath;
        c.representation = src.representation;
        c.orcSettings = src.orcSettings == null ? new OrcWriteSettings() : src.orcSettings.copy();
        c.partitionColumns = src.partitionColumns == null
                ? Collections.<String>emptyList()
                : new ArrayList<>(src.partitionColumns);
        c.sorting = src.sorting == null
                ? Collections.<String>emptyList()
                : new ArrayList<>(src.sorting);
        c.flatColumns = src.flatColumns == null
                ? Collections.<String>emptyList()
                : new ArrayList<>(src.flatColumns);
        c.bucketCount = src.bucketCount;
        c.bucketKey = src.bucketKey;
        c.targetFileSizeMib = src.targetFileSizeMib;
        c.sourceBytesEstimate = src.sourceBytesEstimate;
        c.markerPath = src.markerPath;
        return c;
    }

    private static String normalize(String path) {
        if (path == null) {
            return "";
        }
        String p = path.replace('\\', '/');
        while (p.endsWith("/") && p.length() > 1) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }
}
