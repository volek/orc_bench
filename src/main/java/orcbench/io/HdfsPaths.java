package orcbench.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Byte size of an ORC tree. Used for {@code orcFileBytes} on both the Spark
 * and Hive paths so {@code scanRatio = hdfsBytesRead / orcFileBytes} has a
 * denominator.
 */
public final class HdfsPaths {
    private HdfsPaths() {}

    /**
     * Sum of file lengths under {@code layoutPath}, recursively.
     * Returns null when the path cannot be listed.
     */
    public static Long sumFileBytes(String layoutPath) {
        return sumFileBytes(layoutPath, new Configuration());
    }

    public static Long sumFileBytes(String layoutPath, Configuration conf) {
        if (layoutPath == null || layoutPath.isEmpty()) {
            return null;
        }
        try {
            Path p = new Path(layoutPath);
            Configuration hconf = conf == null ? new Configuration() : conf;
            FileSystem fs = p.getFileSystem(hconf);
            long sum = 0L;
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(p, true);
            while (it.hasNext()) {
                sum += it.next().getLen();
            }
            return Long.valueOf(sum);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Immediate child names, or null when the path cannot be listed.
     * An empty list means the path exists and has no children.
     */
    public static List<String> listChildNames(String layoutPath) {
        if (layoutPath == null || layoutPath.isEmpty()) {
            return null;
        }
        try {
            Path p = new Path(layoutPath);
            FileSystem fs = p.getFileSystem(new Configuration());
            if (!fs.exists(p)) {
                return new ArrayList<String>();
            }
            FileStatus[] kids = fs.listStatus(p);
            List<String> names = new ArrayList<String>();
            if (kids != null) {
                for (FileStatus st : kids) {
                    names.add(st.getPath().getName());
                }
            }
            return names;
        } catch (Exception ex) {
            return null;
        }
    }
}
