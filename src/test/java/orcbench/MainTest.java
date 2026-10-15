package orcbench;

import orcbench.config.CliOptions;
import orcbench.model.Models.CliArgs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    @TempDir
    Path tmp;

    @Test
    void preflightDryRunCompletes() throws Exception {
        CliArgs args = CliOptions.parse(new String[]{
                "--config", "conf",
                "--scale", "S",
                "--phase", "preflight",
                "--dry-run",
                "--results-dir", tmp.toString()
        });
        assertNotNull(args);
        Main.run(args);
        assertTrue(Files.list(tmp).findAny().isPresent());
    }
}
