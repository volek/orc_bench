package orcbench.generate;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkSessionSupportTest {

    @Test
    void localJvmIsNotAYarnClusterDriver() {
        assertFalse(SparkSessionSupport.runningInsideYarnCluster());
    }

    @Test
    void yarnAppIdMarksTheClusterDriver() {
        System.setProperty("spark.yarn.app.id", "application_1_0001");
        try {
            assertTrue(SparkSessionSupport.runningInsideYarnCluster());
        } finally {
            System.clearProperty("spark.yarn.app.id");
        }
        assertFalse(SparkSessionSupport.runningInsideYarnCluster());
    }
}
