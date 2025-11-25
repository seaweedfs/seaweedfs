package seaweed.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Simplified test with only one column to isolate the EOF issue.
 */
public class SimpleOneColumnTest extends SparkTestBase {

    @Test
    public void testSingleIntegerColumn() {
        skipIfTestsDisabled();

        // Clean up any previous test data
        String tablePath = getTestPath("simple_data");
        try {
            spark.read().parquet(tablePath);
            // If we get here, path exists, so delete it
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(
                new java.net.URI(tablePath),
                spark.sparkContext().hadoopConfiguration());
            fs.delete(new org.apache.hadoop.fs.Path(tablePath), true);
        } catch (Exception e) {
            // Path doesn't exist, which is fine
        }

        // Create simple data with just one integer column
        List<SimpleData> data = Arrays.asList(
                new SimpleData(1),
                new SimpleData(2),
                new SimpleData(3),
                new SimpleData(4));

        Dataset<Row> df = spark.createDataFrame(data, SimpleData.class);

        // Write to SeaweedFS
        df.write().mode(SaveMode.Overwrite).parquet(tablePath);

        // Read back
        Dataset<Row> readDf = spark.read().parquet(tablePath);
        
        // Simple count
        assertEquals(4, readDf.count());
        
        // Create view and query
        readDf.createOrReplaceTempView("simple");
        
        // Simple WHERE query
        Dataset<Row> filtered = spark.sql("SELECT value FROM simple WHERE value > 2");
        assertEquals(2, filtered.count());
        
        // Verify values
        List<Row> results = filtered.collectAsList();
        assertTrue(results.stream().anyMatch(r -> r.getInt(0) == 3));
        assertTrue(results.stream().anyMatch(r -> r.getInt(0) == 4));
    }

    @Test
    public void testSingleStringColumn() {
        skipIfTestsDisabled();

        // Create simple data with just one string column
        List<StringData> data = Arrays.asList(
                new StringData("Alice"),
                new StringData("Bob"),
                new StringData("Charlie"),
                new StringData("David"));

        Dataset<Row> df = spark.createDataFrame(data, StringData.class);

        // Write to SeaweedFS
        String tablePath = getTestPath("string_data");
        df.write().mode(SaveMode.Overwrite).parquet(tablePath);

        // Read back
        Dataset<Row> readDf = spark.read().parquet(tablePath);
        
        // Simple count
        assertEquals(4, readDf.count());
        
        // Create view and query
        readDf.createOrReplaceTempView("strings");
        
        // Simple WHERE query
        Dataset<Row> filtered = spark.sql("SELECT name FROM strings WHERE name LIKE 'A%'");
        assertEquals(1, filtered.count());
        
        // Verify value
        List<Row> results = filtered.collectAsList();
        assertEquals("Alice", results.get(0).getString(0));
    }

    // Test data classes
    public static class SimpleData implements java.io.Serializable {
        private int value;

        public SimpleData() {
        }

        public SimpleData(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }

    public static class StringData implements java.io.Serializable {
        private String name;

        public StringData() {
        }

        public StringData(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}

