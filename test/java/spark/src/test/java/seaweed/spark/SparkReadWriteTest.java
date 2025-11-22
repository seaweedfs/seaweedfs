package seaweed.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Integration tests for Spark read/write operations with SeaweedFS.
 */
public class SparkReadWriteTest extends SparkTestBase {

    @Test
    public void testWriteAndReadParquet() {
        skipIfTestsDisabled();

        // Create test data
        List<Person> people = Arrays.asList(
                new Person("Alice", 30),
                new Person("Bob", 25),
                new Person("Charlie", 35));

        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        // Write to SeaweedFS
        String outputPath = getTestPath("people.parquet");
        df.write().mode(SaveMode.Overwrite).parquet(outputPath);

        // Read back from SeaweedFS
        Dataset<Row> readDf = spark.read().parquet(outputPath);

        // Verify
        assertEquals(3, readDf.count());
        assertEquals(2, readDf.columns().length);

        List<Row> results = readDf.collectAsList();
        assertTrue(results.stream().anyMatch(r -> "Alice".equals(r.getAs("name")) && (Integer) r.getAs("age") == 30));
        assertTrue(results.stream().anyMatch(r -> "Bob".equals(r.getAs("name")) && (Integer) r.getAs("age") == 25));
        assertTrue(results.stream().anyMatch(r -> "Charlie".equals(r.getAs("name")) && (Integer) r.getAs("age") == 35));
    }

    @Test
    public void testWriteAndReadCSV() {
        skipIfTestsDisabled();

        // Create test data
        List<Person> people = Arrays.asList(
                new Person("Alice", 30),
                new Person("Bob", 25));

        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        // Write to SeaweedFS as CSV
        String outputPath = getTestPath("people.csv");
        df.write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath);

        // Read back from SeaweedFS
        Dataset<Row> readDf = spark.read().option("header", "true").option("inferSchema", "true").csv(outputPath);

        // Verify
        assertEquals(2, readDf.count());
        assertEquals(2, readDf.columns().length);
    }

    @Test
    public void testWriteAndReadJSON() {
        skipIfTestsDisabled();

        // Create test data
        List<Person> people = Arrays.asList(
                new Person("Alice", 30),
                new Person("Bob", 25),
                new Person("Charlie", 35));

        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        // Write to SeaweedFS as JSON
        String outputPath = getTestPath("people.json");
        df.write().mode(SaveMode.Overwrite).json(outputPath);

        // Read back from SeaweedFS
        Dataset<Row> readDf = spark.read().json(outputPath);

        // Verify
        assertEquals(3, readDf.count());
        assertEquals(2, readDf.columns().length);
    }

    @Test
    public void testWritePartitionedData() {
        skipIfTestsDisabled();

        // Create test data with multiple years
        List<PersonWithYear> people = Arrays.asList(
                new PersonWithYear("Alice", 30, 2020),
                new PersonWithYear("Bob", 25, 2021),
                new PersonWithYear("Charlie", 35, 2020),
                new PersonWithYear("David", 28, 2021));

        Dataset<Row> df = spark.createDataFrame(people, PersonWithYear.class);

        // Write partitioned data to SeaweedFS
        String outputPath = getTestPath("people_partitioned");
        df.write().mode(SaveMode.Overwrite).partitionBy("year").parquet(outputPath);

        // Read back from SeaweedFS
        Dataset<Row> readDf = spark.read().parquet(outputPath);

        // Verify
        assertEquals(4, readDf.count());

        // Verify partition filtering works
        Dataset<Row> filtered2020 = readDf.filter("year = 2020");
        assertEquals(2, filtered2020.count());

        Dataset<Row> filtered2021 = readDf.filter("year = 2021");
        assertEquals(2, filtered2021.count());
    }

    @Test
    public void testAppendMode() {
        skipIfTestsDisabled();

        String outputPath = getTestPath("people_append.parquet");

        // Write first batch
        List<Person> batch1 = Arrays.asList(
                new Person("Alice", 30),
                new Person("Bob", 25));
        Dataset<Row> df1 = spark.createDataFrame(batch1, Person.class);
        df1.write().mode(SaveMode.Overwrite).parquet(outputPath);

        // Append second batch
        List<Person> batch2 = Arrays.asList(
                new Person("Charlie", 35),
                new Person("David", 28));
        Dataset<Row> df2 = spark.createDataFrame(batch2, Person.class);
        df2.write().mode(SaveMode.Append).parquet(outputPath);

        // Read back and verify
        Dataset<Row> readDf = spark.read().parquet(outputPath);
        assertEquals(4, readDf.count());
    }

    @Test
    public void testLargeDataset() {
        skipIfTestsDisabled();

        // Create a larger dataset
        Dataset<Row> largeDf = spark.range(0, 10000)
                .selectExpr("id as value", "id * 2 as doubled");

        String outputPath = getTestPath("large_dataset.parquet");
        largeDf.write().mode(SaveMode.Overwrite).parquet(outputPath);

        // Read back and verify
        Dataset<Row> readDf = spark.read().parquet(outputPath);
        assertEquals(10000, readDf.count());

        // Verify some data (sort to ensure deterministic order)
        Row firstRow = readDf.orderBy("value").first();
        assertEquals(0L, firstRow.getLong(0));
        assertEquals(0L, firstRow.getLong(1));
    }

    // Test data classes
    public static class Person implements java.io.Serializable {
        private String name;
        private int age;

        public Person() {
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static class PersonWithYear implements java.io.Serializable {
        private String name;
        private int age;
        private int year;

        public PersonWithYear() {
        }

        public PersonWithYear(String name, int age, int year) {
            this.name = name;
            this.age = age;
            this.year = year;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }
    }
}
