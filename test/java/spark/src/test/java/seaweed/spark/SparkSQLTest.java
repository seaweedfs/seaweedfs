package seaweed.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Integration tests for Spark SQL operations with SeaweedFS.
 */
public class SparkSQLTest extends SparkTestBase {

    @Test
    public void testCreateTableAndQuery() {
        skipIfTestsDisabled();

        // Create test data
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "Engineering", 100000),
            new Employee(2, "Bob", "Sales", 80000),
            new Employee(3, "Charlie", "Engineering", 120000),
            new Employee(4, "David", "Sales", 75000)
        );

        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);

        // Write to SeaweedFS
        String tablePath = getTestPath("employees");
        df.write().mode(SaveMode.Overwrite).parquet(tablePath);

        // Create temporary view
        Dataset<Row> employeesDf = spark.read().parquet(tablePath);
        employeesDf.createOrReplaceTempView("employees");

        // Run SQL queries
        Dataset<Row> engineeringEmployees = spark.sql(
            "SELECT name, salary FROM employees WHERE department = 'Engineering'"
        );
        
        assertEquals(2, engineeringEmployees.count());

        Dataset<Row> highPaidEmployees = spark.sql(
            "SELECT name, salary FROM employees WHERE salary > 90000"
        );
        
        assertEquals(2, highPaidEmployees.count());
    }

    @Test
    public void testAggregationQueries() {
        skipIfTestsDisabled();

        // Create sales data
        List<Sale> sales = Arrays.asList(
            new Sale("2024-01", "Product A", 100),
            new Sale("2024-01", "Product B", 150),
            new Sale("2024-02", "Product A", 120),
            new Sale("2024-02", "Product B", 180),
            new Sale("2024-03", "Product A", 110)
        );

        Dataset<Row> df = spark.createDataFrame(sales, Sale.class);

        // Write to SeaweedFS
        String tablePath = getTestPath("sales");
        df.write().mode(SaveMode.Overwrite).parquet(tablePath);

        // Create temporary view
        Dataset<Row> salesDf = spark.read().parquet(tablePath);
        salesDf.createOrReplaceTempView("sales");

        // Aggregate query
        Dataset<Row> monthlySales = spark.sql(
            "SELECT month, SUM(amount) as total FROM sales GROUP BY month ORDER BY month"
        );

        List<Row> results = monthlySales.collectAsList();
        assertEquals(3, results.size());
        assertEquals("2024-01", results.get(0).getString(0));
        assertEquals(250, results.get(0).getLong(1));
    }

    @Test
    public void testJoinOperations() {
        skipIfTestsDisabled();

        // Create employee data
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "Engineering", 100000),
            new Employee(2, "Bob", "Sales", 80000)
        );

        // Create department data
        List<Department> departments = Arrays.asList(
            new Department("Engineering", "Building Products"),
            new Department("Sales", "Selling Products")
        );

        Dataset<Row> empDf = spark.createDataFrame(employees, Employee.class);
        Dataset<Row> deptDf = spark.createDataFrame(departments, Department.class);

        // Write to SeaweedFS
        String empPath = getTestPath("employees_join");
        String deptPath = getTestPath("departments_join");
        
        empDf.write().mode(SaveMode.Overwrite).parquet(empPath);
        deptDf.write().mode(SaveMode.Overwrite).parquet(deptPath);

        // Read back and create views
        spark.read().parquet(empPath).createOrReplaceTempView("emp");
        spark.read().parquet(deptPath).createOrReplaceTempView("dept");

        // Join query
        Dataset<Row> joined = spark.sql(
            "SELECT e.name, e.salary, d.description " +
            "FROM emp e JOIN dept d ON e.department = d.name"
        );

        assertEquals(2, joined.count());
        
        List<Row> results = joined.collectAsList();
        assertTrue(results.stream().anyMatch(r -> 
            "Alice".equals(r.getString(0)) && "Building Products".equals(r.getString(2))
        ));
    }

    @Test
    public void testWindowFunctions() {
        skipIfTestsDisabled();

        // Create employee data with salaries
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "Engineering", 100000),
            new Employee(2, "Bob", "Engineering", 120000),
            new Employee(3, "Charlie", "Sales", 80000),
            new Employee(4, "David", "Sales", 90000)
        );

        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);

        String tablePath = getTestPath("employees_window");
        df.write().mode(SaveMode.Overwrite).parquet(tablePath);

        Dataset<Row> employeesDf = spark.read().parquet(tablePath);
        employeesDf.createOrReplaceTempView("employees_ranked");

        // Window function query - rank employees by salary within department
        Dataset<Row> ranked = spark.sql(
            "SELECT name, department, salary, " +
            "RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank " +
            "FROM employees_ranked"
        );

        assertEquals(4, ranked.count());
        
        // Verify Bob has rank 1 in Engineering (highest salary)
        List<Row> results = ranked.collectAsList();
        Row bobRow = results.stream()
            .filter(r -> "Bob".equals(r.getString(0)))
            .findFirst()
            .orElse(null);
        
        assertNotNull(bobRow);
        assertEquals(1, bobRow.getInt(3));
    }

    // Test data classes
    public static class Employee implements java.io.Serializable {
        private int id;
        private String name;
        private String department;
        private int salary;

        public Employee() {}

        public Employee(int id, String name, String department, int salary) {
            this.id = id;
            this.name = name;
            this.department = department;
            this.salary = salary;
        }

        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDepartment() { return department; }
        public void setDepartment(String department) { this.department = department; }
        public int getSalary() { return salary; }
        public void setSalary(int salary) { this.salary = salary; }
    }

    public static class Sale implements java.io.Serializable {
        private String month;
        private String product;
        private int amount;

        public Sale() {}

        public Sale(String month, String product, int amount) {
            this.month = month;
            this.product = product;
            this.amount = amount;
        }

        public String getMonth() { return month; }
        public void setMonth(String month) { this.month = month; }
        public String getProduct() { return product; }
        public void setProduct(String product) { this.product = product; }
        public int getAmount() { return amount; }
        public void setAmount(int amount) { this.amount = amount; }
    }

    public static class Department implements java.io.Serializable {
        private String name;
        private String description;

        public Department() {}

        public Department(String name, String description) {
            this.name = name;
            this.description = description;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
    }
}

