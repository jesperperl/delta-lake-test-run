from delta_operations import create_delta_table

# Sample employee data
employee_data = [
    ("John Doe", 35, "Engineering", 120000.0),
    ("Jane Smith", 28, "Marketing", 95000.0),
    ("Robert Brown", 42, "Sales", 110000.0),
    ("Emily Davis", 31, "HR", 85000.0),
    ("Michael Wilson", 39, "Engineering", 125000.0)
]

# Define schema
employee_schema = ["name", "age", "department", "salary"]

# Create a delta table with the employee data
employees_df = create_delta_table(
    table_name="employees",
    data=employee_data,
    schema=employee_schema,
    table_path="/app/data/delta/employees"
)

# Show the created table
print("Created table content:")
employees_df.show()

# Run a simple query
print("Engineers with salary > 120k:")
engineers = employees_df.filter((employees_df.department == "Engineering") &
                               (employees_df.salary > 120000))
engineers.show()

# Demonstrate time travel capabilities
print("Updating the table...")
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Get existing Spark session
spark = SparkSession.getActiveSession()

# Add a new employee
new_employee = [("Sarah Johnson", 33, "Engineering", 130000.0)]
new_df = spark.createDataFrame(new_employee, employee_schema)

# Append to the existing table
new_df.write.format("delta").mode("append").save("/app/data/delta/employees")

# Read the updated version
updated_df = spark.read.format("delta").load("/app/data/delta/employees")
print("Updated table (version 1):")
updated_df.show()

# Read the original version
original_df = spark.read.format("delta").option("versionAsOf", 0).load("/app/data/delta/employees")
print("Original table (version 0):")
original_df.show()

# Display Delta table history
print("Table history:")
spark.sql(f"DESCRIBE HISTORY delta.`/app/data/delta/employees`").show(truncate=False)
