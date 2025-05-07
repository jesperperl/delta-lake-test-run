# from delta_operations import create_delta_table
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import argparse
import os

DELTA_PATH = "/app/data/delta/employees"

def execute_sql_query(spark, delta_path, sql_query):
    """Execute SQL query and display results"""
    # Register table as temporary view
    table_name = "employees"
    spark.read.format("delta").load(delta_path).createOrReplaceTempView(table_name)

    # Execute query
    print(f"\nExecuting query: {sql_query}")
    result = spark.sql(sql_query)

    # Show results
    print("Results:")
    result.show(truncate=False)

    return result

def query_delta_table(delta_path, sql=None, output_format="display"):
    """
    Query a Delta Lake table using SQL.

    Parameters:
    -----------
    delta_path : str
        Path to the Delta Lake table
    sql : str, optional
        SQL query to execute (uses SELECT * if not provided)
    output_format : str, optional
        Output format: 'display', 'csv', or 'json'

    Returns:
    --------
    None
    """
    # Initialize Spark with Delta Lake
    builder = SparkSession.builder \
        .appName("DeltaQuery") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Create Spark session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    try:
        # Check if Delta path exists
        if not os.path.exists(delta_path):
            print(f"Error: Delta table path '{delta_path}' does not exist.")
            return

        # Load the Delta table
        delta_df = spark.read.format("delta").load(delta_path)

        # Register as temporary view for SQL
        table_name = "delta_table"
        delta_df.createOrReplaceTempView(table_name)

        # Create default SQL if not provided
        if not sql:
            sql = f"SELECT * FROM {table_name}"

        # Execute SQL query
        print(f"Executing SQL: {sql}")
        result = spark.sql(sql)

        # Output results based on format
        if output_format == "display":
            print("\nQuery Results:")
            result.show(truncate=False)
        elif output_format == "csv":
            output_path = "/app/data/output.csv"
            result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
            print(f"Results written to CSV: {output_path}")
        elif output_format == "json":
            output_path = "/app/data/output.json"
            result.coalesce(1).write.mode("overwrite").json(output_path)
            print(f"Results written to JSON: {output_path}")
        else:
            print("Invalid output format. Using display mode.")
            result.show(truncate=False)

        # Show table information
        print("\nTable Schema:")
        delta_df.printSchema()

        # Use describe to get statistics
        print("\nStatistics:")
        delta_df.describe().show()

    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        # Stop Spark session
        spark.stop()
        print("Query completed.")


def main():
    # Initialize Spark with Delta Lake support
    builder = SparkSession.builder \
        .appName("DeltaLakeSQLExample") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Configure and create the Spark session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    try:
        # Create sample Delta table
        # create_sample_delta_table(spark, DELTA_PATH)

        # Example 1: Simple SELECT
        query1 = "SELECT * FROM employees"
        execute_sql_query(spark, DELTA_PATH, query1)

        # Example 2: Aggregation query
        query2 = """
            SELECT department,
                   COUNT(*) as num_employees,
                   SUM(salary) as total_salary
            FROM employees
            GROUP BY department
            ORDER BY total_salary DESC
        """
        execute_sql_query(spark, DELTA_PATH, query2)

        # # Example 3: Filtering and ordering
        # query3 = """
        #     SELECT date, product, price, quantity, (price * quantity) as total
        #     FROM sales
        #     WHERE category = 'Electronics'
        #     ORDER BY total DESC
        # """
        # execute_sql_query(spark, DELTA_PATH, query3)

        # # Example 4: Using SQL functions
        # query4 = """
        #     SELECT
        #         SUBSTRING(date, 1, 7) as month,
        #         COUNT(*) as order_count,
        #         ROUND(AVG(price), 2) as avg_price,
        #         SUM(quantity) as total_items_sold
        #     FROM sales
        #     GROUP BY SUBSTRING(date, 1, 7)
        # """
        # execute_sql_query(spark, DELTA_PATH, query4)

        # # Example 5: Window functions
        # query5 = """
        #     SELECT
        #         category,
        #         product,
        #         price,
        #         RANK() OVER (PARTITION BY category ORDER BY price DESC) as price_rank
        #     FROM sales
        # """
        # execute_sql_query(spark, DELTA_PATH, query5)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Stop the Spark session
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()