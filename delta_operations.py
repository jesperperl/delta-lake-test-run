from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd

def create_delta_table(table_name, data, schema=None, table_path=None):
    """
    Create a new Delta Lake table within a Docker container.

    Parameters:
    -----------
    table_name : str
        Name of the table to create
    data : list, pandas DataFrame, or Spark DataFrame
        Data to insert into the table
    schema : list of tuples or StructType, optional
        Schema definition if input data is a list
    table_path : str, optional
        Path where the Delta table will be stored. If None,
        defaults to /tmp/delta/{table_name}

    Returns:
    --------
    pyspark.sql.DataFrame
        DataFrame representing the created Delta table
    """
    # Set default table path if not provided
    if table_path is None:
        table_path = f"/tmp/delta/{table_name}"

    # Build Spark session with Delta Lake support
    builder = SparkSession.builder \
        .appName(f"DeltaTable-{table_name}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Create and configure the Spark session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Convert input data to Spark DataFrame
    if isinstance(data, list):
        if schema:
            if isinstance(schema, list):
                df = spark.createDataFrame(data, schema)
            else:  # Assuming StructType schema
                df = spark.createDataFrame(data, schema)
        else:
            raise ValueError("Schema must be provided when data is a list")
    elif hasattr(data, "toPandas"):  # Check if it's already a Spark DataFrame
        df = data
    elif isinstance(data, pd.DataFrame):
        df = spark.createDataFrame(data)
    else:
        raise ValueError("Data must be a list, pandas DataFrame, or Spark DataFrame")

    # Write data to Delta Lake format
    df.write.format("delta").mode("overwrite").save(table_path)

    # Register the table in the metastore (optional)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")

    # Read and return the created table
    result_df = spark.read.format("delta").load(table_path)
    print(f"Delta table '{table_name}' created successfully at {table_path}")

    return result_df

def read_delta_table(table_path, version=None):
    """
    Read a Delta Lake table at a specific version.

    Parameters:
    -----------
    table_path : str
        Path to the Delta table
    version : int, optional
        Version of the table to read, if None reads the latest version

    Returns:
    --------
    pyspark.sql.DataFrame
        DataFrame containing the table data
    """
    # Build Spark session with Delta Lake support
    builder = SparkSession.builder \
        .appName("DeltaTableReader") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Create and configure the Spark session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Read the Delta table
    if version is not None:
        df = spark.read.format("delta").option("versionAsOf", version).load(table_path)
    else:
        df = spark.read.format("delta").load(table_path)

    return df

def update_delta_table(table_path, data, schema=None, mode="append"):
    """
    Update an existing Delta Lake table.

    Parameters:
    -----------
    table_path : str
        Path to the Delta table
    data : list, pandas DataFrame, or Spark DataFrame
        Data to insert into the table
    schema : list of tuples or StructType, optional
        Schema definition if input data is a list
    mode : str, optional
        Write mode: 'append', 'overwrite', 'ignore', or 'error'

    Returns:
    --------
    pyspark.sql.DataFrame
        DataFrame of the updated table
    """
    # Build Spark session with Delta Lake support
    builder = SparkSession.builder \
        .appName("DeltaTableUpdater") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Create and configure the Spark session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Convert input data to Spark DataFrame
    if isinstance(data, list):
        if schema:
            if isinstance(schema, list):
                df = spark.createDataFrame(data, schema)
            else:  # Assuming StructType schema
                df = spark.createDataFrame(data, schema)
        else:
            raise ValueError("Schema must be provided when data is a list")
    elif hasattr(data, "toPandas"):  # Check if it's already a Spark DataFrame
        df = data
    elif isinstance(data, pd.DataFrame):
        df = spark.createDataFrame(data)
    else:
        raise ValueError("Data must be a list, pandas DataFrame, or Spark DataFrame")

    # Write data to Delta Lake format
    df.write.format("delta").mode(mode).save(table_path)

    # Read and return the updated table
    result_df = spark.read.format("delta").load(table_path)
    print(f"Delta table at {table_path} updated successfully with mode '{mode}'")

    return result_df
