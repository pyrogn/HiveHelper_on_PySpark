from functools import reduce
import subprocess
from spark_init import pyspark, spark, sc, col
from pyspark.sql import DataFrame
from typing import List, Set, Tuple
from exceptions import HhopException

DEFAULT_SCHEMA_WRITE = "default"


def read_table(
    schema_table: str,
    columns: List[str] = "all",
    verbose: bool = False,
    alias: str = None,
    cnt_files: bool = False,
) -> DataFrame:
    """Function for fast reading a table from Hive

    Args:
        schema_table (str): Full name of Hive table. Example: 'default.my_table'
        columns (List  |  Set  |  Tuple, optional): List of columns to select from Hive table.
            Defaults to "all".
        verbose (bool, optional): Check to get .printSchema() of the table.
            Defaults to False.
        alias (str, optional): Alias of the DF to use. Defaults to None.
        cnt_files (bool, optional): Check to get number of parquet files in the location.
            Only possible to get with attr: verbose=True. Defaults to False.

    Returns:
        DataFrame: PySpark DataFrame from Hive
    """
    df = spark.sql(f"select * from {schema_table}")

    if columns != "all":
        df = df.select(columns)

    if alias:
        df = df.alias(alias)

    if verbose:
        # all columns of the table
        df.printSchema()

        # partition columns
        schema_name, table_name = schema_table.split(".")
        cols = spark.catalog.listColumns(tableName=table_name, dbName=schema_name)
        _part_cols = [col.name for col in cols if col.isPartition == True]
        if _part_cols:
            print(f"partition columns: {_part_cols}")
        else:
            print("there are no partition columns")

        # table_location + count parquet files
    if cnt_files:
        __analyze_table_location(schema_table=schema_table)
    return df


def __analyze_table_location(schema_table: str):
    """
    Function finds a table location and counts number of parquet files

    Args:
        schema_table (str): Name of the table. Example: 'default.my_table'
    """
    describe_table = spark.sql(f"describe formatted {schema_table}")
    table_location = (
        describe_table.filter(col("col_name") == "Location")
        .select("data_type")
        .rdd.flatMap(lambda x: x)
        .collect()[0]
    )

    shell_command = f"hdfs dfs -ls -R {table_location} | grep '.parquet' | wc -l"
    cnt_files_raw = subprocess.getoutput(shell_command)
    print(f"Running command: {shell_command}")

    try:
        cnt_files = int(cnt_files_raw.split("\n")[-1].strip())
        print(f"{cnt_files} parquet files in the specified above location")

    except Exception as e:
        print("Error in count files. Check command output:")
        print(cnt_files_raw)
        print(e)


def union_all(*dfs) -> DataFrame:
    """
    Shortcut function to union many tables

    Example: union_all(df1, df2, df3, df4)

    Returns:
        DataFrame: unioned DataFrame
    """
    return reduce(DataFrame.unionByName, dfs)


def write_table(
    df: DataFrame,
    table: str,
    schema: str = DEFAULT_SCHEMA_WRITE,
    mode: str = "overwrite",
    format_files: str = "parquet",
    partition_cols: List[str] = [],
) -> DataFrame:
    """
    This function saves a DF to Hive using common default values

    Args:
        df (DataFrame): DataFrame to write to Hive
        table (str): Name of the table (without schema)
        schema (str, optional): Name of the schema. Defaults to DEFAULT_SCHEMA_WRITE in this file.
        mode (str, optional): Mode to write (overwrite or append). Defaults to "overwrite".
        format_files (str, optional): Format of files in HDFS. Defaults to "parquet".
        partition_cols (List  |  Set  |  Tuple, optional): Partitioned columns of the table. Defaults to [].

    Raises:
        HhopException: raised if partition columns are not in the DF
    """
    df_save = df.write.mode(mode).format(format_files)

    if set(partition_cols) - set(df.columns):
        raise HhopException(
            f"{set(partition_cols) - set(df.columns)} are not in columns of provided DF"
        )

    if partition_cols:
        df_save = df_save.partitionBy(partition_cols)
    df_save.saveAsTable(f"{schema}.{table}")
    print(f"DF saved as {schema}.{table}")
