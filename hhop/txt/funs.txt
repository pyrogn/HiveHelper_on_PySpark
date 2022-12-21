from functools import reduce
import subprocess
from spark_init import pyspark, spark, sc, col
from pyspark.sql import DataFrame
from typing import List, Set, Tuple


def read_table(
    schema_table: str,
    columns: List[str] = "all",
    verbose: bool = False,
    alias: str = None,
    cnt_files: bool = False,
) -> DataFrame:
    """Function for fast reading a table from Hive

    Args:
        schema_table (str): _description_
        columns (List  |  Set  |  Tuple, optional): _description_. Defaults to "all".
        verbose (bool, optional): _description_. Defaults to False.
        alias (str, optional): _description_. Defaults to None.
        cnt_files (bool, optional): _description_. Defaults to False.

    Returns:
        DataFrame: _description_
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
        __analyze_table_location(
            schema_table=schema_table, is_partitioned=len(_part_cols) > 0
        )
    return df


def __analyze_table_location(schema_table, is_partitioned):
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


def union_all(*dfs):
    return reduce(DataFrame.unionByName, dfs)


def write_table(
    df: DataFrame,
    table: str,
    schema: str = "default",
    mode: str = "overwrite",
    format_files: str = "parquet",
    partition_cols: List[str] = [],
) -> DataFrame:
    """This function saves a DF to Hive using common default values

    Args:
        df (DataFrame): _description_
        table (str): _description_
        schema (str, optional): _description_. Defaults to "default".
        mode (str, optional): _description_. Defaults to "overwrite".
        format_files (str, optional): _description_. Defaults to "parquet".
        partition_cols (List  |  Set  |  Tuple, optional): _description_. Defaults to [].

    Raises:
        Exception: _description_
    """
    df_save = df.write.mode(mode).format(format_files)

    if set(partition_cols) - set(df.columns):
        raise Exception(
            f"{set(partition_cols) - set(df.columns)} are not in columns of provided DF"
        )

    if partition_cols:
        df_save = df_save.partitionBy(partition_cols)
    df_save.saveAsTable(f"{schema}.{table}")
    print(f"DF saved as {schema}.{table}")
