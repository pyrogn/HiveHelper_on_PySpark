"useful functions for Pyspark"
from functools import reduce
import subprocess
import inspect
from typing import List, Set, Tuple


from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W

from .exceptions import HhopException
from .spark_init import get_spark_builder

spark = lambda: get_spark_builder().getOrCreate()

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
            Defaults to False.

    Returns:
        DataFrame: PySpark DataFrame from Hive
    """
    df = spark().sql(f"select * from {schema_table}")

    if columns != "all":
        df = df.select(columns)

    if alias:
        df = df.alias(alias)

    if verbose:
        # all columns of the table
        df.printSchema()

        # partition columns
        schema_name, table_name = schema_table.split(".")
        cols = spark().catalog.listColumns(tableName=table_name, dbName=schema_name)
        _part_cols = [col.name for col in cols if col.isPartition is True]
        if _part_cols:
            print(f"partition columns: {_part_cols}")
        else:
            print("there are no partition columns")

    # table_location + count parquet files
    if cnt_files:
        __analyze_table_location(schema_table=schema_table)
    return df


def get_table_location(schema_table: str):
    """You pass name of a Hive table
    Funtction returns HDFS address of a table if it exists"""

    try:
        describe_table = spark().sql(f"describe formatted {schema_table}")

        table_location = (
            describe_table.filter(col("col_name") == "Location")
            .select("data_type")
            .rdd.flatMap(lambda x: x)
            .collect()[0]
        )
        return table_location

    except Exception:
        return None


def __analyze_table_location(schema_table: str):
    """
    Function finds a table location and counts number of parquet files
    Args:
        schema_table (str): Name of the table. Example: 'default.my_table'
    """

    table_location = get_table_location(schema_table)

    if table_location:
        shell_command = f"hdfs dfs -ls -R {table_location} | grep '.parquet' | wc -l"
        print(f"Running command: {shell_command}")
        cnt_files_raw = subprocess.getoutput(shell_command)

        try:
            cnt_files = int(cnt_files_raw.split("\n")[-1].strip())
            print(f"{cnt_files} parquet files in the specified above location")

        except Exception as e:
            print("Error in count files. Check command output:")
            print(e)
            print(cnt_files_raw)

    else:
        print(f"table {schema_table} is not found")


def union_all(*dfs) -> DataFrame:
    """
    Shortcut function to union many tables

    Example: union_all(df1, df2, df3, df4)

    Returns:
        DataFrame: unioned DataFrame
    """
    return reduce(DataFrame.unionByName, dfs)


def make_set_lower(iterable):
    if iterable is None:
        iterable = {}
    return {i.lower() for i in iterable}


def write_table(
    df: DataFrame,
    table: str,
    schema: str = DEFAULT_SCHEMA_WRITE,
    type_write: str = "save",
    mode: str = "overwrite",
    format_files: str = "parquet",
    partition_cols: List[str] = None,
    verbose: bool = True,
) -> DataFrame:
    """
    This function saves a DF to Hive using common default values

    Exception: If you get error that HDFS location already exists, then try to remove files using:
    hdfs dfs -rm -f -r {hdfs location in the error}

    Args:
        df (DataFrame): DataFrame to write to Hive
        table (str): Name of the table (without schema)
        schema (str, optional): Name of the schema. Defaults to DEFAULT_SCHEMA_WRITE in this file.
        type_write (str, optional): 'save' for saveAsTable, 'insert' for InsertIntoTable
            If you want to replace dynamic partitions do not forget the next param
            ("spark.sql.sources.partitionOverwriteMode","dynamic")
        mode (str, optional): Mode to write (overwrite or append). Defaults to "overwrite".
        format_files (str, optional): Format of files in HDFS. Defaults to "parquet".
        partition_cols (List  |  Set  |  Tuple, optional): Partitioned columns of the table. Defaults to [].

    Raises:
        HhopException: raised if partition columns are not in the DF
    """

    location_if_exists = get_table_location(f"{schema}.{table}")

    df_save = df.write

    if location_if_exists:
        # it allows to rewrite files if location of to-be-written table is not empty
        df_save = df_save.option("path", location_if_exists)

    df_save = df_save.mode(mode).format(format_files)

    extra_columns = make_set_lower(partition_cols) - make_set_lower(df.columns)
    if extra_columns:
        raise HhopException(f"{extra_columns} are not in columns of provided DF")

    if partition_cols:
        df_save = df_save.partitionBy(partition_cols)

    schema_table = f"{schema}.{table}"
    if type_write == "save":
        df_save.saveAsTable(schema_table)
    elif type_write == "insert":
        df_save.insertInto(schema_table)
    else:
        raise HhopException("Use save (.saveAsTable) or insert (insertInto) keywords")

    if verbose:
        print(f"DF saved as {schema}.{table}")


def deduplicate_df(df: DataFrame, pk: List[str], order_by_cols: List[col]):
    """Function to deduplicate DF using row_number function
    Attrs:
        df: Spark DF
        pk: list of future PK columns. Example: ['pk1', 'pk2']
        order_by_cols: list of columns to do order_by.
            Example: [col('val1'), col('val2').desc()]
    """
    window_rn = W.partitionBy(pk).orderBy(order_by_cols)
    df_out = (
        df.withColumn("_rn", F.row_number().over(window_rn))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    return df_out


def write_read_table(df_write: DataFrame, *write_args, **write_kwargs) -> DataFrame:
    """Function for making checkpoints by applying
    1. write_table with provided write_args and write_kwargs
    2. read_table with default values
    """
    write_table(df_write, *write_args, **write_kwargs)

    func = write_table
    num_required_args = len(inspect.getfullargspec(func).args) - len(func.__defaults__)
    default_values = dict(
        zip(func.__code__.co_varnames[num_required_args:], func.__defaults__)
    )

    default_values.update(write_kwargs)
    write_kwargs = default_values
    write_kwargs.update(
        zip(write_table.__code__.co_varnames[1:], write_args)
    )  # first element is DataFrame

    schema_table = f"{write_kwargs['schema']}.{write_kwargs['table']}"
    df = read_table(schema_table)

    return df


def safely_write_table():
    """function is going to
    1. write DF to a temp location
    2. remove table in the target location (files are going to trash)
    3. move files from a temp location to the target location"""
    pass
