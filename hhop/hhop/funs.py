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
        _analyze_table_location(schema_table=schema_table)
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


def _analyze_table_location(schema_table: str):
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
    mode: str = "overwrite",
    rewrite: bool = False,
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
        mode (str, optional): Mode to write (overwrite or append). Defaults to "overwrite".
        rewrite (bool, optional): If True the existing table will be dropped from Hive and HDFS
        format_files (str, optional): Format of files in HDFS. Only applied on creation of the table.
            Defaults to "parquet".
        partition_cols (Collection, optional): Partitioned columns of the table. Defaults to [].
        verbose (bool, optional): choose if you want to get a message in stdout on write

    Raises:
        HhopException: raised if partition columns are not in the DF
    """
    partition_cols = partition_cols or []

    schema_table = f"{schema}.{table}"

    if rewrite:
        drop_table(schema_table)
        location_if_exists = None
    else:
        location_if_exists = get_table_location(schema_table)

    extra_columns = make_set_lower(partition_cols) - make_set_lower(df.columns)
    if extra_columns:
        raise HhopException(f"{extra_columns} are not in columns of provided DF")

    try:  # select exact columns as existing table
        cols_existing_table = read_table(schema_table).columns
        df = df.select(
            *cols_existing_table
        )  # TODO: decide what to do if columns don't match: drop table, raise error,
        # add a contant to the config of module?
    except Exception as e:  # put partition cols at the end
        # print(e)
        df_cols_part_at_end = [
            column for column in df.columns if column not in partition_cols
        ] + partition_cols
        df = df.select(*df_cols_part_at_end)

    df_save = df.write

    if location_if_exists:
        # it allows to rewrite files if location of to-be-written table is not empty
        df_save = df_save.option("path", location_if_exists)
    else:
        derived_schema = df.schema
        empty_df = spark().createDataFrame([], derived_schema)  # create empty table
        empty_df_save = empty_df.write.format(format_files)
        if partition_cols:
            empty_df_save = empty_df_save.partitionBy(partition_cols)
        empty_df_save.saveAsTable(schema_table)

    df_save.insertInto(schema_table, overwrite=(True if mode == "overwrite" else False))

    if verbose:
        print(f"DF saved as {schema_table}")


def drop_table(
    table_name, drop_hdfs: bool = True, if_exists: bool = True, verbose: bool = False
):
    """This function drops a Hive table and cleans up hdfs folder if it exists

    Args:
        table_name (_type_): _description_
        drop_hdfs (bool, optional): _description_. Defaults to True.
        if_exists (bool, optional): _description_. Defaults to True.
        verbose (bool, optional): _description_. Defaults to False.
    """

    if drop_hdfs:
        table_location = get_table_location(table_name)
        shell_command = f"hdfs dfs -rm -r {table_location}"
        if table_location:
            delete_table_output = subprocess.getoutput(shell_command)
            if verbose:
                print("shell output:", delete_table_output)

    if_exists_str = ""
    if if_exists:
        if_exists_str = "if exists"
    # without if exists it will throw an exception if it really doesn't exist
    spark().sql(f"drop table {if_exists_str} {table_name}")
    if verbose:
        print("table_location:", table_location)
        print(f"shell command: {shell_command}")
        print(f"sql query: drop table {if_exists_str} {table_name}")


def deduplicate_df(df: DataFrame, pk: List[str], order_by_cols: List[col]):
    """Function to deduplicate DF using row_number function so DF will have provided pk as Primary Key
    Attrs:
        df: Spark DF
        pk: list of desired PK columns. Example: ['pk1', 'pk2']
        order_by_cols: list of columns to do order_by. You may use strings, but default order is ascending
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

    Afterthought: Does it really need to be that complex?
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
