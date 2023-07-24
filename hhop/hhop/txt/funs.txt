"useful functions for Pyspark"
from functools import reduce
import subprocess
import inspect
from typing import List, Set, Tuple
import re


from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql.utils import AnalysisException

from .exceptions import HhopException, DiffColsException
from .spark_init import get_spark_builder

spark = lambda: get_spark_builder().getOrCreate()

DEFAULT_SCHEMA_WRITE = "default"
DROP_TABLE_ON_WRITE_IF_DIFF_DDL = True


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
        rewrite (bool, optional): If True the existing table will be dropped from Hive and HDFS.
            No exceptions are raised if it doesn't exist
        format_files (str, optional): Format of files in HDFS. Only applied on creation of the table.
            Defaults to "parquet".
        partition_cols (Collection, optional): Partitioned columns of the table. Defaults to [].
        verbose (bool, optional): choose if you want to get a message in stdout on write

    Raises:
        HhopException: raised if partition columns are not in the DF
    """
    schema_table = f"{schema}.{table}"
    partition_cols = partition_cols or []
    current_cols = DFColCleaner(df, partition_cols=partition_cols)

    create_table = True
    if rewrite:
        drop_table(schema_table)

    try:  # is table already exists
        existing_df = read_table(schema_table)
        try:  # is existing table has the same columns
            existing_cols = DFColCleaner(existing_df)
            comp_cols = DFColValidator(current_cols, existing_cols)
            if not comp_cols.is_equal_cols_groups(["all"]):
                if DROP_TABLE_ON_WRITE_IF_DIFF_DDL:
                    drop_table(schema_table)
                else:
                    raise DiffColsException(
                        "columns with existing DF and new DF are different"
                        " and force rewrite flag on diff DDL is set to False"
                        f" diff is {comp_cols.get_xor_groups(['all'])}"
                        f" {current_cols.get_columns_from_groups(['all'])}"
                        f" {existing_cols.get_columns_from_groups(['all'])}"
                    )
            else:
                df = df.select(*existing_df.columns)
                create_table = False
        except DiffColsException:
            raise
    except AnalysisException:
        df_cols_part_at_end = [
            column for column in df.columns if column not in partition_cols
        ] + partition_cols
        df = df.select(*df_cols_part_at_end)

    df_save = df.write

    if create_table:
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
    table_name: str,
    drop_hdfs: bool = True,
    if_exists: bool = True,
    verbose: bool = False,
):
    """This function drops a Hive table and cleans up hdfs folder if it exists

    Args:
        table_name (str, required): name of Hive table÷÷÷
        drop_hdfs (bool, optional): To find and drop hdfs files. Defaults to True.
        if_exists (bool, optional): If false it will raise an Exception if table doesn't exists.
            Defaults to True.
        verbose (bool, optional): Enable to print logs. Defaults to False.
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
    Agrs:
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


class DFColCleaner:
    """
    Helps with comparing and raising exceptions on columns of DFs
    I still do not like design of this class, it could be better
    """

    def __init__(self, df, **group_cols: List[str]):
        """Provide DF and groups of columns

        Args:
            df (DataFrame): DataFrame
            **group_cols (List[str]): groups with lists of columns

        Raises:
            DiffColsException: Raised there is an error in columns
        """
        self.df = df
        group_cols = {key: value or [] for key, value in group_cols.items()}
        df_columns = self.lower_list(self.df.columns)
        # will throw an exception on duplicated groups
        _ = self.lower_list(group_cols.keys(), type_error="column groups")

        all_columns_set = set(df_columns)  # for N(1) check of entry
        not_in_all_columns = set()  # raise error if not empty
        group_cols_clean = {}  # new dict with lower columns and groups
        for group, cols in group_cols.items():
            group = group.lower()
            columns_in_group = []
            for column in cols:
                column = column.lower()
                if column not in all_columns_set:
                    not_in_all_columns.add(column)
                columns_in_group.append(column)
            group_cols_clean[group] = self.lower_list(columns_in_group)
        if not_in_all_columns:
            raise DiffColsException(
                f"Columns {not_in_all_columns} not in the DF: {df_columns}"
            )

        # this adds groups: all and extra
        group_cols_clean = {
            str.lower(group): [str.lower(column) for column in cols]
            for group, cols in group_cols_clean.items()
        }  # to lower columns in groups
        group_cols_clean["all"] = df_columns
        group_cols_clean["extra"] = list(
            set(df_columns)
            - set(
                [
                    c
                    for cg in group_cols_clean
                    for c in group_cols_clean[cg]
                    if cg != "all"
                ]
            )
        )
        self.group_cols = group_cols_clean

    @staticmethod
    def lower_list(cols: List, type_error="columns") -> List:
        lower_list = [str.lower(elem) for elem in cols]

        seen = set()
        dupes = [x for x in lower_list if x in seen or seen.add(x)]
        if dupes:
            raise DiffColsException(f"Found duplicates {type_error}: {dupes}")

        return lower_list

    @classmethod
    def get_columns_with_suffix(cls, df, suffix):
        "selects columns with suffix on DF"
        df_cols = cls.lower_list(df.columns)
        cols_with_suffix = []
        for column in df_cols:
            if re.match(rf"[\w_\d]+{suffix}$", column):
                cols_with_suffix.append(column)
        return cols_with_suffix

    def mass_rename(
        self, suffix, is_append_suffix, group_cols_include=None, group_cols_exclude=None
    ):
        cols_to_rename = self.get_columns_from_groups(
            group_cols_include, group_cols_exclude
        )
        dict_rename = {}
        if is_append_suffix:
            new_colname = lambda x: x + suffix
        else:
            new_colname = lambda x: re.sub(rf"{suffix}$", "", x)
        for column in cols_to_rename:
            dict_rename[column] = new_colname(column)

        df = self.rename_to(self.df, dict_rename)
        # TODO: change self.group_cols accordingly
        return df

    @staticmethod
    def rename_to(df, old_new_mapping: dict, backwards=False):
        if backwards:
            old_new_mapping = {v: k for k, v in old_new_mapping.items()}
        for old, new in old_new_mapping.items():
            if old != new:
                df = df.withColumnRenamed(old, new)
        return df

    def get_columns_from_groups(
        self, group_cols_include=None, group_cols_exclude=None
    ) -> List:
        group_cols_include = self.lower_list(
            group_cols_include or ["all"], type_error="column groups"
        )  # all is default
        group_cols_exclude = self.lower_list(
            group_cols_exclude or [], type_error="column groups"
        )
        # suboptimal solution, but order of columns is saved
        cols_exclude = set()
        for gc_exclude in group_cols_exclude:
            for column in self.group_cols[gc_exclude]:
                cols_exclude.add(column)
        cols_out = []
        for gc_include in group_cols_include:
            for column in self.group_cols[gc_include]:
                if column not in cols_out and column not in cols_exclude:
                    cols_out.append(column)
        return cols_out

    def is_cols_subset(
        self, cols, group_cols_include=None, group_cols_exclude=None
    ) -> bool:
        cols_in_groups = self.get_columns_from_groups(
            group_cols_include, group_cols_exclude
        )
        cols_left = set(cols) - cols_in_groups
        return len(cols_left) == 0


class DFColValidator:
    "To validate 2 dataframes from class DFColCleaner"

    def __init__(self, obj1, obj2) -> None:
        self.obj1 = obj1
        self.obj2 = obj2

    def get_cols_on_groups(self, group_cols_include=None, group_cols_exclude=None):
        cols1 = set(
            self.obj1.get_columns_from_groups(group_cols_include, group_cols_exclude)
        )
        cols2 = set(
            self.obj2.get_columns_from_groups(group_cols_include, group_cols_exclude)
        )
        return cols1, cols2

    def _compare_groups(
        self, operator, group_cols_include=None, group_cols_exclude=None
    ):
        cols1, cols2 = self.get_cols_on_groups(group_cols_include, group_cols_exclude)
        return operator(cols1, cols2)

    # this looks like something can be done to simplify methods
    def get_intersection_groups(
        self, group_cols_include=None, group_cols_exclude=None
    ) -> Set:
        return self._compare_groups(
            set.intersection,
            group_cols_include=group_cols_include,
            group_cols_exclude=group_cols_exclude,
        )

    def get_union_groups(self, group_cols_include=None, group_cols_exclude=None):
        return self._compare_groups(
            set.union,
            group_cols_include=group_cols_include,
            group_cols_exclude=group_cols_exclude,
        )

    def get_xor_groups(self, group_cols_include=None, group_cols_exclude=None):
        return self._compare_groups(
            set.__xor__,
            group_cols_include=group_cols_include,
            group_cols_exclude=group_cols_exclude,
        )

    def is_equal_cols_groups(
        self, group_cols_include=None, group_cols_exclude=None, raise_exception=False
    ):
        is_equal = len(self.get_xor_groups(group_cols_include, group_cols_exclude)) == 0
        if raise_exception and not is_equal:
            raise DiffColsException(
                f"Groups in 2 DF are not equal. Groups: {group_cols_include} minus {group_cols_exclude}"
                "Use methods get_TYPE_groups or get_cols_on_groups"
            )
        return is_equal
