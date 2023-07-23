"main module with helpers"
from functools import reduce
from operator import add
from collections import namedtuple
from typing import List
import os

import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql.types import NumericType

from .funs import (
    read_table,
    deduplicate_df,
    union_all,
    write_read_table,
    DEFAULT_SCHEMA_WRITE,
    DFColCleaner,
    DFColValidator,
)

from .exceptions import HhopException
from .spark_init import get_spark_builder

# get spark app by calling a builder, not sure if it is the best way to get spark app
spark = lambda: get_spark_builder().getOrCreate()

# to make output smaller if dict of errors is too long
# set higher if you need longer dictionary to pring
DICT_PRINT_MAX_LEN = 15
# fraction digits to round in method compare_tables()
SCALE_OF_NUMBER_IN_COMPARING = 2


PkStatsGenerator = namedtuple(
    "PK_stats", "cnt_rows unique_pk_cnt pk_with_duplicates_pk"
)
CompareTablesPkStatsGenerator = namedtuple(
    "Compare_tables_pk_stats",
    "not_in_main_table_cnt not_in_ref_table correct_matching_cnt",
)


class DFExtender(pyspark.sql.dataframe.DataFrame):
    """
    Class functions allow:
        1. Getting info about PK of one DF
        2. Getting info about NULL columns of one DF
        3. Comparing two tables based on PK
    Every time you are given a DF with errors to analyze by yourself

    Methods:
        get_info - information about the DF (PK + NULL info)
        get_df_with_null - get a DF with NULLs in provided columns
        compare_tables - compare two tables based on PK
    """

    def __init__(
        self,
        df: DataFrame,
        pk: List[str] = None,
        verbose: bool = False,
        silent_mode: bool = False,
        custom_null_values: List[str] = ["", "NULL", "null", "Null"],
    ) -> DataFrame:
        """Initialization

        Args:
            df (pyspark.sql.dataframe.DataFrame): DataFrame to use for analysis
            pk ((list, tuple, set), optional): Primary Key of the DF. Defaults to None.
            verbose (bool, optional): Choose if you want to receive additional messages.
                Defaults to False.
            custom_null_values (List[str]) - provide values that will be considered as NULLs in NULLs check
            silent_mode (bool): if true, doesn't print anything. Defaults to False
        Return:
            DataFrame as provided in call
        """
        # usual print if not silence_mode
        self._s_print = (
            lambda *args, **kwargs: print(*args, **kwargs) if not silent_mode else None
        )
        # print if verbose = True and not silence_mode
        self._v_print = (
            lambda *args, **kwargs: print(*args, **kwargs)
            if not silent_mode and verbose
            else None
        )

        self._pk = pk
        self._df = df
        self._verbose = verbose
        self._custom_null_values = custom_null_values

        self.dict_null_in_cols = None
        self.pk_stats = None
        self.df_duplicates_pk = None
        self.df_with_nulls = None

        self.matching_results = None
        self.dict_cols_with_errors = None
        self.df_with_errors = None
        self.columns_diff_reordered_all = None

        super().__init__(
            self._df._jdf,
            self._df.sql_ctx,  # change sql_ctx to sparkSession in spark>=3.4
        )  # magic to integrate pyspark DF methods into this class

        # get sorted dict with count + share without zero values
        self._get_sorted_dict = lambda dict, val: {
            k: [v, round(v / val, 4)]
            for k, v in sorted(dict.items(), key=lambda item: -item[1])
            if v > 0
        }

        self._print_stats = lambda string, val: self._s_print(
            "{:<25} {:,}".format(string + ":", val)
        )

        self._cond_col_is_null = lambda c: col(c).isNull() | col(c).isin(
            self._custom_null_values
        )

        self._sanity_checks()

    def __print_dict(self, dictionary: dict, attr_name: str, *args, **kwargs):
        """Prevent printing a dictionary longer than DICT_PRINT_MAX_LEN

        Args:
            dictionary (dict): dictionary to print and check its length
            attr_name (str): prints attr_name if dictionary is too big
        """
        if len(dictionary) <= DICT_PRINT_MAX_LEN:
            self._s_print(dictionary, *args, **kwargs)
        else:
            self._s_print(
                f"dictionary is too large ({len(dictionary)} > {DICT_PRINT_MAX_LEN})"
            )
            self._s_print(f"You can access the dictionary in the attribute {attr_name}")

    def _sanity_checks(self):
        """Sanity checks for provided DataFrame
        1. Check if PK columns are in DF
        2. Check if DF is not empty

        Raises:
            Exception: Provide only columns to pk that are present in the DF
        """

        if self._pk:
            # raise an exception if partition_cols do not exist in DF
            _ = DFColCleaner(self._df, pk=self._pk)
        if len(self._df.head(1)) == 0:
            raise HhopException("DF is empty")

    def get_info(
        self,
        pk_stats=True,
        null_stats=True,
    ):
        """Methods returns statistics about DF.

        1. If PK is provided there will be statistics on PK duplicates
        2. Statistics about NULL values in columns

        Params:
            pk_stats: if True, calculate stats on Primary Key
            null_stats: if True, calculate stats on NULL values in a table

        Attrs:
            dict_null_in_cols
            df_duplicates_pk (optional) (from method _analyze_pk)
            df_with_nulls (optional) (from method get_df_with_null)
            pk_stats (optional) - namedtuple with stats on PK
        """
        cnt_all = None
        if pk_stats and self._pk:
            self._analyze_pk()
            self._print_pk_stats()

            cnt_all = self.pk_stats[0]

        if cnt_all is None and null_stats:
            cnt_all = self._df.count()

        if null_stats:
            dict_null = (
                self._df.select(
                    [
                        F.count(F.when(self._cond_col_is_null(c), c)).alias(c)
                        for c in self._df.columns
                    ]
                )
                .rdd.collect()[0]
                .asDict()
            )
            self.dict_null_in_cols = self._get_sorted_dict(dict_null, cnt_all)

            self._s_print(
                "\nNull values in columns - {'column': [count NULL, share NULL]}:"
            )
            self.__print_dict(self.dict_null_in_cols, "dict_null_in_cols")

            self._v_print(
                "Use method `.get_df_with_null(List[str])` to get a df with specified NULL columns"
            )

            if self._pk and pk_stats and self._verbose:
                for key in self._pk:
                    if key in self.dict_null_in_cols:
                        self._v_print(
                            f"PK column '{key}' contains empty values, be careful!"
                        )

    def _analyze_pk(self):
        """
        Method analizes DF based on provided PK
        Computed attrs:
            pk_stats - [Count all, Unique PK count, PK with duplicates]
            df_duplicates_pk (optional) - DF with PK duplicates if there are any
        """
        if self._pk:
            df_temp = (
                self._df.groupBy(self._pk)
                .agg(F.count(F.lit(1)).alias("cnt_pk"))
                .groupBy("cnt_pk")
                .agg(F.count(F.lit(1)).alias("cnt_of_counts"))
                .cache()
            )
            cnt_all = (
                df_temp.withColumn("cnt_restored", col("cnt_pk") * col("cnt_of_counts"))
                .agg(F.sum("cnt_restored").alias("cnt_all"))
                .collect()[0]["cnt_all"]
            ) or 0

            cnt_unique_pk = cnt_with_duplicates_pk = 0

            cnt_unique_pk = (
                df_temp.agg(F.sum("cnt_of_counts").alias("unique_pk")).collect()[0][
                    "unique_pk"
                ]
            ) or 0

            cnt_with_duplicates_pk = (
                df_temp.filter(col("cnt_pk") > 1)
                .agg(F.sum("cnt_of_counts").alias("pk_with_duplicates"))
                .collect()[0]["pk_with_duplicates"]
            ) or 0

            if cnt_with_duplicates_pk:
                window_duplicates_pk = W.partitionBy(self._pk)
                self.df_duplicates_pk = (
                    self._df.withColumn(
                        "cnt_pk", F.count(F.lit(1)).over(window_duplicates_pk)
                    )
                    .filter(col("cnt_pk") > 1)
                    .orderBy([col("cnt_pk").desc(), *[col(i) for i in self._pk]])
                )

                self._v_print(
                    "You can access DF with PK duplicates in an attribute `.df_duplicates_pk`\n"
                )

            # 0 - cnt rows, 1 - Unique PK, 2 - PK with duplicates
            self.pk_stats = PkStatsGenerator(
                *[cnt_all, cnt_unique_pk, cnt_with_duplicates_pk]
            )
        else:
            HhopException("PK hasn't been provided!\n")

    def _print_pk_stats(self):
        """Method only prints stats"""
        self._print_stats("Count all", self.pk_stats[0])
        if self._pk:
            self._print_stats("Unique PK count", self.pk_stats[1])
            self._print_stats("PK with duplicates", self.pk_stats[2])

    def get_df_with_null(self, null_columns: List[str] = None):
        """This method calculates and returns DF with selected cols that have NULL values

        Args:
            null_columns (list, optional): Columns as list of string. Defaults to [].

        Raises:
            HhopException: Provide only columns to null_columns that are present in the DF

        Returns:
            pyspark.sql.dataframe.DataFrame:
                Returns a DF sorted by count of nulls in selected columns
                in descending order
        """
        null_columns = null_columns or []
        if not hasattr(self, "dict_null_in_cols"):
            self._s_print("Running method .get_info() first", end="\n")
            self.get_info(pk_stats=False)

        # raise an exception if partition_cols do not exist in DF
        _ = DFColCleaner(self._df, null_columns=null_columns)

        if self.dict_null_in_cols:
            if set(null_columns) & set(self.dict_null_in_cols):
                cols_filter = null_columns
            else:
                self._s_print(
                    f"No NULL values found in provided {null_columns}, using all: {self.dict_null_in_cols.keys()}"
                )
                cols_filter = self.dict_null_in_cols.keys()

            self.df_with_nulls = (
                self._df.withColumn(
                    "cnt_nulls",
                    sum(self._cond_col_is_null(col).cast("int") for col in cols_filter),
                )
                .filter(col("cnt_nulls") > 0)
                .orderBy(col("cnt_nulls").desc())
            )
            return self.df_with_nulls
        self._s_print("no NULL values in selected or all null columns")

    def compare_tables(self, df_ref: DataFrame):
        """
        Comparing two tables based on `pk` attribute
        It has two parts:
            1. Comparing and printing errors in non PK columns
            2. Comparing errors in PK attributes
        Every time you are given a DF with errors to analyze by yourself

        Args:
            df_ref (DataFrame): second DF (reference DF)

        Primary Key is inherited from calling a DFExtender

        Raises:
            Exception: Provide Primary Key for tables

        Attrs:
            dict_cols_with_errors - dictionary with count of errors in non PK attributes
            matching_stats - namedtuple with stats on matching by pk columns
            dict_cols_with_errors - dict with aggregated errors by non-pk columns
                All numeric columns in DFs get rounded with scale specified in
                    SCALE_OF_NUMBER_IN_COMPARING
            dfs_extracols - dict with extra columns in 2 dfs. Keys: 'df1', 'df2'. Values are set.
            df_with_errors - df with errors.
                Added attributes:
                    is_joined_main - 1 if this PK in main DF, NULL otherwise
                    is_joined_ref - 1 if this PK in reference DF, NULL otherwise
                    is_diff_[column name] - 1 if column differs between Main and Ref DF
                        otherwise 0
                    sum_errors - sum of all errors in a row. Doesn't exist
                        if there are no errors in non PK attributes.
                It is not cached, write it to Hive or cache it with filter, select!
        """
        if not self._pk:
            raise HhopException("No PK is provided")
        if self._df is df_ref:
            raise HhopException("Two DFs are the same objects, create a new one")

        self._df_ref = df_ref

        for df, name in zip((self._df, self._df_ref), ("Main DF", "Reference DF")):
            if not hasattr(df, "pk_stats"):
                df = DFExtender(df, self._pk, verbose=False)
                df._analyze_pk()
            self._s_print(name)
            df._print_pk_stats()
            self._s_print()

        # rounding in any numeric columns so diffs don't considered as errors because of machine rounding
        self._df, self._df_ref = map(
            self.__round_numberic_cols_df, [self._df, self._df_ref]
        )
        # might want to create a class to compare and find columns of 2 DF,
        # because this functionality is applied in the script at least 3 times
        df_c1 = DFColCleaner(self._df, pk=self._pk)
        df_c2 = DFColCleaner(self._df_ref, pk=self._pk)
        compare_cols = DFColValidator(df_c1, df_c2)

        true_common_cols = set(compare_cols.get_intersection_groups(["all"]))
        self._common_cols = set(compare_cols.get_intersection_groups(["all"], ["pk"]))
        df1_cols = set(df_c1.get_columns_from_groups(["all"]))
        df2_cols = set(df_c2.get_columns_from_groups(["all"]))
        self._df1_extracols = df1_cols - true_common_cols
        self._df2_extracols = df2_cols - true_common_cols
        self.dfs_extracols = {"df1": self._df1_extracols, "df2": self._df2_extracols}

        self._diff_postfix, self._sum_postfix = "_is_diff", "_sum_error"
        self._columns_diff_postfix = [
            column + self._diff_postfix for column in self._common_cols
        ]

        cols_not_in_main, cols_not_in_ref = df2_cols - df1_cols, df1_cols - df2_cols
        if cols_not_in_main:
            self._s_print(f"cols not in main: {cols_not_in_main}")
        if cols_not_in_ref:
            self._s_print(f"cols not in ref: {cols_not_in_ref}")

        dummy_column = "is_joined_"
        self._dummy1, self._dummy2 = dummy_column + "main", dummy_column + "ref"

        df1 = self._df.withColumn(self._dummy1, F.lit(1)).alias("main")
        df2 = self._df_ref.withColumn(self._dummy2, F.lit(1)).alias("ref")

        self._df_joined = df1.join(df2, on=self._pk, how="full")

        # diff in non PK cols
        # creates as a new attribute self.df_with_errors
        diff_results_dict = self.__compare_calc_diff()

        # diff in PK cols
        self.matching_results = self.__compare_calc_pk()

        # cnt of error / count of all correct matching
        self.dict_cols_with_errors = self._get_sorted_dict(
            diff_results_dict, self.matching_results[2]
        )
        # printing results
        if not self._common_cols:
            self._s_print("There are no common columns outside of PK")
        elif self.dict_cols_with_errors:
            self._s_print(
                f"Errors in columns - {{'column': [count is_error, share is_error]}}"
            )
            self.__print_dict(self.dict_cols_with_errors, "dict_cols_with_errors")
        else:
            self._s_print("There are no errors in non PK columns")

        self._s_print("\nCount stats of matching main and reference tables:")
        for key, val in dict(
            zip(self._cases_full_join.keys(), self.matching_results)
        ).items():
            self._s_print("{:<25} {:,}".format(key + ":", val))
        self._v_print(
            (
                "\nUse DF in attribute `.df_with_errors` for further analysis\n"
                "You can find alternative order of columns in attr .columns_diff_reordered_all"
            )
        )

    def __compare_calc_diff(self):
        """Calculating difference in non PK columns
        Creates DF in attribute .df_with_errors to use it for manual analysis"""

        def add_column_is_diff(df, column):
            """Filter for detecting differences in non PK attributes"""
            # to save memory zero value may be changed to NULL
            cond_diff = f"""case
                when
                    ({self._dummy1} is null or {self._dummy2} is null)
                    or
                    (main.{column} is null and ref.{column} is null)
                    or
                    (main.{column} = ref.{column})
                then 0
                else 1
            end"""
            return df.withColumn(column + self._diff_postfix, F.expr(cond_diff))

        df_with_errors = reduce(add_column_is_diff, self._common_cols, self._df_joined)

        def put_postfix_columns(column, table, expr=True):
            """Helps distinguish columns with the same name but different alias of table
            Add attr .columns_diff_reordered_all to use alternative ordering of columns
            """
            if expr:
                return f"{table}.{column} as {column}_{table}"
            else:
                return f"{column}_{table}"

        basic_diff_columns = (
            *self._pk,
            self._dummy1,
            self._dummy2,
        )

        self.df_with_errors = df_with_errors.selectExpr(
            *basic_diff_columns,
            *map(
                put_postfix_columns,
                self._common_cols,
                ["main"] * len(self._common_cols),
            ),
            *map(
                put_postfix_columns, self._common_cols, ["ref"] * len(self._common_cols)
            ),
            *self._columns_diff_postfix,
            # to include all columns from both DF, including not shared attrs
            *map(
                put_postfix_columns,
                self._df1_extracols,
                ["main"] * len(self._df1_extracols),
            ),
            *map(
                put_postfix_columns,
                self._df2_extracols,
                ["ref"] * len(self._df2_extracols),
            ),
        )

        common_cols_grouped = []
        for column in self._common_cols:
            new_modified_columns = [
                put_postfix_columns(column, "main", expr=False),
                put_postfix_columns(column, "ref", expr=False),
                column + self._diff_postfix,
            ]
            common_cols_grouped.extend(new_modified_columns)

        self.columns_diff_reordered_all = (
            # col1_ref, col1_main, col1_diff... This may be easier to read and compare
            # however _common_cols is python's set and it loses order
            *basic_diff_columns,
            *common_cols_grouped,
            *map(
                put_postfix_columns,
                self._df1_extracols,
                ["main"] * len(self._df1_extracols),
                [False] * len(self._df1_extracols),
            ),
            *map(
                put_postfix_columns,
                self._df2_extracols,
                ["ref"] * len(self._df2_extracols),
                [False] * len(self._df2_extracols),
            ),
        )

        diff_results_dict = {}  # main dict with results
        if self._common_cols:  # Calculate stats of common columns excluding PK
            self.df_with_errors = self.df_with_errors.withColumn(
                "sum_errors",
                reduce(add, [col(column) for column in self._columns_diff_postfix]),
            )

            diff_results = self.df_with_errors.agg(
                *(
                    F.sum(col_is_diff + self._diff_postfix).alias(
                        col_is_diff + self._sum_postfix
                    )
                    for col_is_diff in self._common_cols
                )
            ).collect()[0]

            for column in self._common_cols:
                sum_column = column + self._sum_postfix
                diff_results_dict[column] = diff_results[sum_column]
        else:
            self._v_print(
                "No common columns are found. Results will only contain PK errors"
            )

        return diff_results_dict

    def __compare_calc_pk(self):
        """Calculating difference in PK between 2 tables"""
        df_cnt_pk_errors = (
            self._df_joined.groupBy(self._dummy1, self._dummy2).count().cache()
        )

        self._cases_full_join = {
            # 0
            "not in main table": (
                col(self._dummy1).isNull() & col(self._dummy2).isNotNull()
            ),
            # 1
            "not in reference table": (
                col(self._dummy1).isNotNull() & col(self._dummy2).isNull()
            ),
            # 2
            "correct matching": (
                col(self._dummy1).isNotNull() & col(self._dummy2).isNotNull()
            ),
        }

        cnt_results = []
        for condition in self._cases_full_join.values():
            res = df_cnt_pk_errors.filter(condition).select("count").collect()
            if res:
                res_int = res[0]["count"]
            else:
                res_int = 0

            cnt_results.append(res_int)

        return CompareTablesPkStatsGenerator(*cnt_results)

    def __round_numberic_cols_df(
        self, df, fraction_digits=SCALE_OF_NUMBER_IN_COMPARING
    ):
        """
        round only numeric columns in DF
        param:
            fraction_digits - change to get different rounding of numeric columns
        """
        numeric_cols = [
            f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)
        ]
        for c in numeric_cols:
            df = df.withColumn(c, F.round(c, fraction_digits))
        return df


class TablePartitionDescriber:
    """The class helps to get partitions of partitioned Hive table
    in a readable and ready-to-use format

    Methods:
        get_partitions_parsed: returns parsed DF like 'show partitions'
        get_max_value_from_partitions: return max value of the selected partition column,
            might include prefilter for a 'show partitions' table
        cast_col_types: change types of columns from 'show partitions'
    """

    def __init__(self, schema_table: str):
        """
        Args:
            schema_table (str): Hive table. Example: 'default.my_table'
        Raise:
            Exception if the table doesn't have partitions
        """
        self.schema_table = schema_table
        part_cols = self.__get_partitioned_cols()
        parsed_part_cols = self.__get_mapping_for_part_cols(part_cols, "partitions")
        self.df_partitions = (
            spark()
            .sql(f"show partitions {schema_table}")
            .select(F.split("partition", "/").alias("partitions"))
            .select(*parsed_part_cols)
        )

    def cast_col_types(self, dict_types: dict = None):
        """dict_types - dictionary with columns and corresponding new types
        It will only change types IN PLACE, no new or deleted columns
        example: {'dt_part': 'int', 'engine_id': 'bigint'}"""
        for key, value in dict_types.items():
            self.df_partitions = self.df_partitions.withColumn(
                key, col(key).cast(value)
            )

    def __get_partitioned_cols(self):
        """Returs list of partitioned columns"""
        schema_name, table_name = self.schema_table.split(".")
        cols = spark().catalog.listColumns(tableName=table_name, dbName=schema_name)
        part_cols = [col.name for col in cols if col.isPartition is True]
        if not part_cols:
            raise HhopException(
                f"The table {self.schema_table} doesn't have partitions"
            )
        return part_cols

    def __get_mapping_for_part_cols(self, part_cols: list, splitted_col: str):
        """Returns correctly parsed columns for result DF
        Args:
            part_cols: list of partitioned columns
            splitted_col: name of column with splitted by '/' partitions
        """
        return [
            F.split(col(splitted_col)[num], "=")[1].alias(part_col)
            for num, part_col in enumerate(part_cols)
        ]

    def get_partitions_parsed(self):
        """Returns DF with all partitions from metadata"""
        return self.df_partitions

    def get_max_value_from_partitions(self, col_name: str, prefilter=None):
        """Find max value of partitioned column
        Args:
            col_name: column name to apply max function
            prefiler: (optional) apply filter by another partitions before finding max value
        """
        col_name_dummy = "last_val"
        df_partitions = self.df_partitions
        if prefilter is not None:
            df_partitions = df_partitions.filter(prefilter)
        max_val = df_partitions.select(F.max(col_name).alias(col_name_dummy)).first()[
            col_name_dummy
        ]
        return max_val


class SchemaManager:
    """
    Class drops empty tables where there are 0 records or table folder doesn't exist

    Args:
        schema (str) - Hive schema where to find empty tables

    Methods:
        find_empty_tables - find empty tables from schema
        drop_empty_tables - drop empty tables from schema

    Attrs:
        dict_of_tables - dictionary with tables from selected schema
    """

    def __init__(self, schema: str):
        self.schema = schema
        self._cnt_list_tables()
        print(f"{self._cnt_tables} tables in {schema}")
        print(
            f"run find_empty_tables() on instance to find empty tables in {self.schema}"
        )

    def _cnt_list_tables(self):
        def get_list_of_tables(type_of_table):
            dict_table_view = {
                "table": ["tables", "tableName"],
                "view": ["views", "viewName"],
            }
            show_arg, colname = (
                dict_table_view[type_of_table][0],
                dict_table_view[type_of_table][1],
            )
            tables = (
                spark()
                .sql(f"show {show_arg} in {self.schema}")
                .select(colname)
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            return set(tables)

        self._list_of_tables_all = get_list_of_tables("table")
        self._list_of_views = get_list_of_tables("view")

        self._list_of_tables = self._list_of_tables_all - self._list_of_views
        self._cnt_tables = len(self._list_of_tables)
        self.dict_of_tables = dict.fromkeys(self._list_of_tables, 1)

    def find_empty_tables(self):
        """
        Empty tables are stored in the attribute .dict_of_tables
        1 - has data, 0 - doesn't and going to be deleted
        """
        for table in self.dict_of_tables:
            schema_name = self.schema + "." + table
            try:
                slice_df = read_table(schema_name).take(2)
                if len(slice_df) == 0:
                    self.dict_of_tables[table] = 0
            except Exception:  # spark might fail to read a table without a root folder
                self.dict_of_tables[table] = 0

        self._cnt_empty_tables = len(
            [table for table, val in self.dict_of_tables.items() if val == 0]
        )
        perc_empty = round(self._cnt_empty_tables / self._cnt_tables * 100, 2)

        print(
            f"{self._cnt_empty_tables} tables going to be dropped out of {self._cnt_tables} ({perc_empty}%)",
            "Data about tables is stored in an attribute '.dict_of_tables':",
            "\n",
            "1 - has data, 0 - doesn't and going to be deleted",
            end="\n\n",
            sep="",
        )

        print(
            f"run drop_empty_tables() on instance to drop empty tables in {self.schema}"
        )

    def drop_empty_tables(self):
        """
        Drops empty tables in a selected schema
        Use this with caution and check if the attribute .dict_of_tables has some non empty tables
        """

        for table, val in self.dict_of_tables.items():
            if val == 0:
                spark().sql(f"drop table if exists {self.schema}.{table}")

        self._cnt_list_tables()

        print(
            f"After dropping tables there are {self._cnt_tables} tables in {self.schema}"
        )


ErrorsInSCD2TableGenerator = namedtuple(
    "Errors_In_SCD2_table",
    "duplicates_by_pk invalid_dates broken_history duplicates_by_version",
)


class SCD2Helper(pyspark.sql.dataframe.DataFrame):
    """Class helps to work with SCD2 tables"""

    def __init__(
        self,
        df: DataFrame,
        pk: List[str] = None,
        non_pk: List[str] = None,
        time_col: str = None,
        BOW: str = "1000-01-01",
        EOW: str = "9999-12-31",
    ) -> "SCD2Helper":
        """Pass correct configs to use for the provided DF

        Args:
            df (DataFrame): _description_
            pk (List[str]): Primary Key. Defaults to empty list.
            non_pk (List[str]): Non key attributes, for which changes should be created
            time_col (str): Attribute which will be used for sorting and generating row_actual_from
            BOW (str, optional): BeginningOfWorld. Defaults to "1000-01-01".
            EOW (str, optional): EndOfWorld. Defaults to "9999-12-31".

        Correct format of tech columns: row_actual_from, row_actual_to
        """
        # add df, pk, non_pk_cols(which can be calculated)
        # change_tech_col_names is the last thing, fix df yourself!

        kwargs = (
            locals()
        )  # doesn't look nice but it makes possible to reuse passed attrs
        self._passed_args = {
            x: kwargs[x] for x in kwargs if x not in ["df", "self", "__class__"]
        }

        super().__init__(
            df._jdf, df.sql_ctx  # change sql_ctx to sparkSession in spark>=3.4
        )  # magic to integrate pyspark DF into this class
        self._df = df

        self._pk = pk or []
        self._non_pk = non_pk or []
        self._time_col = time_col

        self._BOW = BOW
        self._EOW = EOW

        self._inc_any_version = W.partitionBy(*self._pk).orderBy("row_actual_from")
        self._inc_true_version = W.partitionBy(*self._pk, "version_num").orderBy(
            "row_actual_from"
        )

        self._tech_cols_default_names = ["row_hash", "row_actual_from", "row_actual_to"]
        # create all tech cols
        self._df = self._df.withColumn("row_hash", self.hash_cols())
        all_cols = DFColCleaner(self._df).get_columns_from_groups(["all"])
        for column in self._tech_cols_default_names[1:]:
            if column not in all_cols:
                self._df = self._df.withColumn(column, F.lit(None))

        self._df_cols_cl = DFColCleaner(
            self._df, pk=pk, non_pk=non_pk, tech_cols=self._tech_cols_default_names
        )

        self._all_attrs_ordered = self._df_cols_cl.get_columns_from_groups(["all"])
        self._extra_attributes = self._df_cols_cl.get_columns_from_groups(["extra"])

    def df_to_scd2(self):
        """
        Create SCD2 DF
        Required attrs:
            pk
            non
        """
        if not self._time_col:
            raise HhopException("time_col is a required parameter")

        df_cols_no_tech = self._df_cols_cl.get_columns_from_groups(
            ["all"], ["tech_cols"]
        )
        all_attrs_ordered = [
            *df_cols_no_tech,
            "row_hash",
            col("row_actual_from").cast("string"),
            col("row_actual_to").cast("string"),
        ]

        window_pk_asc = W.partitionBy(*self._pk).orderBy(self._time_col)
        df_hash = (
            self._df.withColumn(
                "row_hash", self.hash_cols()
            )  # hash of pk and essential non pk attributes
            .withColumn("row_actual_from", col(self._time_col).cast("date"))
            .withColumn(
                "version_num",
                F.count(
                    F.when(F.lag("row_hash").over(window_pk_asc) != col("row_hash"), 1)
                ).over(window_pk_asc),
            )
        )
        df_ded_by_version = deduplicate_df(
            df_hash,
            pk=[*self._pk, "version_num"],
            order_by_cols=[
                self._time_col
            ],  # first row with same hash with respect to version numbers
        )
        df_ded_by_date = deduplicate_df(
            df_ded_by_version,
            pk=[*self._pk, "row_actual_from"],
            order_by_cols=[F.desc(self._time_col)],  # last value for every day
        )

        df_result = df_ded_by_date.withColumn(
            "row_actual_to",
            F.coalesce(
                F.date_sub(F.lead("row_actual_from").over(self._inc_any_version), 1),
                F.lit(self._EOW),
            ),
        ).select(*all_attrs_ordered)

        return SCD2Helper(
            df_result, **self._passed_args
        )  # return same class with updated DF, is there a better way of doing this?

    def hash_cols(self, *cols):
        """MD5 hash of pk+non_pk columns by default.
        Or hash of provided columns.
        Columns will always be in sorted order"""
        if not len(cols):
            cols = [*self._pk, *self._non_pk]
        return F.md5(F.concat_ws("", *sorted(cols)))  # sorting for consistent hash

    def _df_with_true_version_num(self, df):
        df_with_true_versions = (
            df.withColumn(
                "row_hash", self.hash_cols()
            )  # hash of pk and essential non pk attributes
            .withColumn(
                "previous_to", F.lag("row_actual_to").over(self._inc_any_version)
            )
            .withColumn(
                "diff_with_previous_hash",
                F.lag("row_hash").over(self._inc_any_version) != col("row_hash"),
            )
            .withColumn(
                "version_num",  # true version num
                F.count(
                    F.when(
                        col("diff_with_previous_hash")
                        | (
                            col("row_actual_from") != F.date_add(col("previous_to"), 1)
                        ),  # any hole in history also creates new version
                        F.lit(1),
                    )
                ).over(self._inc_any_version),
            )
        )
        return df_with_true_versions

    def validate_scd2(self) -> namedtuple:
        """Validation of a SCD2 table
        Right now it looks messy, it's going to be better

        Args:
            df (_type_): _description_
            pk (_type_): _description_
            non_pk (_type_): _description_
            time_col (_type_): _description_
        Returns:
            namedtuple with 4 counts. All 0 means all checks are passed

        """

        # check on basic key: pk + row_actual_to
        pk_check_basic = [*self._pk, "row_actual_to"]
        self.basic_pk_check = DFExtender(self._df, pk=pk_check_basic, silent_mode=True)
        self.basic_pk_check.get_info(pk_stats=True, null_stats=False)
        cnt_duplicates_pk = self.basic_pk_check.pk_stats[2]
        if cnt_duplicates_pk != 0:
            print(
                f"There are {cnt_duplicates_pk} PK duplicates by {pk_check_basic} "
                "Look at `.basic_pk_check.df_duplicates_pk`"
            )

        # check if there are invalid dates in row_actual_(to/from)
        def is_valid_date(column):
            return (
                ~F.upper(column).isin(["", "NULL"])
                & col(column).rlike("^\d{4}-\d{2}-\d{2}$")
                & col(column).cast("date").isNotNull()
            )

        self.df_invalid_dates = (
            self._df.withColumn("valid_date_from", is_valid_date("row_actual_from"))
            .withColumn("valid_date_to", is_valid_date("row_actual_to"))
            .withColumn(
                "incorrect_direction",
                F.coalesce(
                    col("row_actual_from") > col("row_actual_to"),
                    F.lit(False).cast("boolean"),
                ),
            )
            .filter(
                "valid_date_from is false or valid_date_to is false or incorrect_direction is true"
            )
        )
        cnt_invalid_dates = self.df_invalid_dates.count()
        if cnt_invalid_dates != 0:
            print(
                f"{cnt_invalid_dates} rows with invalid dates, look at `.df_invalid_dates`"
            )

        # check if version history is broken (overlapping or non continuous)
        window_continuous_history = W.partitionBy(*self._pk).orderBy("row_actual_from")
        self.df_broken_history = self._df.withColumn(
            "is_good_history",
            col("row_actual_to")
            == F.date_sub(
                F.lead("row_actual_from").over(window_continuous_history),
                1,
            ),
        ).filter("is_good_history is false")
        cnt_broken_history = self.df_broken_history.count()
        if cnt_broken_history != 0:
            print(
                f"{cnt_broken_history} rows with invalid history, look at `.df_broken_history`"
            )

        # check if there are new versions with the same hash (extra version in this case is harmless but wrong)

        df_hash_version = self._df_with_true_version_num(self._df)
        pk_check_version = [*self._pk, "version_num"]
        self.pk_by_version = DFExtender(
            df_hash_version, pk=pk_check_version, silent_mode=True
        )
        self.pk_by_version.get_info(pk_stats=True, null_stats=False)
        cnt_duplicates_version = self.pk_by_version.pk_stats[2]

        if cnt_duplicates_version != 0:
            print(
                f"There are {cnt_duplicates_version} PK duplicates by {pk_check_version} "
                "Look at `.pk_by_version.df_duplicates_pk`"
            )

        print(f"Number of records: {self.basic_pk_check.pk_stats[0]:,}")

        errors_scd2 = ErrorsInSCD2TableGenerator(
            cnt_duplicates_pk,
            cnt_invalid_dates,
            cnt_broken_history,
            cnt_duplicates_version,
        )

        if not any(errors_scd2):
            print("All tests passed")

        return errors_scd2

    def fill_scd2_history(self) -> DataFrame:
        """Method fills holes in SCD2 history with NULL values
        1. It searches where it is need to fill history behind of the current version
        2. If it is last version of window, check if there's a need in the version till EOW
        Returns:
            DataFrame: DataFrame with full history from BOW to EOW
        """

        null_attrs = self._df_cols_cl.get_columns_from_groups(
            ["non_pk", "extra"], ["tech_cols"]
        )
        null_attrs_create_null = [F.lit(None).alias(x) for x in null_attrs]
        cols_null_holes = lambda x: [
            *self._pk,
            *x,
            *self._df_cols_cl.get_columns_from_groups(["tech_cols"]),
        ]
        all_attrs_ordered_create_null = cols_null_holes(null_attrs_create_null)
        all_attrs_ordered = cols_null_holes(null_attrs)

        window_inc_versions = W.partitionBy(*self._pk).orderBy("row_actual_from")
        df_holes = (
            self._df.withColumn(
                "earliest_from_in_hole",
                F.coalesce(
                    F.date_add(F.lag("row_actual_to").over(window_inc_versions), 1),
                    F.lit(self._BOW),
                ),
            )
            .withColumn(
                "is_need_fill_behind",
                col("row_actual_from") != col("earliest_from_in_hole"),
            )
            .withColumn(
                "hole_behind",
                F.when(
                    col("is_need_fill_behind"),
                    F.concat_ws(
                        ",",
                        col("earliest_from_in_hole"),
                        F.date_sub("row_actual_from", 1),
                    ),
                ),
            )
            .withColumn(
                "hole_plus_infinity_history",
                F.when(
                    F.lead("row_actual_to").over(window_inc_versions).isNull()
                    & (col("row_actual_to") != F.lit(self._EOW)),
                    F.concat_ws(",", F.date_add("row_actual_to", 1), F.lit(self._EOW)),
                ),
            )
            .withColumn(
                "merged_history",
                F.concat_ws(";", "hole_behind", "hole_plus_infinity_history"),
            )
            .withColumn("from_to_str", F.explode(F.split("merged_history", ";")))
            .filter(col("from_to_str") != "")
            .withColumn("splitted_from_to", F.split("from_to_str", ","))
            .withColumn("row_actual_from", col("splitted_from_to").getItem(0))
            .withColumn("row_actual_to", col("splitted_from_to").getItem(1))
            .select(*all_attrs_ordered_create_null)
        )

        df_result = self._df.unionByName(df_holes).select(*all_attrs_ordered)

        return SCD2Helper(df_result, **self._passed_args)

    def merge_scd2_history(self) -> DataFrame:
        """
        If there are holes in history, this method is going to extrapolate versions falsely
        If it is the case, set a flag fill_history=True
        Returns:
            DataFrame: DF with merged SCD2 history
        """
        df_cols_no_tech = self._df_cols_cl.get_columns_from_groups(
            ["all"], ["tech_cols"]
        )
        all_attrs_ordered = [
            *df_cols_no_tech,
            "row_hash",
            col("row_actual_from").cast("string"),
            col("row_actual_to").cast("string"),
        ]
        df_with_true_versions = self._df_with_true_version_num(self._df)
        df_max_2_vers_per_true_version = (
            df_with_true_versions.withColumn(
                "version_num_behind",
                F.coalesce(
                    F.lag("version_num").over(self._inc_any_version),
                    col("version_num") - 1,
                ),
            )
            .withColumn(
                "version_num_infront",
                F.coalesce(
                    F.lead("version_num").over(self._inc_any_version),
                    col("version_num") + 1,
                ),
            )
            .filter(
                ~(  # exclude rows surrounded by the same version.
                    # so at max 2 rows are left with the same version
                    (col("version_num") == col("version_num_behind"))
                    & (col("version_num") == col("version_num_infront"))
                )
            )
            .withColumn(
                "row_actual_to",  # if there is second version, we get row_actual_to from it
                F.coalesce(
                    F.lead("row_actual_to").over(self._inc_true_version),
                    col("row_actual_to"),
                ),
            )
        )

        df_ded_by_version = deduplicate_df(
            df_max_2_vers_per_true_version,
            pk=[*self._pk, "version_num"],
            order_by_cols=[
                "row_actual_from"
            ],  # first row with same hash with respect to version numbers
        )
        df_result = df_ded_by_version.select(*all_attrs_ordered)

        return SCD2Helper(df_result, **self._passed_args)

    def merge_scd2_update(
        self, df_new: DataFrame, is_deduplicate_df=True
    ) -> "SCD2Helper":
        """_summary_

        Args:
            df_new (DataFrame): It must have the same key as main DF!
                Otherwise the result will be incorrect

        Returns:
            SCD2Helper: _description_
        """
        # TODO: raise error when cols are different or different PK

        # TODO: add row_hash and clean up this mess
        # TODO: think what can be second DF. Deduplicated? Does it contain tech_cols?

        # validate that 2 df have the same columns

        # cols_not_pk = set(common_cols) - set(self._pk) - set(["row_hash"])
        # # tech_cols_final = ["row_hash", "row_actual_from", "row_actual_to"]
        # tech_cols_final = []
        # dict_num_cols = {
        #     str(enum): cols
        #     for enum, cols in zip([1, 2], cols_not_pk - set(tech_cols_final))
        # }

        # def rename_cols(df, num):
        #     df_temp = df
        #     for column in cols_not_pk:
        #         df_temp = df_temp.withColumnRenamed(column, column + str(num))
        #     return df_temp

        # self._df.printSchema()
        if "row_hash" not in df_new.columns:
            df_new = df_new.withColumn("row_hash", self.hash_cols())

        cols1 = DFColCleaner(
            self._df,
            pk=self._pk,
            non_pk=self._non_pk,
            tech_cols=self._tech_cols_default_names,
        )
        cols2 = DFColCleaner(
            df_new,
            pk=self._pk,
            non_pk=self._non_pk,
            tech_cols=self._tech_cols_default_names,
        )
        df1 = cols1.mass_rename(
            "_1",
            is_append_suffix=True,
            group_cols_include=["all"],
            group_cols_exclude=["pk"],
        )
        df2 = cols2.mass_rename(
            "_2",
            is_append_suffix=True,
            group_cols_include=["all"],
            group_cols_exclude=["pk"],
        )
        # df1.printSchema()
        # df1.printSchema()
        # cols1.

        # df1 = DFColCleaner.mass_rename(self._df, 1)
        # df_new = rename_cols(df_new, 2)
        # df1.printSchema()
        # df2 = rename_cols(df_new._df, 2)
        df_new = df2
        df_history = df1.filter(col("row_actual_to_1") != self._EOW)
        df_actual = df1.filter(col("row_actual_to_1") == self._EOW)

        df_merged = df_actual.join(df2, on=[*self._pk], how="full").withColumn(
            "operation_type",
            F.when(df_actual["row_hash_1"] == df_new["row_hash_2"], F.lit("nochange"))
            .when(df_actual["row_hash_1"] != df_new["row_hash_2"], F.lit("update"))
            .when(df_actual["row_hash_1"].isNull(), F.lit("insert"))
            .when(df_new["row_hash_2"].isNull(), F.lit("close")),
        )
        # writing stg table to save many reads of source table
        stg_table_name = f"hhop_stg_merged_scd2_{os.getpid()}"
        # drop_table is done inside write_table using flag rewrite
        df_merged = write_read_table(
            df_merged,
            stg_table_name,
            schema=DEFAULT_SCHEMA_WRITE,
            rewrite=True,
            verbose=True,
        )
        # DRY
        tech_cols_suffix_1 = {f"{i}_{1}" for i in self._tech_cols_default_names}
        tech_cols_suffix_2 = {f"{i}_{2}" for i in self._tech_cols_default_names}
        cols_suffix_1 = [
            i
            for i in DFColCleaner.get_columns_with_suffix(df_merged, "_1")
            if i not in tech_cols_suffix_1
        ]
        cols_suffix_2 = [
            i
            for i in DFColCleaner.get_columns_with_suffix(df_merged, "_2")
            if i not in tech_cols_suffix_2
        ]
        df_close = (
            df_merged.filter(col("operation_type") == "close")
            .withColumnRenamed("row_actual_from_1", "row_actual_from")
            .withColumn("row_actual_to", F.date_sub(F.current_date(), 1))
            .withColumnRenamed("row_hash_1", "row_hash")
            .select(
                *self._pk,
                *cols_suffix_1,  # TODO: need to exclude tech cols and rename back
                *self._tech_cols_default_names,
            )
        )

        df_update = df_merged.filter(
            col("operation_type") == "update"
        )  # two versions on each update
        df_update_close = (
            df_update.withColumnRenamed("row_actual_from_1", "row_actual_from")
            .withColumn("row_actual_to", F.date_sub(F.current_date(), 1))
            .withColumnRenamed("row_hash_1", "row_hash")
            .select(
                *self._pk,
                *DFColCleaner.get_columns_with_suffix(df_merged, "_1"),
                *self._tech_cols_default_names,
            )
        )
        df_update_new = (
            df_update.withColumn("row_actual_from", F.current_date())
            .withColumnRenamed("row_actual_to_1", "row_actual_to")
            .withColumnRenamed("row_hash_1", "row_hash")
            .select(
                *self._pk,
                *DFColCleaner.get_columns_with_suffix(df_merged, "_1"),
                *self._tech_cols_default_names,
            )
        )
        df_nochange = (
            df_merged.filter(col("operation_type") == "nochange")
            .withColumnRenamed("row_actual_from_1", "row_actual_from")
            .withColumnRenamed("row_actual_to_1", "row_actual_to")
            .withColumnRenamed("row_hash_1", "row_hash")
            .select(
                *self._pk,
                *DFColCleaner.get_columns_with_suffix(df_merged, "_1"),
                *self._tech_cols_default_names,
            )
        )
        df_insert = (
            df_merged.filter(col("operation_type") == "insert")
            .withColumnRenamed("row_actual_from_1", "row_actual_from")
            .withColumnRenamed("row_actual_to_1", "row_actual_to")
            .withColumnRenamed("row_hash_2", "row_hash")
            .select(
                *self._pk,
                *cols_suffix_2,
                *self._tech_cols_default_names,
            )
        )
        # DFColCleaner.mass_rename(df_insert, '_2', False,)

        df_merged_update = union_all(
            df_history, df_nochange, df_close, df_update_close, df_update_new, df_insert
        ).select(cols1.get_columns_from_groups(["all"]))

        # return df_result
        return SCD2Helper(df_merged_update, **self._passed_args)

    def join_scd2(self, df2: "SCD2Helper", join_type: str = "full") -> DataFrame:
        """_summary_

        Args:
            df2 (SCD2Helper): DataFrame from the enclosing class
            join_type (str): type of join. Default to 'full'

        Returns:
            DataFrame: joined DataFrame with SCD2 history
        """
        instance1 = self
        instance2 = df2
        df1, df2 = self._df.alias("df1"), df2.alias("df2")
        comp_cols = DFColValidator(instance1._df_cols_cl, instance2._df_cols_cl)
        comp_cols.is_equal_cols_groups(["pk"], raise_exception=True)

        tech_attr = {"row_actual_from", "row_actual_to", "row_hash"}

        def get_non_pk_attrs(df):
            all_attrs = set(df.columns)
            pk_attrs = set(df._pk)
            non_pk_attrs = all_attrs - tech_attr - pk_attrs
            return non_pk_attrs

        greatest_from = F.greatest(
            df1["row_actual_from"], df2["row_actual_from"]
        )  # functions greatest/least ignore NULL values
        least_to = F.least(df1["row_actual_to"], df2["row_actual_to"])
        pk_cond_join = " and ".join(
            [f"df1.{pk_col} = df2.{pk_col}" for pk_col in self._pk]
        )

        cond_scd2_join = F.expr(pk_cond_join) & (greatest_from <= least_to)
        df_joined = df1.join(df2, on=cond_scd2_join, how=join_type)

        df_joined_scd2 = df_joined.select(
            *[
                F.coalesce(f"df1.{pk_col}", f"df2.{pk_col}").alias(pk_col)
                for pk_col in self._pk
            ],  # PK
            *instance1._df_cols_cl.get_columns_from_groups(
                ["all"], ["tech_cols", "pk"]
            ),  # non_pk+extra in df1
            *instance2._df_cols_cl.get_columns_from_groups(
                ["all"], ["tech_cols", "pk"]
            ),  # non_pk+extra in df2
            greatest_from.alias("row_actual_from"),
            least_to.alias("row_actual_to"),
        )

        # return df_joined_scd2
        return SCD2Helper(df_joined_scd2, **self._passed_args)
