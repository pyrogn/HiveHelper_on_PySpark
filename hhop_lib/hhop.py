"main module with helpers"
from functools import reduce
from operator import add
from collections import namedtuple
from typing import List

import pyspark
from pyspark.sql import DataFrame
from spark_init import spark
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql.types import NumericType

from funs import read_table, make_set_lower, deduplicate_df
from exceptions import HhopException

# lower if output of errors is too long
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
        custom_null_values: list[str] = ["", "NULL", "null", "Null"],
    ) -> DataFrame:
        """Initialization

        Args:
            df (pyspark.sql.dataframe.DataFrame): DataFrame to use for analysis
            pk ((list, tuple, set), optional): Primary Key of the DF. Defaults to None.
            verbose (bool, optional): Choose if you want to receive additional messages.
                Defaults to False.
            custom_null_values (list[str]) - provide values that will be considered as NULLs in NULLs check
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
            self._df._jdf, self._df.sql_ctx
        )  # magic to integrate pyspark DF into this class

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
            self.__check_cols_entry(self._pk, self._df.columns)
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
        if null_columns is None:
            null_columns = []
        if not hasattr(self, "dict_null_in_cols"):
            self._s_print("Running method .get_info() first", end="\n")
            self.get_info(pk_stats=False)

        self.__check_cols_entry(null_columns, self._df.columns)

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
            df_with_errors - df with errors.
            matching_stats - namedtuple with stats on matching by pk columns
            dict_cols_with_errors - dict with aggregated errors by non-pk columns
                All numeric columns in DFs get rounded with scale specified in
                    SCALE_OF_NUMBER_IN_COMPARING
                Added attributes:
                    is_joined_main - 1 if this PK in main DF, NULL otherwise
                    is_joined_ref - 1 if this PK in reference DF, NULL otherwise
                    is_diff_[column name] - 1 if column differs between Main and Ref DF
                        otherwise 0
                    sum_errors - sum of all errors in a row. Doesn't exist
                        if there are no errors in non PK attributes.
                It is not cached, write it to Hive or cache it with filters!
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

        df1_cols = set(self._df.columns)
        df2_cols = set(self._df_ref.columns)
        self._common_cols = (df1_cols & df2_cols) - set(self._pk)
        self._df1_extracols = df1_cols - self._common_cols - set(self._pk)
        self._df2_extracols = df2_cols - self._common_cols - set(self._pk)

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
            # col1_ref, col1_main, col1_diff... This may be easier to read
            # however common_cols is python's set and it loses order
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
                [False] * len(self._df1_extracols),
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

    def __check_cols_entry(self, cols_subset, cols_all):
        """
        Raise exception if provided columns are not in DF
        """

        extra_columns = make_set_lower(cols_subset) - make_set_lower(cols_all)

        if extra_columns:
            raise HhopException(
                f"columns {extra_columns} are not present in provided columns: {cols_all}"
            )

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
            spark.sql(f"show partitions {schema_table}")
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
        cols = spark.catalog.listColumns(tableName=table_name, dbName=schema_name)
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
                spark.sql(f"show {show_arg} in {self.schema}")
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
                spark.sql(f"drop table if exists {self.schema}.{table}")

        self._cnt_list_tables()

        print(
            f"After dropping tables there are {self._cnt_tables} tables in {self.schema}"
        )


class SCD2Helper:
    """Class helps to work with SCD2 tables"""

    def __init__(self, change_tech_col_names: dict = None) -> DataFrame:
        self.tech_col_names = {  # for quick change if needed
            "row_hash": "row_hash",
            "row_actual_from": "row_actual_from",
            "row_actual_to": "row_actual_to",
        }
        if change_tech_col_names:
            self.tech_col_names.update(change_tech_col_names)

    def df_to_scd2(self, df, pk_cols, non_pk_cols, time_col):
        """
        Create SCD2 DF
        Attrs:
            non_pk_cols (list):
        """

        df_cols = df.columns

        window_pk_asc = W.partitionBy(*pk_cols).orderBy(time_col)
        df_hash = (
            df.withColumn(
                "row_hash", self.hash_cols(*pk_cols, *non_pk_cols)
            )  # hash of pk and essential non pk attributes
            .withColumn("row_actual_from", col(time_col).cast("date"))
            .withColumn(
                "version_num",
                F.count(
                    F.when(F.lag("row_hash").over(window_pk_asc) != col("row_hash"), 1)
                ).over(window_pk_asc),
            )
        )
        df_ded_by_version = deduplicate_df(
            df_hash,
            pk=[*pk_cols, "version_num"],
            order_by_cols=[
                time_col
            ],  # first row with same hash with respect to version numbers
        )
        df_ded_by_date = deduplicate_df(
            df_ded_by_version,
            pk=[*pk_cols, "row_actual_from"],
            order_by_cols=[F.desc(time_col)],  # last value for every day
        )

        window_row_actual_to = W.partitionBy(*pk_cols).orderBy("row_actual_from")

        alias_tech_col_names = lambda x: col(x).alias(self.tech_col_names[x])

        df_result = df_ded_by_date.withColumn(
            "row_actual_to",
            F.coalesce(
                F.date_sub(F.lead("row_actual_from").over(window_row_actual_to), 1),
                F.lit("9999-12-31"),
            ),
        ).select(
            *df_cols,
            alias_tech_col_names("row_hash"),
            alias_tech_col_names("row_actual_from").cast("string"),
            alias_tech_col_names("row_actual_to").cast("string"),
        )

        return df_result

    def hash_cols(self, *cols):
        return F.md5(F.concat_ws("", *sorted(cols)))  # sorting for consistent hash

    def validate_scd2(self, df, pk, non_pk, time_col) -> None:
        """Validation of a SCD2 table
        Right now it looks messy, it's going to be better

        Args:
            df (_type_): _description_
            pk (_type_): _description_
            non_pk (_type_): _description_
            time_col (_type_): _description_
        """
        # check on basic key: pk + row_actual_to
        pk_check_basic = [*pk, "row_actual_to"]
        self.basic_pk_check = DFExtender(df, pk=pk_check_basic, silent_mode=True)
        self.basic_pk_check.get_info(null_stats=False)
        if self.basic_pk_check.pk_stats[2] != 0:
            print(
                f"There are {self.basic_pk_check.pk_stats[2]} PK duplicates by {pk_check_basic} "
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
            df.withColumn("valid_date_from", is_valid_date("row_actual_from"))
            .withColumn("valid_date_to", is_valid_date("row_actual_to"))
            .filter("valid_date_from is False or valid_date_to is false")
        )
        cnt_invalid_dates = self.df_invalid_dates.count()
        if cnt_invalid_dates != 0:
            print(
                f"{cnt_invalid_dates} rows with invalid dates, look at `.df_invalid_dates`"
            )

        # check if version history is broken (overlapping or non continuous)
        window_continuous_history = W.partitionBy(*pk).orderBy("row_actual_from")
        self.df_broken_history = df.withColumn(
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
        window_pk_asc = W.partitionBy(*pk).orderBy(time_col)
        df_hash_versions = df.withColumn(
            "row_hash", self.hash_cols(*pk, *non_pk)
        ).withColumn(
            "version_num",
            F.count(
                F.when(F.lag("row_hash").over(window_pk_asc) != col("row_hash"), 1)
            ).over(window_pk_asc),
        )
        pk_check_versions = [*pk, "version_num"]
        self.pk_by_versions = DFExtender(
            df_hash_versions, pk=pk_check_versions, silent_mode=True
        )
        self.pk_by_versions.get_info(null_stats=False)
        self.pk_by_versions.pk_stats[2] == 0
        if self.pk_by_versions.pk_stats[2] != 0:
            print(
                f"There are {self.pk_by_versions.pk_stats[2]} PK duplicates by {pk_check_versions} "
                "Look at `.pk_by_versions.df_duplicates_pk`"
            )
        print(f"Number of records: {self.basic_pk_check.pk_stats[0]:,}")
        if (
            self.basic_pk_check.pk_stats[2] == 0
            and cnt_invalid_dates == 0
            and cnt_broken_history == 0
            and self.pk_by_versions.pk_stats[2] == 0
        ):
            print("All tests passed")

    def merge_scd2_update(self, df_new):
        pass

    def join_scd2(
        self,
    ):
        pass
