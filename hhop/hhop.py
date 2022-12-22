from functools import reduce
from operator import add
from inspect import cleandoc
from typing import List

from pyspark.sql import DataFrame
from spark_init import pyspark, spark
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql.types import NumericType
from funs import read_table
from exceptions import EmptyDFException, ExtraColumnsException

# lower if output of errors is too long
# set higher if you need longer dictionary to pring
DICT_PRINT_MAX_LEN = 15
SCALE_OF_NUMBER_IN_COMPARING = 2


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

    !Class is not tested on complex types as array and struct!
    """

    def __init__(
        self,
        df: DataFrame,
        pk: List[str] = [],
        verbose: bool = False,
    ) -> DataFrame:
        """_summary_
        
        Args:
            df (pyspark.sql.dataframe.DataFrame): DataFrame to use for analysis
            pk ((list, tuple, set), optional): Primary Key of the DF. Defaults to None.
            verbose (bool, optional): Choose if you want to receive additional messages. \
                Defaults to False.
        
        Return:
            DataFrame as provided in call
        """
        self.pk = pk
        self.df = df
        self.verbose = verbose

        super().__init__(self.df._jdf, self.df.sql_ctx)

        # get sorted dict with count + share without zero values
        self._get_sorted_dict = lambda dict, val: {
            k: [v, round(v / val, 4)]
            for k, v in sorted(dict.items(), key=lambda item: -item[1])
            if v > 0
        }

        # print if verbose = True
        self.v_print = (
            lambda *args, **kwargs: print(*args, **kwargs) if verbose else None
        )
        self._print_stats = lambda string, val: print(
            "{:<25} {:,}".format(string + ":", val)
        )

        self._sanity_checks()

    def __print_dict(self, dictionary: dict, attr_name: str, *args, **kwargs):
        """Prevent printing a dictionary longer than DICT_PRINT_MAX_LEN

        Args:
            dictionary (dict): dictionary to print and check its length
            attr_name (str): prints attr_name if dictionary is too big
        """
        if len(dictionary) <= DICT_PRINT_MAX_LEN:
            print(dictionary, *args, **kwargs)
        else:
            print(f"dictionary is too large ({len(dictionary)} > {DICT_PRINT_MAX_LEN})")
            print(f"You can access the dictionary in the attribute {attr_name}")

    def _sanity_checks(self):
        """Sanity checks for provided DataFrame
        1. Check if PK columns are in DF
        2. Check if DF is not empty

        Raises:
            Exception: Provide only columns to pk that are present in the DF
        """

        if self.pk:
            self.__check_cols_entry(self.pk, self.df.columns)
        if len(self.df.head(1)) == 0:
            raise EmptyDFException("DF is empty")

    def get_info(self):
        """Methods returns statistics about DF

        1. If PK is provided there will be statistics on PK duplicates
        2. Statistics about NULL values in columns

        Attrs:
            dict_null_in_cols
            df_duplicates_pk (optional) (from method _analyze_pk)
            df_with_nulls (optional) (from method get_df_with_null)
        """
        self._analyze_pk()
        self._print_pk_stats()

        cnt_all = self.pk_stats[0]

        dict_null = (
            self.df.select(
                [F.count(F.when(col(c).isNull(), c)).alias(c) for c in self.df.columns]
            )
            .rdd.collect()[0]
            .asDict()
        )
        self.dict_null_in_cols = self._get_sorted_dict(dict_null, cnt_all)

        if self.pk and self.verbose:
            for key in self.pk:
                if key in self.dict_null_in_cols:
                    print(f"PK column '{key}' contains empty values, be careful!")

        print(f"\nNull values in columns - {{'column': [count NULL, share NULL]}}:")
        self.__print_dict(self.dict_null_in_cols, "dict_null_in_cols")

        self.v_print(
            f"Use method `.get_df_with_null(List[str])` to get a df with specified NULL columns"
        )

    def _analyze_pk(self):
        """
        Attr:
            pk_stats - [Count all, Unique PK count, PK with duplicates]
            df_duplicates_pk (optional) - DF with PK duplicates if there are any
        """

        df_temp = (
            self.df.groupBy(self.pk)
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

        if self.pk:
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
                window_duplicates_pk = W.partitionBy(self.pk)
                self.df_duplicates_pk = (
                    self.df.withColumn(
                        "cnt_pk", F.count(F.lit(1)).over(window_duplicates_pk)
                    )
                    .filter(col("cnt_pk") > 1)
                    .orderBy([col("cnt_pk").desc(), *[col(i) for i in self.pk]])
                )
                self.v_print(
                    f"You can access DF with PK duplicates in an attribute `.df_duplicates_pk`\n"
                )
        else:
            self.v_print(f"PK hasn't been provided!\n")
        # 0 - cnt rows, 1 - Unique PK, 2 - PK with duplicates
        self.pk_stats = [cnt_all, cnt_unique_pk, cnt_with_duplicates_pk]

    def _print_pk_stats(self):
        """Method only prints stats"""
        self._print_stats("Count all", self.pk_stats[0])
        if self.pk:
            self._print_stats("Unique PK count", self.pk_stats[1])
            self._print_stats("PK with duplicates", self.pk_stats[2])

    def get_df_with_null(self, null_columns=[]):
        """This method calculates and returns DF with selected cols that have NULL values

        Args:
            null_columns (list, optional): Columns . Defaults to [].

        Raises:
            ExtraColumnsException: Provide only columns to null_columns that are present in the DF

        Returns:
            pyspark.sql.dataframe.DataFrame:
                Returns a DF sorted by count of nulls in selected columns
                in descending order
        """
        if not hasattr(self, "dict_null_in_cols"):
            print("Running method .get_info() first", end="\n")
            self.get_info()

        self.__check_cols_entry(null_columns, self.df.columns)

        if self.dict_null_in_cols:
            if set(null_columns) & set(self.dict_null_in_cols):
                cols_filter = null_columns
            else:
                print(
                    f"No NULL values found in provided {null_columns}, using all: {self.dict_null_in_cols.keys()}"
                )
                cols_filter = self.dict_null_in_cols.keys()

            self.df_with_nulls = (
                self.df.withColumn(
                    "cnt_nulls",
                    sum(self.df[col].isNull().cast("int") for col in cols_filter),
                )
                .filter(col("cnt_nulls") > 0)
                .orderBy(col("cnt_nulls").desc())
            )
            return self.df_with_nulls
        else:
            print("no NULL values in selected or all null columns")

    def compare_tables(self, df_ref: DataFrame):
        """Comparing two tables based on `pk` attribute
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
                It is not cached, write to Hive or cache it with filters!
        """
        if not self.pk:
            raise Exception("No PK is provided")
        if self.df is df_ref:
            raise Exception("Two DFs are the same objects, create a new one")

        self.df_ref = df_ref

        for df, name in zip((self.df, self.df_ref), ("Main DF", "Reference DF")):
            if not hasattr(df, "pk_stats"):
                df = DFExtender(df, self.pk, verbose=False)
                df._analyze_pk()
            print(name)
            df._print_pk_stats()
            print()
            # rounding in any numeric columns so they don't considered errors by machine rounding

        self.df, self.df_ref = map(self.__round_numberic_cols_df, [self.df, self.df_ref])

        df1_cols = set(self.df.columns)
        df2_cols = set(self.df_ref.columns)
        self._common_cols = (df1_cols & df2_cols) - set(self.pk)

        self.diff_postfix, self.sum_postfix = "_is_diff", "_sum_error"
        self.columns_diff_postfix = [
            column + self.diff_postfix for column in self._common_cols
        ]

        cols_not_in_main, cols_not_in_ref = df2_cols - df1_cols, df1_cols - df2_cols
        if cols_not_in_main:
            print(f"cols not in main: {cols_not_in_main}")
        if cols_not_in_ref:
            print(f"cols not in ref: {cols_not_in_ref}")

        dummy_column = "is_joined_"
        self.dummy1, self.dummy2 = dummy_column + "main", dummy_column + "ref"

        df1 = self.df.withColumn(self.dummy1, F.lit(1)).alias("main")
        df2 = self.df_ref.withColumn(self.dummy2, F.lit(1)).alias("ref")

        self._df_joined = df1.join(df2, on=self.pk, how="full")

        # diff in non PK cols
        # creates as a new attribute self.df_with_errors
        diff_results_dict = self.__compare_calc_diff()

        # diff in PK cols
        cnt_results = self.__compare_calc_pk()

        # cnt of error / count of all correct matching
        self.dict_cols_with_errors = self._get_sorted_dict(
            diff_results_dict, cnt_results[2]
        )
        # printing results
        if not self._common_cols:
            print("There are no common columns outside of PK")
        elif self.dict_cols_with_errors:
            print(f"Errors in columns - {{'column': [count is_error, share is_error]}}")
            self.__print_dict(self.dict_cols_with_errors, "dict_cols_with_errors")
        else:
            print("There are no errors in non PK columns")

        print("\nCount stats of matching main and reference tables:")
        for key, val in dict(zip(self._cases_full_join.keys(), cnt_results)).items():
            print("{:<25} {:,}".format(key + ":", val))
        self.v_print(f"\nUse DF in attribute `.df_with_errors` for further analysis")

    def __compare_calc_diff(self):
        """Calculating difference in non PK columns"""

        def add_column_is_diff(df, column):
            """Filter for detecting differences in non PK attributes"""
            cond_diff = f"""case
                when
                    ({self.dummy1} is null or {self.dummy2} is null) 
                    or
                    (main.{column} is null and ref.{column} is null)
                    or 
                    (main.{column} = ref.{column})
                then 0
                else 1
            end"""
            return df.withColumn(column + self.diff_postfix, F.expr(cond_diff))

        df_with_errors = reduce(add_column_is_diff, self._common_cols, self._df_joined)

        def put_postfix_columns(column, table):
            """Helps distinguish columns with the same name but different alias of table"""
            return f"{table}.{column} as {column}_{table}"

        self.df_with_errors = df_with_errors.selectExpr(
            *self.pk,
            *map(
                put_postfix_columns,
                self._common_cols,
                ["main"] * len(self._common_cols),
            ),
            *map(
                put_postfix_columns, self._common_cols, ["ref"] * len(self._common_cols)
            ),
            *self.columns_diff_postfix,
            self.dummy1,
            self.dummy2,
        )

        diff_results_dict = {}  # main dict with results
        if self._common_cols:  # Calculate stats of common columns excluding PK
            self.df_with_errors = self.df_with_errors.withColumn(
                "sum_errors",
                reduce(add, [col(column) for column in self.columns_diff_postfix]),
            )

            diff_results = self.df_with_errors.agg(
                *(
                    F.sum(col_is_diff + self.diff_postfix).alias(
                        col_is_diff + self.sum_postfix
                    )
                    for col_is_diff in self._common_cols
                )
            ).collect()[0]

            for column in self._common_cols:
                sum_column = column + self.sum_postfix
                diff_results_dict[column] = diff_results[sum_column]
        else:
            self.v_print(
                "No common columns are found. Results will only contain PK errors"
            )

        return diff_results_dict

    def __compare_calc_pk(self):
        """Calculating difference in PK between 2 tables"""
        df_cnt_pk_errors = (
            self._df_joined.groupBy(self.dummy1, self.dummy2).count().cache()
        )

        self._cases_full_join = {
            # 0
            "not in main table": (
                col(self.dummy1).isNull() & col(self.dummy2).isNotNull()
            ),
            # 1
            "not in reference table": (
                col(self.dummy1).isNotNull() & col(self.dummy2).isNull()
            ),
            # 2
            "correct matching": (
                col(self.dummy1).isNotNull() & col(self.dummy2).isNotNull()
            ),
        }

        cnt_results = []
        for condition in self._cases_full_join.values():
            res = df_cnt_pk_errors.filter(condition).select("count").collect()

            res_int = res[0]["count"] or 0  # 0 if such a case doesn't exist
            cnt_results.append(res_int)

        return cnt_results

    def __check_cols_entry(self, cols_subset, cols_all):
        """
        Raise exception if provided columns are not in DF
        """
        extra_columns = set(cols_subset) - set(cols_all)
        if extra_columns:
            raise ExtraColumnsException(
                f"columns {extra_columns} do not present in the provided cols: {cols_all}"
            )

    def __round_numberic_cols_df(self, df):
        numeric_cols = [
                f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)
            ]
        if numeric_cols:
            for c in df.columns:
                df = df.withColumn(c, F.round(c, SCALE_OF_NUMBER_IN_COMPARING))
        return df


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
            except:  # spark might fail to read a table without a root folder
                self.dict_of_tables[table] = 0

        self._cnt_empty_tables = len(
            [table for table, val in self.dict_of_tables.items() if val == 0]
        )
        perc_empty = round(self._cnt_empty_tables / self._cnt_tables * 100, 2)

        print(
            f"{self._cnt_empty_tables} tables going to be dropped out of {self._cnt_tables} ({perc_empty}%)",
            end="\n",
        )
        print(
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
