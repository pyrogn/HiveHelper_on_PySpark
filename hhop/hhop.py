from functools import reduce
from operator import add
from inspect import cleandoc
from typing import List

from pyspark.sql import DataFrame
from spark_init import pyspark, spark
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from funs import read_table, write_table

DICT_PRINT_MAX_LEN = 15


class DFExtender(pyspark.sql.dataframe.DataFrame):
    """_summary_

    Args:
        pyspark (_type_): _description_

    !Class is not tested on complex types as array and struct!
    """

    def __init__(
        self,
        df: DataFrame,
        pk: List[str] = [],
        default_schema_write: str = "default",
        verbose=False,
    ) -> DataFrame:
        """_summary_
        
        Args:
            df (pyspark.sql.dataframe.DataFrame): DataFrame to analyze
            pk ((list, tuple, set), optional): Primary Key of DF. Defaults to None.
            default_schema_write (str, optional): MIGHT BE REDUNDANT. Defaults to 'default'.
            verbose (bool, optional): Choose if you want to receive additional messages. \
                Defaults to False.
        """
        self.pk = pk
        self.df = df
        self.verbose = verbose
        self.default_schema_write = default_schema_write

        self.v_print = (
            lambda *args, **kwargs: print(*args, **kwargs) if verbose else None
        )
        self._print_stats = lambda string, val: print(
            "{:<25} {:,}".format(string + ":", val)
        )

        super().__init__(self.df._jdf, self.df.sql_ctx)

        self._introduction_checks()

        self._get_sorted_dict = lambda dict, val: {
            k: [v, round(v / val, 4)]
            for k, v in sorted(dict.items(), key=lambda item: -item[1])
            if v > 0
        }

    def __print_dict(self, dictionary, attr_name, *args, **kwargs):
        if len(dictionary) <= DICT_PRINT_MAX_LEN:
            print(dictionary)
        else:
            print(f"dictionary is too large ({len(dictionary)} > {DICT_PRINT_MAX_LEN})")
            print(f"You can access the dictionary in the attribute {attr_name}")

    def _introduction_checks(self):
        """_summary_


        Raises:
            Exception: Provide only columns to pk that are present in the DF
        """

        if self.pk:
            for key in self.pk:
                if key not in self.df.columns:
                    raise Exception(
                        f"{key} is not in columns of the chosen table, fix input or add {key} in the table"
                    )

    def get_info(self):
        """_summary_"""
        self._analyze_pk()
        self._print_pk_stats()

        cnt_all = self.pk_stats[0]

        dict_null = (
            self.df.select(
                [F.count(F.when(col(c).isNull(), c)).alias(c) for c in self.df.columns]
            )
            .rdd.collect()[0] # is it fail proof?
            .asDict()
        )
        self.dict_null_in_cols = self._get_sorted_dict(dict_null, cnt_all)

        if self.pk and self.verbose:
            for key in self.pk:
                if key in self.dict_null_in_cols:
                    print(f"PK column '{key}' contains empty values, be careful!")

        print(f"\nNull values in columns - {{'column': [count NULL, share NULL]}}:")
        self.__print_dict(self.dict_null_in_cols, "dict_null_in_cols")

    def get_df_with_null(self, null_columns=[]):
        """_summary_

            This method calculate and return DF with selected cols that have NULL values

        Args:
            null_columns (list, optional): Columns. Defaults to [].

        Raises:
            Exception: Provide only columns to null_columns that are present in the DF

        Attrs:
            dict_null_in_cols
            df_with_nulls

        Returns:
            pyspark.sql.dataframe.DataFrame:

            Returns a DF sorted by count of nulls
            in selected columns
            in descending order
        """
        if not hasattr(self, "dict_null_in_cols"):
            print("Running method .get_info() first\n")
            self.get_info()

        extra_columns = set(null_columns) - set(
            self.df.columns
        )  # what if this is very long?
        if extra_columns:
            raise Exception(
                f"columns {extra_columns} do not present in the provided DF"
            )
        if self.dict_null_in_cols:
            if set(null_columns) & set(self.dict_null_in_cols):
                cols_filter = null_columns
            else:
                print(
                    f"No NULL values found in provided {null_columns}, using all: {self.dict_null_ext.keys()}"
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

    def _analyze_pk(self):
        cnt_all = self.df.count()

        cnt_unique_pk = cnt_with_duplicates_pk = 0
        if self.pk:
            df_grouped = self.df.groupBy(self.pk)

            cnt_unique_pk = df_grouped.count().count()

            cnt_with_duplicates_pk = df_grouped.count().filter(col("count") > 1).count()

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
        self._print_stats("Count all", self.pk_stats[0])
        if self.pk:
            self._print_stats("Unique PK count", self.pk_stats[1])
            self._print_stats("PK with duplicates", self.pk_stats[2])

    def compare_tables(self, df_ref):
        """_summary_

        Args:
            df_ref (pyspark.sql.dataframe.DataFrame): _description_

        Raises:
            Exception: _description_
            Exception: _description_

        Returns:
            _type_: _description_
        """
        if not self.pk:
            raise Exception("No PK have been provided")
        if self.df is df_ref:
            raise Exception("Two DFs are the same object, create a new one")

        key = self.pk
        stats_list = []
        for df, name in zip((self.df, df_ref), ("Main DF", "Reference DF")):
            if not hasattr(df, "pk_stats"):
                df = DFExtender(df, pk=key, verbose=False)
                df._analyze_pk()
            stats_list.append(df.pk_stats)
            print(name)
            df._print_pk_stats()
            print()

        df1_cols = set(self.df.columns)
        df2_cols = set(df_ref.columns)
        common_cols = (df1_cols & df2_cols) - set(key)

        diff_postfix = "_is_diff"
        columns_diff_postfix = [column + diff_postfix for column in common_cols]
        sum_postfix = "_sum_error"

        cols_not_in_main, cols_not_in_ref = df2_cols - df1_cols, df1_cols - df2_cols
        if cols_not_in_main:
            print(f"cols not in main: {cols_not_in_main}")
        if cols_not_in_ref:
            print(f"cols not in ref: {cols_not_in_ref}")

        dummy_column = "is_joined_"
        dummy1, dummy2 = dummy_column + "main", dummy_column + "ref"

        df1 = self.df.withColumn(dummy1, F.lit(1)).alias("main")
        df2 = df_ref.withColumn(dummy2, F.lit(1)).alias("ref")

        df_joined = df1.join(df2, on=key, how="full")

        def add_column_is_diff(df, col):
            cond_diff = f"""case
            when
                ({dummy1} is null or {dummy2} is null) 
                or
                (main.{col} is null and ref.{col} is null)
                or 
                (main.{col} = ref.{col})
                then 0
                else 1
            end"""
            return df.withColumn(col + diff_postfix, F.expr(cond_diff))

        df_temp = reduce(add_column_is_diff, common_cols, df_joined)

        def put_postfix_columns(column, table):
            return f"{table}.{column} as {column}_{table}"

        self.df_with_errors = df_temp.selectExpr(
            *key,
            *map(put_postfix_columns, common_cols, ["main"] * len(common_cols)),
            *map(put_postfix_columns, common_cols, ["ref"] * len(common_cols)),
            *columns_diff_postfix,
            dummy1,
            dummy2
            # something else?
        )

        diff_results_dict = {}
        if common_cols:  # Calculate stats of common column without PK
            self.df_with_errors = self.df_with_errors.withColumn(
                "sum_errors",
                reduce(add, [col(column) for column in columns_diff_postfix]),
            )

            self.df_with_errors = DFExtender(self.df_with_errors, pk=key)
            # need to make protection if only PK is given
            diff_results = df_temp.agg(
                *(
                    F.sum(col_is_diff + diff_postfix).alias(col_is_diff + sum_postfix)
                    for col_is_diff in common_cols
                )
            ).collect()[0]

            for column in common_cols:
                sum_column = column + sum_postfix
                diff_results_dict[column] = diff_results[sum_column]

        # 2 part
        k = df_temp.groupBy(dummy1, dummy2).count().cache()

        cases_full_join = {
            # 0
            "not in main table": (col(dummy1).isNull() & col(dummy2).isNotNull()),
            # 1
            "not in reference table": col(dummy1).isNotNull() & col(dummy2).isNull(),
            # 2
            "correct matching": col(dummy1).isNotNull() & col(dummy2).isNotNull(),
        }

        cnt_results = []
        for key, value in cases_full_join.items():
            res = k.filter(value).select("count").collect()
            res_int = 0
            if res:
                res_int = res[0]["count"]
            cnt_results.append(res_int)

        dict_print_errors = self._get_sorted_dict(diff_results_dict, cnt_results[2])
        if not common_cols:
            print("There are no common columns outside of PK\n")
        elif dict_print_errors:
            self.dict_cols_with_errors = dict_print_errors
            print(f"Errors in columns - {{'column': [count is_error, share is_error]}}")
            self.__print_dict(dict_print_errors, "dict_cols_with_errors")
        else:
            print("There are no errors in non PK columns\n")

        print("\nCount stats of matching main and reference tables:")
        for key, val in dict(zip(cases_full_join.keys(), cnt_results)).items():
            print("{:<25} {:,}".format(key + ":", val))

    def __compare_calc_diff(self):
        ...

    def __compare_calc_pk(self):
        ...

    def write_table(self, table_name):
        """may be redundant"""
        postfix = "_check_detail"
        table_name += postfix
        write_table(self, schema=self.default_schema_write, table=table_name)
        print(f"written table {self.default_schema_write}.{table_name}")


class SchemaManager:

    """
    Class drops empty tables where there are 0 records or table folder doesn't exist
    """

    def __init__(self, schema: str = "default"):
        self.schema = schema
        self._cnt_list_tables()
        print(f"{self._cnt_tables} tables in {schema}")
        print(
            f"run find_empty_tables() on instance to find empty tables in {self.schema}"
        )

    def _cnt_list_tables(self):
        self._list_of_tables = (
            spark.sql(f"show tables in {self.schema}")
            .select("tableName")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        self._cnt_tables = len(self._list_of_tables)
        self.dict_of_tables = dict.fromkeys(self._list_of_tables, 1)

    def find_empty_tables(self):
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
            f"{self._cnt_empty_tables} going to be dropped out of {self._cnt_tables} ({perc_empty}%)",
            end="\n",
        )
        print(
            "Data about tables stored in attribute '.dict_of_tables':",
            "\n",
            "1 - has data, 0 - doesn't and going to be deleted",
            end="\n\n",
            sep="",
        )

        print(
            f"run drop_empty_tables() on instance to drop empty tables in {self.schema}"
        )

    def drop_empty_tables(self):
        """Drops empty tables in a selected schema
        Use this with caution and check if the attribute .dict_of_tables has some non empty tables"""

        for table, val in self.dict_of_tables.items():
            if val == 0:
                spark.sql(f"drop table if exists {self.schema}.{table}")

        self._cnt_list_tables()

        print(
            f"After dropping tables there are {self._cnt_tables} tables in {self.schema}"
        )
