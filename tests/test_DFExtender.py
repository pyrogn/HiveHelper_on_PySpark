import os
import sys
import pytest

# do not know how to make it the Python way
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir, "hhop_lib"))

from pyspark.sql.functions import col
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window as W

custom_spark_params = {
    "app_name": "custom_app_name123",
}


# import pass_spark_config
from pass_spark_config import write_spark_config

write_spark_config(custom_spark_params)

from hhop import (
    DFExtender,
    SchemaManager,
    TablePartitionDescriber,
)  # main classes
from funs import (
    read_table,
    write_table,
    write_read_table,
    union_all,
    deduplicate_df,
)  # useful functions
from spark_init import spark
from exceptions import HhopException


@pytest.mark.skip()
def read_synth_df(table_name, subfolder=None):
    folder_name = os.path.join("synth_data", "tests")
    if subfolder:
        folder_name = os.path.join(folder_name, subfolder)
    df = spark.read.csv(os.path.join(folder_name, table_name), header=True, sep=";")
    return df


@pytest.mark.skip()
def is_exact_dfs(df1, df2, pk: list[str]) -> bool:
    df1_check = DFExtender(df1, pk=pk)
    df1_check.compare_tables(df2)

    is_matching_correct = (
        df1_check.matching_results[0] == df1_check.matching_results[1] == 0
        and df1_check.matching_results[2] != 0
    )
    is_no_errors = len(df1_check.dict_cols_with_errors) == 0

    return is_matching_correct and is_no_errors


def test_start_spark():
    assert spark.sql("select 1 limit 1").collect()[0][0] == 1


@pytest.mark.parametrize(
    "table_name,correct_result",
    [("input1.csv", (5, 5, 0)), (("input2.csv"), (9, 8, 1))],
)
def test_pk_check(table_name, correct_result):
    df = read_synth_df(table_name, "pk_check")
    c = DFExtender(df, pk=["pk1", "pk2"])
    c.get_info(null_stats=False, pk_stats=True)
    assert all([c.pk_stats[i] == correct_result[i] for i in range(3)])
