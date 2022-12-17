from spark_init import pyspark, spark, col, F
from funs import read_table
from functools import reduce
from operator import add

class DFExtender(pyspark.sql.dataframe.DataFrame):
    '''
    1. Print table info once or don't print
    2. checks on null in PK
    3. pk duplicates -> new df with transformations
    '''
    def __init__(self, df, pk=None, default_schema_write='default', verbose=False):

        self.pk = pk
        self.df = df
        self.verbose = verbose

        v_print = lambda *args, **kwargs: print(*args, **kwargs) if verbose else None
        self._print_stats = lambda string, val: print('{:<25} {:,}'.format(string+':', val))

        super().__init__(self.df._jdf, self.df.sql_ctx)
        
        self._introduction_checks()

        self._print_sorted_dict = lambda dict, val: {k: [v, round(v/val, 4)] for k, v in sorted(dict.items(), key=lambda item: -item[1]) if v > 0}
        
        
    def _introduction_checks(self):
        '''
        Preliminary information 
        '''
        if self.pk:
            for key in self.pk:
                if key not in self.df.columns:
                    raise Exception(f'{key} is not in columns of the chosen table, fix input or add {key} in the table')

    def getInfo(self):
        '''
        Main information about the table using configs
        '''

        self._analyze_pk()
        self._print_pk_stats()

        cnt_all = self.pk_stats[0]
        
        self.dict_null = {col: self.df.filter(self.df[col].isNull()).count() for col in self.df.columns}
        self.dict_null_ext = {k: [v, round(v/cnt_all, 4)] for k, v in sorted(self.dict_null.items(), key=lambda item: -item[1]) if v > 0}
        if self.pk and self.verbose:
            for key in self.pk:
                if key in self.dict_null_ext:
                    self.v_print(f"PK column '{key}' contains empty values, be careful!")
        
        print(f"\nNull values in columns - {{'column': [count NULL, share NULL]}}:\n{self.dict_null_ext}")

    def getDFWithNull(self, null_columns=[]):
        # return df with null values
        # what's the priority
        if null_columns or self.dict_null_ext:
            ...
        else:
            print('no NULL values in selected or all columns')

    def _analyze_pk(self):
        
        cnt_all = self.df.count()

        df_grouped = self.df.groupBy(self.pk)

        cnt_unique_pk = df_grouped.count().count()

        cnt_with_duplicates_pk = (
            df_grouped
            .count()
            .filter(col('count') > 1)
            .count()
        )
        # 0 - cnt rows, 1 - Unique PK, 2 - PK with duplicates
        self.pk_stats = [cnt_all, cnt_unique_pk, cnt_with_duplicates_pk]
        
        # return df with duplicated PK
        # show require window functions I suppose

    def _print_pk_stats(self):
        self._print_stats('Count all', self.pk_stats[0])
        self._print_stats('Unique PK count', self.pk_stats[1])
        self._print_stats('PK with duplicates', self.pk_stats[2])

    def compareTables(self, df_ref):
        '''
        write dataset as option
        output -> stats
        '''
        if not self.pk:
            raise Exception('No PK have been provided')
        if self.df is df_ref:
            raise Exception('Two DFs are the same object, create a new one')

        key = self.pk
        stats_list = []
        for df, name in zip((self.df, df_ref), ('Main DF', 'Reference DF')):
            if not hasattr(df, 'pk_stats'):
                df = DFExtender(df, pk=key, verbose=False)
                df._analyze_pk()
            stats_list.append(df.pk_stats)
            print(name)
            df._print_pk_stats()
            print()

        df1_cols = set(self.df.columns)
        df2_cols = set(df_ref.columns)
        common_cols = (df1_cols & df2_cols) - set(key)

        diff_postfix = '_is_diff'
        sum_postfix = '_sum_error'

        cols_not_in_main, cols_not_in_ref = df2_cols - df1_cols, df1_cols - df2_cols
        if cols_not_in_main: print(f'cols not in main: {cols_not_in_main}')
        if cols_not_in_ref: print(f'cols not in ref: {cols_not_in_ref}')

        dummy_column = 'hhop_const_value_column'
        dummy1, dummy2 = dummy_column + '1', dummy_column + '2'

        df1 = self.df.withColumn(dummy1, F.lit(1)).alias('main')
        df2 = df_ref.withColumn(dummy2, F.lit(1)).alias('ref')

        df_joined = df1.join(df2, on=key, how='full')

        def add_column_is_diff(df, col):
            cond_diff = f"""case
            when
                main.{dummy1} is not null and ref.{dummy2} is not null and main.{col} != ref.{col}
                then 1
                else 0
            end"""
            return df.withColumn(col+diff_postfix, F.expr(cond_diff))

        df_temp = reduce(add_column_is_diff, common_cols, df_joined)

        diff_results = (
            df_temp
            .agg(
                *(
                    F.sum(col_is_diff + diff_postfix).alias(col_is_diff + sum_postfix)
                for col_is_diff in common_cols
                )
            )
            .collect()[0]
        )

        diff_results_dict = {}

        for column in common_cols:
            sum_column = column + sum_postfix
            diff_results_dict[column] = diff_results[sum_column]

        # 2 part
        k = df_temp.groupBy(dummy1, dummy2).count().cache()

        cases_full_join = {
            'not in main table': (col(dummy1).isNull() & col(dummy2).isNotNull()), #0
            'not in reference table': col(dummy1).isNotNull() & col(dummy2).isNull(), #1
            'correct matching': col(dummy1).isNotNull() & col(dummy2).isNotNull(), #2
        }

        cnt_results = []
        for key, value in cases_full_join.items():
            res = k.filter(value).select('count').collect()
            res_int = 0
            if res:
                res_int = res[0]['count']
            cnt_results.append(res_int)

        dict_print_errors = self._print_sorted_dict(diff_results_dict, cnt_results[2])
        if dict_print_errors:
            print(f"Errors in columns - {{'column': [count is_error, share is_error]}}\n{dict_print_errors}\n")
        else:
            print('There are no errors in non PK columns\n')

        for key, val in dict(zip(cases_full_join.keys(), cnt_results)).items():
            print('{:<25} {:,}'.format(key+':', val))


class SchemaManager:
    '''
    Class drops empty tables where there are 0 records or table folder doesn't exist
    '''
    def __init__(self, schema='default'):
        self.schema=schema
        self._cnt_list_tables()
        print(f'{self._cnt_tables} tables in {schema}')
        print(f'run drop_empty_tables() on instance to drop empty tables in {schema}')
        
    def _cnt_list_tables(self):
        self._list_of_tables = (
            spark.sql(f"show tables in {self.schema}")
            .select('tableName')
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        self._cnt_tables = len(self._list_of_tables)
        self._dict_of_tables = dict.fromkeys(self._list_of_tables, 1)
    
    def _find_empty_tables(self):
        for table in self._dict_of_tables:
            schema_name = self.schema + '.' + table
            try:
                slice = read_table(schema_name).take(2)
                if len(slice) == 0:
                    self._dict_of_tables[table] = 0
            except:
                self._dict_of_tables[table] = 0

    def drop_empty_tables(self):

        self._find_empty_tables()
        self._cnt_empty_tables = len([table for table, val in self._dict_of_tables.items() if val == 0])
        perc_empty = round(self._cnt_empty_tables / self._cnt_tables * 100, 2)

        print(f'{self._cnt_empty_tables} going to be dropped out of {self._cnt_tables} ({perc_empty}%). Schema: {self.schema}')

        for table, val in self._dict_of_tables.items():
            if val == 0:
                spark.sql(f"drop table if exists {self.schema}.{table}")

        self._cnt_list_tables()

        print(f'After dropping tables there are {self._cnt_tables} tables in {self.schema}')