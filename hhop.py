from spark_init import pyspark, spark, col, F
from funs import read_table

class DFExtender(pyspark.sql.dataframe.DataFrame):
    '''
    1. Print table info once or don't print
    2. checks on null in PK
    3. pk duplicates -> new df with transformations
    '''
    def __init__(self, df, pk=None, default_schema_write='default', verbose=False):

        self.pk = pk
        self.df = df

        v_print = lambda *args, **kwargs: print(*args, **kwargs) if verbose else None
        self._print_stats = lambda string, val: print('{:<20} {:,}'.format(string+':', val))

        super().__init__(self.df._jdf, self.df.sql_ctx)
        
        self._introduction_checks()
        
        
    def _introduction_checks(self):
        '''
        Preliminary information 
        '''
        for key in self.pk:
            if key not in self.df.columns:
                raise Exception(f'{key} is not in columns of the chosen table, fix input or add {key} in the table')

    def getInfo(self):
        '''
        Main information about the table using configs
        '''
        cnt_all = self.df.count()
        self.dict_null = {col: self.df.filter(self.df[col].isNull()).count() for col in self.df.columns}
        self.dict_null_ext = {k: [v, round(v/cnt_all, 4)] for k, v in sorted(self.dict_null.items(), key=lambda item: -item[1]) if v > 0}

        self._print_stats('Count all', cnt_all)
        self._analyze_pk()

        print(f"\nNull values in columns - {{'column': [count NULL, share NULL]}}:\n{self.dict_null_ext}")

    def getDFWithNull(self, null_columns=[]):
        # return df with null values
        # what's the priority
        if null_columns or self.dict_null_ext:
            ...
        else:
            print('no NULL values in selected or all columns')

    def _analyze_pk(self):
        for key in self.pk:
            if key in self.dict_null_ext:
                print(f"PK column '{key}' contains empty values, be careful!")

        df_grouped = self.df.groupBy(self.pk)

        cnt_unique_pk = df_grouped.count().count()

        cnt_duplicates = (
            df_grouped
            .count()
            .filter(col('count') > 1)
            .count()
        )
        self._print_stats('Unique PK count', cnt_unique_pk)
        self._print_stats('PK with duplicates', cnt_duplicates)
        # return df with duplicated PK
        # show require window functions I suppose

    def compareTables(self, df_ref, key):
        '''
        write dataset as option
        output -> stats
        '''
        ...


class SchemaManager:
    '''
    Class drops empty tables where there are 0 records or table folder doesn't exist
    '''
    def __init__(self, schema='default'):
        self.schema=schema
        self._cnt_list_tables()
        print(f'{self._cnt_tables} tables in {schema}')
        print(f'run drop_empty_tables() to drop empty tables in {schema}')
        
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