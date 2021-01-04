from pyspark import SparkContext
from pyspark.sql import SparkSession, functions
from difflib import ndiff
from time import perf_counter_ns
from python_executor import PythonExecutor


class Chapter2(object):

    def __init__(self):
        self.sc = SparkContext("local", "chapter2_examples")
        self.spark = SparkSession(self.sc)
        self.spark.conf.set("spark.sql.shuffle.partitions", "1000")
        
        self.flight_data_2015 = self.spark.read.option("inferSchema", "true").option("header", "true")\
         .csv("/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv")
        self.table_name = 'flight_data_2015'

    def exercise1(self):
        test_range = self.spark.range(100000).toDF("number")
        test_where = test_range.where("number % 2 == 0 and 10 < number and number < 20")
        print(f"number of the elements: {test_where.count()}")
        test_where.show()

    def exercise2(self):
        self.flight_data_2015.show()
        print(f"take 3: {self.flight_data_2015.take(3)}")

        self.flight_data_2015_sorted = self.flight_data_2015.sort("count", ascending=False)
        self.flight_data_2015_sorted.show()

    def exercise3(self):
        self.flight_data_2015.createOrReplaceTempView(self.table_name)

        # dataframes vs sql
        sql_way = self.spark.sql(f"""
        SELECT DEST_COUNTRY_NAME, count(1)
        FROM {self.table_name}
        GROUP BY DEST_COUNTRY_NAME
        """)
        data_frame_way = self.flight_data_2015.groupBy("DEST_COUNTRY_NAME").count()

        diff = (ndiff(
         sql_way._jdf.queryExecution().toString().splitlines(keepends=True),
         data_frame_way._jdf.queryExecution().toString().splitlines(keepends=True)
        ))
        print(''.join(diff), end="")

        print(f'take 3: {self.flight_data_2015.select(functions.max("count")).take(1)}')

    def exercise4(self):
        sql_start = perf_counter_ns()

        max_sql = self.spark.sql(f"""
        SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM {self.table_name}
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 10
        """)
        max_sql.show()
    
        sql_stop = perf_counter_ns()

        df_start = perf_counter_ns()
    
        self.flight_data_2015\
         .groupBy("DEST_COUNTRY_NAME")\
         .sum("count")\
         .withColumnRenamed("sum(count)", "destination_total")\
         .sort(functions.desc("destination_total"))\
         .limit(10)\
         .show()
    
        df_stop = perf_counter_ns()
    
        print(f'df execution time: {df_stop-df_start} vs sql execution time: {sql_stop-sql_start}')


if __name__ == "__main__":
    PythonExecutor(Chapter2()).run()

