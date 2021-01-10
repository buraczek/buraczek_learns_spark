from pyspark import SparkContext
from pyspark.sql import SparkSession, functions, Row
from python_executor import PythonExecutor
from time import sleep
from pyspark.ml import feature, Pipeline, clustering


class Chapter3(object):

    def __init__(self):
        self.sc = SparkContext("local", "chapter3_examples")
        self.spark = SparkSession(self.sc)
        self.spark.conf.set("spark.sql.shuffle.partitions", "200")
        self.static_data_frame = self.spark.read.format("csv").option("inferSchema", "true").option("header", "true") \
            .load("/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

    def hexercise_in_streaming(self):
        static_name = 'retail_data'

        self.static_data_frame.createOrReplaceTempView(static_name)
        static_schema = self.static_data_frame.schema

        self.static_data_frame \
            .selectExpr(
            "CustomerId",
            "(UnitPrice * Quantity) as total_cost",
            "InvoiceDate") \
            .groupBy(
            functions.col("CustomerId"), functions.window(functions.col("InvoiceDate"), "1 day")) \
            .sum("total_cost") \
            .show(5)

        streaming_data_frame = self.spark.readStream \
            .schema(static_schema) \
            .option("maxFilesPerTrigger", 1) \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

        purchase_by_customer_per_hour = streaming_data_frame \
            .selectExpr(
            "CustomerId",
            "(UnitPrice * Quantity) as total_cost",
            "InvoiceDate") \
            .groupBy(functions.col("CustomerId"), functions.window(functions.col("InvoiceDate"), "1 day")) \
            .sum("total_cost")

        stream = purchase_by_customer_per_hour.writeStream \
            .format("memory") \
            .queryName("customer_purchases") \
            .outputMode("complete") \
            .start()

        # check every X sec
        # values in range and sleep need to be adjusted per machine
        for _ in range(0, 6):
            self.spark.sql("""
             SELECT *
             FROM customer_purchases
             ORDER BY `sum(total_cost)` DESC
             """) \
                .show(5)

            sleep(4)

        # very gracefully
        stream.stop()

    def exercise_in_machine_learning(self):
        self.static_data_frame.printSchema()

        prepped_data_frame = self.static_data_frame.na.fill(0). \
            withColumn("day_of_week", functions.date_format(functions.col("InvoiceDate"), "EEEE")).coalesce(5)

        train_data_frame = prepped_data_frame.where("InvoiceDate < '2011-03-01'")
        test_data_frame = prepped_data_frame.where("InvoiceDate >= '2011-03-01'")

        print(f"TRAINING items: {train_data_frame.count()}")
        print(f"TEST DATA items: {test_data_frame.count()}")

        transformation_pipeline = Pipeline().setStages([
            feature.StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index"),
            feature.OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoded"),
            feature.VectorAssembler().setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"]).setOutputCol(
                "features"),
        ])

        fitted_pipeline = transformation_pipeline.fit(train_data_frame)
        transformed_training = fitted_pipeline.transform(train_data_frame)
        # transformed_training.cache()

        kmeans = clustering.KMeans().setK(2).setSeed(2)
        km_model = kmeans.fit(transformed_training)
        print(f"Training cost: {km_model.summary.trainingCost}")

        transformed_test = fitted_pipeline.transform(test_data_frame)
        transformed_test.summary().show()

    def example_rdd_processing(self):
        self.spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF().show()


if __name__ == "__main__":
    PythonExecutor(Chapter3()).run()

