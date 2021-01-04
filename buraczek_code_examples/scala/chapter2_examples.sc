import org.apache.spark.sql.SparkSession

def main(): {
        val spark = SparkSession
          .builder()
          .master("local")
          .appName("chapter2_examples")
          .getOrCreate()

        val test_range = spark.range(100000).toDF("number")
        val test_where = test_range.where("number % 2 == 0")
        test_where.count()

        }