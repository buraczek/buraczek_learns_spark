
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

val flightsDF = spark.read.format("parquet").load("/Spark-The-Definitive-Guide/data/flight-data/parquet/*")
val flights = flightsDF.as[Flight]

flights.filter(flight_row=>flight_row.ORIGIN_COUNTRY_NAME!="Canada").map(flight_row=>flight_row).take(5)

flights.take(5).filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada").map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))


