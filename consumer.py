from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, substring, to_timestamp, concat, lit, \
    current_date, current_timestamp, from_json, create_map
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("deltaTable") \
    .config('spark.driver.bindAddress', '127.0.0.1') \
    .master('local[*]') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

checkPointLocation = '/home/xs354-riygup/TASKS/implementation_kafka/checkPoint/checkPointConsume'
deltaTableLocation = '/home/xs354-riygup/TASKS/implementation_kafka/deltaTable'

preDefinedSchema = (
    StructType()
    .add("Date/Time", StringType())
    .add('LV ActivePower (kW)', DoubleType())
    .add('Wind Speed (m/s)', DoubleType())
    .add('Theoretical_Power_Curve (KWh)', DoubleType())
    .add('Wind Direction (°)', DoubleType())
)

# write csvDf into kafka in streaming fashion
writeDf = spark \
    .readStream \
    .format("kafka") \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'hello') \
    .load()

# transformed schema
convertString = writeDf \
    .selectExpr('CAST(value As STRING)')

dataFrame = convertString \
    .select(from_json(col('value'), preDefinedSchema).alias('data')) \
    .select('data.*')
# dataFrame.writeStream.format('console').start().awaitTermination()

ConvertDateAndTime = dataFrame \
    .withColumn("date", to_date(col("Date/Time"), "dd MM yyyy HH:mm")) \
    .withColumn("time", substring(col("Date/Time"), 12, 5)) \
    .drop("Date/Time")

addTimeAndDateCol = ConvertDateAndTime \
    .withColumn("signal_date", col("date")) \
    .withColumn("signal_ts", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm")) \
    .drop("date", "time")

deltaDf = addTimeAndDateCol \
    .select(col("signal_date"), col("signal_ts"),
            create_map(lit("LV_ActivePower_kW"), col("LV ActivePower (kW)").cast(DoubleType()),
                       lit("Wind_Speed_ms"), col("Wind Speed (m/s)").cast(DoubleType()),
                       lit("Theoretical_Power_Curve_KWh"), col("Theoretical_Power_Curve (KWh)").cast(DoubleType()),
                       lit("Wind_Direction"), col("Wind Direction (°)").cast(DoubleType())).alias("signals"),
            current_date().alias("create_date"), current_timestamp().alias("create_ts")).alias("data")
finalDf = deltaDf.dropna()

# finalDf.writeStream.format('console').start().awaitTermination()
# write csv data into deltaTable in defined schema
finalDf.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("delta.columnMapping.mode", "name") \
    .option("checkpointLocation", checkPointLocation) \
    .start(deltaTableLocation) \
    .awaitTermination()
