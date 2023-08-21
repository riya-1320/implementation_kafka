from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, broadcast
from pyspark.sql.functions import col, hour


spark = SparkSession \
    .builder \
    .appName("analysis_delta") \
    .config('spark.driver.bindAddress', '127.0.0.1') \
    .master('local[*]') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
deltaTableLocation = '/home/xs354-riygup/TASKS/implementation_kafka/deltaTable'

# Analysis TasK 1: Read from delta table
deltaData = spark.read.format('delta').load(deltaTableLocation)
print('Delta Table Content')
deltaData.show()

# Analysis TasK 2: Calculate number of datapoints per day on distinct signal_ts
numOfDataPoint = deltaData.groupby('signal_date').count()
print('Number of datapoint per day on distinct signal_ts')
numOfDataPoint.show()

# Analysis TasK 3: Average value of all the signals per hour
print('Average value of all the signals per hour')
filteredDf = deltaData.select('signal_ts', 'signals', 'signal_date')

# Convert map column to separate columns
filteredDf2 = filteredDf.select(col("signal_ts"), col('signal_date'),
                                col("signals").getItem("LV_ActivePower_kW").alias("LVActivePower"),
                                col("signals").getItem("Wind_Speed_ms").alias("WindSpeed(m/s)"),
                                col("signals").getItem("Theoretical_Power_Curve_KWh").alias(
                                    "TheoreticalPowerCurve(KWh)"),
                                col("signals").getItem("Wind_Direction").alias("WindDirection(°)")
                                )
# Extract the hour from the timestamp column
groupedDf = filteredDf2.withColumn("hour", hour(col("signal_ts"))).groupBy('signal_date', 'hour')
averageDf = groupedDf.agg({
    "LVActivePower": "avg",
    "WindSpeed(m/s)": "avg",
    "TheoreticalPowerCurve(KWh)": "avg",
    "WindDirection(°)": "avg"})
averageDf.show()

# Analysis TasK 4:Add a column in the dataframe of pt3 named as generation_indicator, and it will have value as given condition
print('Generation_indicator on given condition')

generationDf = (averageDf.withColumn("generation_indicator", when((col('avg(LVActivePower)') < 200), lit("Low"))
                                     .otherwise(
    when((col('avg(LVActivePower)') >= 200) & (col('avg(LVActivePower)') <= 600), lit("Medium"))
    .otherwise(when((col('avg(LVActivePower)') >= 600) & (col('avg(LVActivePower)') <= 1000), lit("High"))
    .otherwise(
            when((col('avg(LVActivePower)') >= 1000), lit("Exceptional")))))))
generationDf.show()

# Analysis task 5,6 create json file with given data and Change signal name of dataframe in step no 4 w.r.t step no 5 by performing broadcast join.
print('change name of signal w.r.t given signal via broadcast join')
jsonPath = '/home/xs354-riygup/TASKS/implementation_kafka/data/data.json'
jsonFile = spark.read.json(jsonPath)
finalDf = generationDf.select(col('signal_date'), col('hour'),
                              col('avg(TheoreticalPowerCurve(KWh))').alias('Theoretical_Power_Curve (KWh)'),
                              col('avg(LVActivePower)').alias('LV ActivePower (kW)'),
                              col('avg(WindDirection(°))').alias('Wind Direction (°)'),
                              col('avg(WindSpeed(m/s))').alias('Wind Speed (m/s)'), col('generation_indicator'))

broadcastJoin = finalDf.join(broadcast(jsonFile)).orderBy("signal_date", "hour")
colMap = {row.sig_name: row.sig_mapping_name for row in
          broadcastJoin.select("sig_name", "sig_mapping_name").distinct().collect()}

newdf = broadcastJoin
for colName in colMap.keys():
    mappedColName = colMap[colName]
    newdf = newdf.withColumnRenamed(colName, mappedColName)

newdf.drop("sig_name", "sig_mapping_name").distinct().show()
