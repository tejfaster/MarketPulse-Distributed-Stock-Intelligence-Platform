import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    StructType, StructField,
    StringType,DoubleType,
    LongType,TimestampType
)

# logging basic config

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# kafka config
KAFKA_BROKER = '10.95.208.20:9092'
KAFKA_TOPIC = 'stock-prices'
SPARK_MASTER = 'spark://10.95.208.20:7077'
BRONZE_PATH = '/data/marketpulse/bronze/stocks'
CHECKPOINT_PATH = '/data/marketpulse/checkpoints/bronze'
TRIGGER_SECONDS = '30 seconds'

# Schema
schema = StructType([
    StructField('symbol',       StringType(),True),
    StructField('price',        DoubleType(),True),
    StructField('prev_close',   DoubleType(),True),
    StructField('change',       DoubleType(),True),
    StructField('change_pct',   DoubleType(),True),
    StructField('volume',       LongType(),  True),
    StructField('market_cap',    LongType(),  True),
    StructField('timestamp',    StringType(),True),
    StructField('exchange',     StringType(),True),
])

def create_spark_session():
    return SparkSession.builder \
        .master(SPARK_MASTER).appName("MarketPulse") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.checkpointLocation",
                CHECKPOINT_PATH).getOrCreate()                    


def ingest_to_bronze(spark):
    logger.info("Starting Bronze Layer Ingestion")

    raw_stream = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers",KAFKA_BROKER)\
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets","latest").load()
    
    parsed_stream = raw_stream.select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    ).select("data.*")\
    .withColumn("date",to_date(col("timestamp")))

    query = parsed_stream.writeStream.format("delta") \
        .outputMode("append").partitionBy("date","symbol") \
        .option("path",BRONZE_PATH).option("checkpointLocation",CHECKPOINT_PATH) \
        .trigger(processingTime=TRIGGER_SECONDS).start()
    
    logger.info(f"Broze stream started")
    logger.info(f"Writing to: {BRONZE_PATH}")
    logger.info(f"Trigger: {TRIGGER_SECONDS}")

    query.awaitTermination()

if __name__ == '__main__':
    try:
        spark = create_spark_session()
        ingest_to_bronze(spark)
    except KeyboardInterrupt:
        logger.info("Bronze ingestion stopped")
    finally:
        spark.stop()            