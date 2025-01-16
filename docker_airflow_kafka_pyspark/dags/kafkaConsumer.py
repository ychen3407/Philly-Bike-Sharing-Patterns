import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, ArrayType


def get_spark_conn():
    spark=None
    try:
        spark=SparkSession\
                .builder\
                .appName('kafka_consumer')\
                .config('spark.jars.packages', """org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,
                                                org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell,
                                                com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1""")\
                .getOrCreate()
    except Exception as e:
        # TODO: add log logging.Error
        pass
    return spark

# Write to BigQuery
def write_to_bigquery(batch_df, batch_id):
    # print('batch_id:', batch_id, batch_df.count())
    batch_df.write \
        .format("bigquery") \
        .option("table", "pa_shared_bikes.stations_status") \
        .option("writeMethod", "direct") \
        .option("credentialsFile", service_account_json)\
        .mode("append") \
        .save()

def kafka_realtime(spark, topic, bucket):
    """
    Args:
        spark: spark connection with SparkSession
        topic: kafka topic
    """
    if not spark:
        raise ValueError('spark connection is None')
    
    # schema for raw api data
    schema = StructType([
        StructField("data", StructType([
            StructField("stations", ArrayType(StructType([
                StructField('is_installed', IntegerType(), False),
                StructField('is_renting', IntegerType(), False),
                StructField('is_returning', IntegerType(), False),
                StructField('last_reported', LongType(), False),
                StructField('num_bikes_available', IntegerType(), False),
                StructField('num_bikes_available_types', StructType([
                    StructField('electric', IntegerType(), True),
                    StructField('smart', IntegerType(), True),
                    StructField('classic', IntegerType(), True),
                ])),
                StructField('num_docks_available', IntegerType(), True),
                StructField('station_id', StringType(), False),
            ]), True)),
        ]), True),
        StructField("last_updated", LongType(), True),
        StructField("ttl", IntegerType(), True),
        StructField("version", StringType(), True),
    ])

    # load micro-batch data from kafka
    df=spark.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', 'kafka:9092')\
            .option("checkpointLocation", f"gs://{bucket}/checkpoints")\
            .option('subscribe', topic)\
            .option('startingOffsets', 'latest')\
            .load()
    
    # convert to dataframe
    stations_df=df.selectExpr("CAST(value AS STRING) as value")\
            .select(F.from_json(F.col("value"), schema).alias("value"))\
            .select(F.col('value.data.stations').alias('stations_data'))\
            .withColumn('stations', F.explode('stations_data'))\
            .drop('stations_data')

    # transform data
    batch_df=stations_df.withColumn('station_id', F.regexp_extract('stations.station_id', r"(\d+)", 0))\
        .withColumn('num_docks_available', F.col('stations.num_docks_available'))\
        .withColumn('num_bikes_available', F.col('stations.num_bikes_available'))\
        .withColumn('num_bikes_available_electric', F.col('stations.num_bikes_available_types.electric'))\
        .withColumn('num_bikes_available_smart', F.col('stations.num_bikes_available_types.smart'))\
        .withColumn('num_bikes_available_classic', F.col('stations.num_bikes_available_types.classic'))\
        .withColumn('utc_timestamp', F.from_unixtime('stations.last_reported').cast('timestamp'))\
        .withColumn('is_installed', F.col('stations.is_installed')==1)\
        .withColumn('is_renting', F.col('stations.is_renting')==1)\
        .withColumn('is_returning', F.col('stations.is_returning')==1)\
        .select('utc_timestamp', 'station_id', 'is_returning', 'is_renting', 'is_installed', 'num_docks_available', 
                'num_bikes_available', 'num_bikes_available_electric', 'num_bikes_available_smart', 'num_bikes_available_classic')
    return batch_df


if __name__ == '__main__':

    topic='stations_status'
    project_id=''
    dataset='pa_shared_bikes'
    table_name='stations_status'
    bucket='gcp-de-bikes-project'
    service_account_json='' # Set the path to your service account JSON key file

    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    
    spark=get_spark_conn()

    batch_df=kafka_realtime(spark, topic, bucket)

    query = batch_df.writeStream \
            .foreachBatch(write_to_bigquery) \
            .outputMode("append") \
            .start()

    query.awaitTermination()