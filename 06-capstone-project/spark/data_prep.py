"""
Spark Job for preparing Taxi trips data for data analysis

The script reads all trips data from S3 bucket and makes required
data transformations before storing it back to S3 in Parquet format
partitioned by year, month, day.

Note:
Modify the scripts `output_conf` and `etl_conf` dictionaries values
to correspond to your S3 bucket

This script was created with minimal effort from a Jupyter Notebook
where I did initial development. As a result, the structure of this
script is not the best, but sufficient taking into account the simple
nature of this demonstration for a hypothetical scenario.
"""


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.ml.feature import StringIndexer

output_conf = {
    's3_bucket': 's3://your-bucket',
    's3_model_key': 'taxi_ml/model',
    's3_data_key': 'taxi_ml/data'
}

etl_conf = {
    "s3_taxi_dir_path": "s3://your-bucket/chicago-taxi-rides-2016",
    "s3_precip_file_path": "s3://your-bucket/ghcnd/2016",
    "s3_weather_dir_path": "s3://your-bucket/historical-hourly-weather-data",
    "s3_holidays_file_path": "s3://your-bucket/US-Bank-holidays.csv"
}


def main():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()

    # use for only one file
    # filename = 'chicago_taxi_trips_2016_01.csv'

    # use for reading all files
    filename = '*'

    df = spark.read \
        .format('csv') \
        .options(header=True, inferSchema=True) \
        .load(os.path.join(etl_conf['s3_taxi_dir_path'], filename))
    # df.printSchema()

    # Take a look at the top rows
    # df.limit(5).toPandas()

    # Check initial number of records
    # df.count()

    df_with_hour = df.withColumn('year', year(df.trip_start_timestamp))\
                     .withColumn('month', month(df.trip_start_timestamp))\
                     .withColumn('day', dayofmonth(df.trip_start_timestamp))\
                     .withColumn('hour', hour(df.trip_start_timestamp))

    df_features = df_with_hour.select('year',
                                      'month',
                                      'day',
                                      'hour',
                                      'pickup_community_area',
                                      'dropoff_community_area')

    df_no_nulls = df_features.dropna()

    # df_no_nulls.count()

    # Create StringIndexer and fit + transform pickup data
    pickup_indexer = StringIndexer(
        inputCol='pickup_community_area',
        outputCol='pickup_community_area_indexed'
    )

    pickup_indexer_model = pickup_indexer.fit(df_no_nulls)
    df_pickup_indexed = pickup_indexer_model.transform(df_no_nulls)

    # Create StringIndexer and fit + transform dropoff data
    dropoff_indexer = StringIndexer(
        inputCol='dropoff_community_area',
        outputCol='dropoff_community_area_indexed'
    )

    dropoff_indexer_model = dropoff_indexer.fit(df_pickup_indexed)
    df_dropoff_indexed = dropoff_indexer_model.transform(df_pickup_indexed)

    # Create OneHotEncoder and fit + transform pickup & dropoff data
    encoder = OneHotEncoderEstimator() \
        .setInputCols(['hour',
                       'pickup_community_area_indexed',
                       'dropoff_community_area_indexed']) \
        .setOutputCols(['hour_encoded',
                        'pickup_community_area_encoded',
                        'dropoff_community_area_encoded'])

    encoder_model = encoder.fit(df_dropoff_indexed)
    df_encoded = encoder_model.transform(df_dropoff_indexed)

    # df_encoded.printSchema()

    bucket = output_conf['s3_bucket']
    key = output_conf['s3_model_key']

    # save the pickup stringINdexer and model
    pickup_indexer_name = 'pickup_indexer_name'
    pickup_indexer_path = os.path.join(bucket, key, pickup_indexer_name)
    pickup_indexer.write().overwrite().save(pickup_indexer_path)

    pickup_indexer_model_name = 'pickup_indexer_model_name'
    pickup_indexer_model_name_path = os.path.join(bucket,
                                                  key,
                                                  pickup_indexer_model_name)
    pickup_indexer_model \
        .write() \
        .overwrite() \
        .save(pickup_indexer_model_name_path)

    # save the dropoff stringINdexer and model
    dropoff_indexer_name = 'dropoff_indexer_name'
    dropoff_indexer_path = os.path.join(bucket, key, dropoff_indexer_name)
    dropoff_indexer.write().overwrite().save(dropoff_indexer_path)

    dropoff_indexer_model_name = 'dropoff_indexer_model_name'
    dropoff_indexer_model_name_path = os.path.join(bucket,
                                                   key,
                                                   dropoff_indexer_model_name)
    dropoff_indexer_model \
        .write() \
        .overwrite() \
        .save(dropoff_indexer_model_name_path)

    # save the one-hot encoder and model
    encoder_name = 'encoder_name'
    encoder_name_path = os.path.join(bucket, key, encoder_name)
    encoder.write().overwrite().save(encoder_name_path)

    encoder_model_name = 'encoder_model_name'
    encoder_model_name_path = os.path.join(bucket, key, encoder_model_name)
    encoder_model.write().overwrite().save(encoder_model_name_path)

    # make final dataframe and store back to S3
    df_final = df_encoded.select('year',
                                 'month',
                                 'day',
                                 'hour_encoded',
                                 'pickup_community_area_encoded',
                                 'dropoff_community_area_encoded'
                                 )

    bucket = output_conf['s3_bucket']
    key = output_conf['s3_data_key']

    output_path = os.path.join(bucket, key)

    df_final.write.partitionBy('year', 'month', 'day') \
            .parquet(output_path, mode='overwrite')


if __name__ == "__main__":
    main()
