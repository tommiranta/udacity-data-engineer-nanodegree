import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.sql.functions import weekofyear, dayofweek, date_format


conf = configparser.ConfigParser()
conf.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = conf.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = conf.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*')

    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song.select('song_id',
                                 'title',
                                 'artist_id',
                                 'year',
                                 'duration')\
        .dropna('any', subset='song_id')\
        .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id')\
        .parquet(os.path.join(output_data, 'songs'), mode='overwrite')

    # extract columns to create artists table
    artists_table = df_song.select('artist_id',
                                   col('artist_name').alias('name'),
                                   col('artist_location').alias('location'),
                                   col('artist_latitude').alias('latitude'),
                                   col('artist_longitude').alias('longitude'))\
        .dropna('any', subset='artist_id')\
        .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(
        output_data, 'artists'), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*')

    # read log data file
    df_log = spark.read.json(log_data)

    # filter by actions for song plays
    df_log = df_log.where(df_log.page == 'NextSong')

    # extract columns for users table
    users_table = df_log.select(col('userId').alias('user_id'),
                                col('firstName').alias('first_name'),
                                col('lastName').alias('last_name'),
                                'gender',
                                'level'
                                ).drop_duplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(
        output_data, 'users'), mode='overwrite')

    # create timestamp column from original timestamp column
    df_log = df_log.withColumn('timestamp', from_unixtime(df_log.ts/1000))

    # create datetime column from original timestamp column
    df_log = df_log.withColumn('start_time', date_format(
        df_log.timestamp, 'yyyMMddHH').cast(IntegerType()))

    # extract columns to create time table
    time_table = df_log.select('start_time',
                               hour('timestamp').alias('hour'),
                               dayofmonth('timestamp').alias('day'),
                               weekofyear('timestamp').alias('week'),
                               month('timestamp').alias('month'),
                               year('timestamp').alias('year'),
                               dayofweek('timestamp').alias('weekday')
                               ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'time'), mode='overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*')
    df_song = spark.read.json(song_data)

    # extract columns from joined datasets to create songplays table
    cond = [df_log.artist == df_song.artist_name, df_log.song == df_song.title]
    songplays_table = df_log.join(df_song, cond)\
        .select('start_time',
                col('userId').alias('user_id'),
                'level',
                'song_id',
                'artist_id',
                col('sessionId').alias('session_id'),
                'location',
                col('userAgent').alias('user_agent'),
                month('timestamp').alias('month'),
                year('timestamp').alias('year'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'songplays'), mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = conf.get('DIR', 'INPUT')
    output_data = conf.get('DIR', 'OUTPUT')

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
