import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T
from datetime import datetime
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # Read the song data file with define expected json schema and drop duplicates

    schema = StructType([
      StructField('num_songs', IntegerType()),
      StructField('artist_id', StringType()),
      StructField('artist_latitude', DoubleType()),
      StructField('artist_longitude', DoubleType()),
      StructField('artist_location', StringType()),
      StructField('artist_name', StringType()),
      StructField('song_id', StringType()),
      StructField('title', StringType()),
      StructField('duration', DoubleType()),
      StructField('year', IntegerType())
    ])

    df = spark.read.schema(schema).json(song_data)
    df.dropDuplicates(['song_id'])

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")

    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM song_data
    """) 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs", mode="overwrite",partitionBy=("year","artist_id"))

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
        FROM song_data
    """)
    
    # drop duplicates, then write artists table to parquet files
    artists_table.dropDuplicates(['artist_id'])
    artists_table.write.parquet(output_data + "artists", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data= os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file with define expected json schema
    log_schema = StructType([
          StructField('artist', StringType()),
          StructField('auth', StringType()),
          StructField('firstName', StringType()),
          StructField('gender', StringType()),
          StructField('itemInSession', IntegerType()),
          StructField('lastName', StringType()),
          StructField('length', DoubleType()),
          StructField('level', StringType()),
          StructField('location', StringType()),
          StructField('method', StringType()),
          StructField('page', StringType()),
          StructField('registration', DoubleType()),
          StructField('sessionId', IntegerType()),
          StructField('song', StringType()),
          StructField('status', IntegerType()),
          StructField('ts', LongType()),
          StructField('userAgent', StringType()),
          StructField('userId', IntegerType())
      ])
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df.page == "NextSong"]

    # extract columns for users table    
    df.createOrReplaceTempView("log_data")

    users_table = spark.sql("""
        SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level
        FROM log_data
    """)
    
    # drop duplicates, then write users table to parquet files
    users_table.dropDuplicates(['user_id'])
    users_table.write.parquet(output_data + "users", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), T.TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), T.DateType())
    df = df.withColumn("datetime", get_datetime(df.timestamp))
    
    # extract columns to create time table
    df.createOrReplaceTempView("log_data_with_dates")

    time_table = spark.sql("""
        SELECT timestamp as start_time, datetime
        ,EXTRACT(HOUR FROM datetime) as hour
        ,EXTRACT(DAY FROM datetime) as day
        ,EXTRACT(WEEK FROM datetime) as week
        ,EXTRACT(MONTH FROM datetime) as month
        ,EXTRACT(YEAR FROM datetime) as year
        ,EXTRACT(DAYOFWEEK FROM datetime) as weekday
        FROM log_data_with_dates
    """)
    
    # drop duplicates, then write time table to parquet files partitioned by year and month
    time_table.dropDuplicates(['datetime'])
    time_table.write.parquet(output_data + "time", mode="overwrite", partitionBy=("year","month"))

    # read in song data to use for songplays table
    song_df = spark.sql("""
        SELECT song_id, artist_id, artist_name, artist_location
        FROM song_data
    """) 

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("songplays_data")
    
    songplays_table = spark.sql("""
        SELECT datetime as start_time, EXTRACT(YEAR FROM datetime) as year, EXTRACT(MONTH FROM datetime) as month , userId as user_id, level, song_id, artist_id, sessionId as session_id, location, userAgent as user_agent
        FROM ( log_data_with_dates JOIN songplays_data ON artist_name=artist)
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays", mode="overwrite", partitionBy=("year","month"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://adrien-datalake-demo/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
