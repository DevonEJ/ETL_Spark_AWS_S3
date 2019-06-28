import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, unix_timestamp
from pyspark.sql.types import *

# Read in config file
config = configparser.ConfigParser()
config.read('dl.cfg')

# Get AWS credentials
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Transforms expected input data JSON file into 2 analytics dataframes, and writes them out to the output location
    given.
    
    Parameters:
    -- spark - spark session object
    -- input_data - string ['LOCAL' OR 'REMOTE'] specifying which path to read from config file
    -- output_data - string ['LOCAL' OR 'REMOTE'] specifying which path to read from config file
    """
    # get filepath to song data file
    song_data = config[input_data]['SONG_DATA']
    
    # Set output filepath
    output_location = config[output_data]['OUTPUT_PATH']
    
    # read song data file
    df = spark.read.format('json').load(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_location + "/songs", "songs_table"), "overwrite")

    # Create artists table
    artists_table = df.select(col('artist_name').alias('name')
                               , col('artist_location').alias('location')
                               , col('artist_latitude').alias('latitude')
                               , col('artist_longitude').alias('longitude')).distinct()

    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_location + "/artists", "artists_table"), "overwrite")
    
    # Return songs dataframe, for use in log processing function
    return df



def process_log_data(spark, input_data, output_data, songs_data):
    """
    Transforms expected input data JSON file into 3 analytics dataframes, and writes them out to the output location
    given.
    
    Parameters:
    -- spark - spark session object
    -- input_data - string ['LOCAL' OR 'REMOTE'] specifying which path to read from config file
    -- output_data - string ['LOCAL' OR 'REMOTE'] specifying which path to read from config file
    """
    # get filepath to log data file
    log_data = config[input_data]['LOG_DATA']
    
    # Set output filepath
    output_location = config[output_data]['OUTPUT_PATH']
    
    # read log data file
    log_df = spark.read.format('json').load(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(col('page') == 'NextSong')

    # extract columns for users table    
    users_table = log_df.select(col('userId').alias('user_id')
                           , col('firstName').alias('first_name')
                           , col('lastName').alias('last_name')
                           , col('gender')
                           , col('level')).distinct()
    
    # write users table to parquet files
    users_table.write.partitionBy('gender').parquet(os.path.join(output_location + "/users", "users_table"), "overwrite")

    # create datetime column from original timestamp column
    log_df = log_df.withColumn('timestamp', from_unixtime(col('ts') / 1000)).drop('ts')
        
    # extract columns to create time table
    time_table = log_df.select(date_format('timestamp', 'HH:MM:ss').alias('start_time')
                                    , hour('timestamp').alias('hour')
                                    , dayofmonth('timestamp').alias('day')
                                    , weekofyear('timestamp').alias('week')
                                    , month('timestamp').alias('month')
                                    , year('timestamp').alias('year')
                                    , date_format('timestamp', 'u').alias('weekday'))
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_location + "/time", "time_table"), "overwrite")

    # read in song data to use for songplays table
    song_df = songs_data.distinct()
     

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df, (log_df.song == song_df.title) & (log_df.artist == song_df.artist_name))\
    .withColumn('songplay_id', monotonically_increasing_id())\
    .withColumn('month', month('timestamp'))\
    .select(col('songplay_id')
            , date_format('timestamp', 'HH:MM:ss').alias('start_time')
            , col('userId').alias('user_id')
            , col('level')
            , col('song_id')
            , col('artist_id')
            , col('sessionId').alias('session_id')
            , col('location')
            , col('userAgent').alias('user_agent')
            , col('year')
            , col('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_location + "/songplays", "songplays_table"), "overwrite")
    
                                                               
def main():
    spark = create_spark_session()
    # Specify whether using LOCAL or REMOTE datasets - determines paths used from config file
    input_data = "LOCAL"
    output_data = "LOCAL"
    
    songs_df = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, songs_df)


if __name__ == "__main__":
    main()
