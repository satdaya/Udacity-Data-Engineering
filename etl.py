import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, from_unixtime, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Pulls the song data from S3. Creates a temp view of entire song dataset (to be used in a coming function for the fact table creation). Creates two tables (songs_table and artists_table), loads data into the respective tables, and writes the data back into S3 in Parquet format.
    
    Parameters:
        spark: Spark Session
        input_data: location of song data json files
        output_data: S3 buckets
    """
    # get filepath to song data filesongplay_id
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView('song_stage')

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    #songs_table = songs_table.dropDuplicates('song_id')
    
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data + 'songs_table'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location' , 'artist_longitude', 'artist_latitude']).dropDuplicates()
    #artists_table = artists_table.dropDuplicates('artist_id')
    
    artists_table.createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data + 'artists_table' ), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Pulls the log data from S3. Creates two tables (users_table and time_table), loads data into the respective tables, and writes the data back into S3 in Parquet format. Creates a dataframe for the log data, adds two time fields using withColumn. A SparkSQL query creates from the newly created dataframe and the temp view from the process_song_funnction.
    
    Parameters:
        spark: Spark Session
        input_data: location of log data json files
        output_data: S3 buckets
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/2018-11-01-events.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    songplays_df = df.filter(df.page == 'NextSong').select(['ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']).dropDuplicates()

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    #users_table = users_table.dropDuplicates('user_id')
    
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data + 'users_table'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    songplays_df = songplays_df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    songplays_df = songplays_df.withColumn('datetime', from_unixtime('start_time'))
    
    songplays_df.createOrReplaceTempView('event_stage')
    
    # extract columns to create time table
    time_table = songplays_df.select('datetime') \
                                .withColumn('start_time', songplays_df.datetime) \
                                .withColumn('hour', hour ('datetime')) \
                                .withColumn('day', dayofmonth ('datetime')) \
                                .withColumn('week', weekofyear ('datetime')) \
                                .withColumn('weekday', dayofweek ('datetime')) \
                                .withColumn('month', month ('datetime')) \
                                .withColumn('year', year ('datetime'))
    time_table = time_table.dropDuplicates()

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data + 'time_table'), 'overwrite')
    
    time_table.createOrReplaceTempView('play_time')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+ 'song_data/A/A/A/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
                                SELECT es.start_time,
                                       es.userId AS user_id,
                                       es.level,
                                       ss.song_id,
                                       ss.artist_id,
                                       es.sessionId AS session_id,
                                       es.location,
                                       es.userAgent,
                                       t.year,
                                       t.month
                                  FROM event_stage es
                                  JOIN song_stage ss
                                    ON es.song = ss.title
                                   AND es.artist = ss.artist_name
                                  JOIN play_time t
                                    ON es.start_time = t.start_time
                                       """
                                  ).withColumn('songplay_id', monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays_table'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

