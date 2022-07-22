import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

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
        - Create date : 2022-06-01
        - Read song_data json into a dataframe
        - From dataframe, Extract data into songs, artist table 
        - Write all dataframe into parquet files
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    dfsong = spark.read.json(song_data)

    # extract columns to create songs table
    # 3.songs - songs in music database: song_id, title, artist_id, year, duration
    songs_table = dfsong.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(os.path.join(output_data, 'songs_table/songs.parquet'))

    # extract columns to create artists table
    # 4.artists - artists in music database: artist_id, name, location, lattitude, longitude
    artists_table = dfsong.select(dfsong['artist_id'], dfsong['artist_name'].alias('name'), dfsong['artist_location'].alias('location')\
                                  , dfsong['artist_latitude'].alias('latitude'), dfsong['artist_longitude'].alias('longitude')).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists_table/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
        - Create date : 2022-06-01
        - Read log_data json into a dataframe
        - From dataframe, Extract data into users, songplays table 
        - Write all dataframe into parquet files
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    dflog = spark.read.json(log_data)
    
    # filter by actions for song plays
    dflog = dflog.where(dflog.page == 'NextSong')

    # extract columns for users table    
    # 2.users - users in the app: user_id, first_name, last_name, gender, level
    users_table = dflog.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users_table/users.parquet'), 'overwrite')
    
    # create timestamp column from original timestamp column
    # I had error "NameError: name 'F' is not defined", so I change "F.to_timestamp" =>
    # I had error "ValueError: year 50806 is out of range" before
    #get_timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x))))
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    dflog = dflog.withColumn("start_time", get_timestamp("ts").cast("timestamp"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    dflog = dflog.withColumn('datetime', get_datetime(dflog.ts))


    # extract columns to create time table
    # 5.time - timestamps of records in songplays broken down into specific units: 
    # start_time, hour, day, week, month, year, weekday
    time_table = dflog.select('datetime') \
    .withColumn('start_time', dflog.datetime) \
    .withColumn('hour', hour('datetime')) \
    .withColumn('day', dayofmonth('datetime')) \
    .withColumn('week', weekofyear('datetime')) \
    .withColumn('month', month('datetime')) \
    .withColumn('year', year('datetime')) \
    .withColumn('weekday', dayofweek('datetime')) \
    .dropDuplicates()    
    # write time table to parquet files partitioned by year and month
    # I had this issue before: "Py4JJavaError: An error occurred while calling o1446.parquet.: org.apache.spark.SparkException: Job aborted."
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data+ 'time_table/time.parquet')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    # 1.songplays - records in log data associated with song plays i.e. records with page NextSong
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent    
    # I had this issue "TypeError: <lambda>() missing 1 required positional argument: 'x'" for this function F.monotonically_increasing_id()
    # create 2 temp view for songs and log, then inner join 2 table with condition

    song_df.createOrReplaceTempView('songs') 
    dflog = dflog.withColumn('songplay_id', monotonically_increasing_id())
    dflog.createOrReplaceTempView('logs')

    # extract columns from joined song and log datasets to create songplays table 
    # I had this issue before "ValueError: year 50806 is out of range" => so I changed to user spark.sql
    songplays_table =  spark.sql("""
                            SELECT
                                l.songplay_id,
                                l.datetime as start_time,
                                year(l.datetime) as year,
                                month(l.datetime) as month,
                                l.userId as user_id,
                                l.level,
                                s.song_id,
                                s.artist_id,
                                l.sessionId as session_id,
                                l.location,
                                l.userAgent as user_agent
                            FROM logs l
                            LEFT JOIN songs s ON
                                l.song = s.title AND
                                l.artist = s.artist_name 
                            """)


    # write songplays table to parquet files partitioned by year and month

    songplays_table = songplays_table.write.parquet('./output/'+'./songplays_table/', partitionBy=["year","month"], mode='overwrite')

def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3a://luonglqtest/"
    
    input_data = "./data/"
    output_data = "./output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
