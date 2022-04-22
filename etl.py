from datetime import datetime
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import  year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date
from pyspark.sql.types import TimestampType



def create_spark_session():
    """
    Create a Spark Session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_directory, output_directory):
    """
        Description: Process the song_data and make songs table and artists table.
    
        Parameters:
            spark = spark session
            input_directory = path to song_data json file with metadata
            output_directory = path to dimensional tables stored in parquet format
    """
   
    # Process the 'song' json data
    song_data = input_directory + "song_data/*.json"
   
    df = spark.read.json(song_data).dropDuplicates()

    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_directory + "/Songs/songs_table.parquet")
    
    # Process the 'artist' json data

    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    artists_table.write.mode("overwrite").parquet(output_directory + "/Artists/artist_data.parquet")
    
    # Create a temp table based on songs_table that will be used to determine song plays later
    df.createOrReplaceTempView("song_df_table")
    

def process_log_data(spark, input_directory, output_directory):
    """
        Description: This function loads log_data from and to S3 by extracting both songs and artists tables, processing them and loading back to S3.
        
        Parameters:
            spark = spark session
            input_directory = path to the raw json files
            output_directory = path to the designated output directory     
    """
    
    log_data = input_directory + "log_data/*.json"

    df = spark.read.json(log_data).dropDuplicates()

    df = df.where(df.page == 'NextSong').cache()

    # extract columns for users table    
    users_table =  df.select([df.userId.alias('user_id'), \
                              df.firstName.alias('first_name'), \
                              df.lastName.alias('last_name'), \
                              df.gender, \
                              df.level]).distinct()
    
    users_table.write.mode("overwrite").parquet(output_directory + "/Users/users_data.parquet")

    get_datetime = udf(lambda x : datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("timestamp", get_datetime(col("ts")))
    
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts)) 
    
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))
    
    time_table = df.select(col("start_time"), col("hour"),col("day"), \
    col("week"),col("month"),col("year"),col("weekday")).distinct()
    
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_directory + "/Time/time_data.parquet")


def main():
    """
        The Main Function is responsible for calling 3 different functions to create a spark session, process the Song Data and to process the Log Data.
    """
    
    spark = create_spark_session()
    input_directory = "s3a://udacity-datalake/"
    output_directory = "s3a://udacity-datawarehouse/"
    
    process_song_data(spark, input_directory, output_directory)    
    process_log_data(spark, input_data, output_directory)


if __name__ == "__main__":
    main()
