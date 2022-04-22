# pyspark-aws-s3-datalake
Problem provided by Udacity

## Table of Contents
* [Use Case](#use-case)
* [Requirements](#requirements)
* [Given Information](#given_information)
* [Solution](#solution)
* [Results](#results)
* [Technology and Libraries](#technology-and-libraries)

## Use Case
A new start-up, Sparkify, currently has raw data stored in an Amazon S3 datalake. In order for the Data Analytics team to provide insights to the Business Team from the data, it must first be cleaned, partitioned, and moved. The Data Analytics team has given explicit requirements (see below) as to how they would like the data to be structured, in order for them to use the data effectively.

## Requirements
- The data must be of parquet format
- The data must be placed into 4 tables, with separate requirements for each tables:
  - Artists 
    - must contain the following columns: *artist_id*, *artist_name*, *artist_location*, *artist_latitude*, *artist_longitude*
  - Songs
    - must contain the following columns: *song_id*, *title*, *artist_id*, *year*, *duration*
    - table should partitioned by *year* and *artist_id*
  - Time
    - must contain the following columns: *start_time*, *hour*, *day*, *week*, *month*, *year*, *weekday*
    - table should be partitioned by *year* and *month*
  - Users
    - must contain the following columns: *user_id*, *first_name*, *last_name*, *df.gender*, *df.level*

## Given Information
An example of the raw input data can be found in the **raw_data** folder

## Solution

Build an ETL pipeline that takes json data from S3, loads it into a dataframe, processes the data, then writes it with or without partitions into the designated output folder.


## Results
Fig.1: `output_data` showing `Artists/`, `Songs/`, `Time/` and `Users/` path
![project-datalake-img1](https://user-images.githubusercontent.com/76578061/132271586-575b1511-c80b-4696-a9a4-770c91c44bf7.png)

Fig.2: Sample `artist_data.parquet/` data loaded back into S3
![project-datalake-img2](https://user-images.githubusercontent.com/76578061/132271624-dcebd7ac-ae54-4bb4-b5f1-c327b2f87adb.png)

Fig.3: Sample `songs_table.parquet/` data loaded back into S3
![project-datalake-img3](https://user-images.githubusercontent.com/76578061/132271661-78894259-d4c9-4854-a972-bd7e258a5061.png)

Fig.4: Sample `time_data.parquet/` partitioned by `year` and `month` loaded into S3
![project-datalake-img4](https://user-images.githubusercontent.com/76578061/132271703-bf77fde2-80b9-464c-a162-b6dc7882f0a5.png)

Fig.5: Sample `users_data.parquet/` data loaded back into S3
![project-datalake-img5](https://user-images.githubusercontent.com/76578061/132271732-6e8bb465-b161-425b-8178-fc6901e1f34e.png)

## Technology and Libraries
* Jupyter - version 1.0.9
* pyspark
