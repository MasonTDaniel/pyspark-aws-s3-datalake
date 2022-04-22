# pyspark-aws-s3-datalake
Problem provided by Udacity

>The goal of this project is to load json data from S3, process the data into analytics tables using Spark, and load them back into S3 as parquet files
>as a set of partitioned tables in order to allow >the analytics team to draw insights in what songs users are listening to.

## Table of Contents
* [Screenshots](#screenshots)
* [Technology and Libraries](#technology-and-libraries)

## Screenshots
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
* configparser
* pyspark
* datetime
