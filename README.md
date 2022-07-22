## Project Description Sparkify 

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Scripts explanation etl.py : 
  1. read song_data and load_data from S3
  2. transform data to create five different tables 
  3. and write tables to partitioned parquet files in table directories on S3.

## How to run the Python scripts  
*  In the dl.cfg, enter the Access Key ID and Secret Access Key
*  Run the etl.py script

