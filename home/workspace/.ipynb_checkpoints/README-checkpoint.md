# Introduction

This code represents an ETL pipeline for the Sparkify startup, which has decided to move its data storage and analytics to AWS, so that growing datasets can be stored and processing using Spark, in the cloud.

# ETL Pipeline

The purpose of the pipeline is to transform two sets of JSON files stored in Amazon S3 into 5 fact and dimension tables that can be loaded back into Amazon S3 and processed using Spark. The ETL pipeline is written in PySpark.

The JSON files represent both logs from users' use of the Sparkify music app, and also data on the songs and artists available to listen to in the app.

You can run the pipeline by;

1. Adding your AWS credentials to the <code>dl.cfg</code> file
2. Specifying your input and output S3 bucket locations in the <code>dl.cfg</code> file
2. At the terminal, running <code>python etl.py</code> 

# Database Schema Design

The following is the rationale for the creation of the 5 tables that form the new database in Amazon S3.

### songplays Fact Table

This table logs each of the music app events that involve the <code>NextSong</code> action; this indicates that the user is listening to songs in the app, as opposed to viewing their account or upgrading.

The output table is created by joining both the log data JSON file and the <code>songs</code> table described below, on both the artist names and song names columns.

The output table is partitioned in S3 by the <code>year</code> and <code>month</code> column values, to ensure an even distribution of data, and optimal query performance.

### users Dimension Table

This table holds data on the users of the Sparkify music app, including whether or not they have paid or free accounts.

It can be joined to the <code>songplays</code> table using the <code>user_id</code> primary/foreign key column to give more information on the users listening to given songs.

The output table is partitioned in S3 by the <code>gender</code> column, to allow for more even distribution of data.

### artists Dimension Table

This table holds data on the artists who sing the songs that users listen to in the Sparkify app.

It can be joined to the <code>songplays</code> table using the <code>artist_id</code> primary/foreign key column.


### songs Dimension Table

This table holds data on the songs that users of the Sparkify app are listening to.

It can be joined to the <code>songplays</code> table using the <code>song_id</code> primary/foreign key column to give more information on the songs themselves.

The output table is partitioned in S3 by the <code>year</code> and <code>artist_id</code> columns, to allow for more even distribution of data and optimised querying.


### time Dimension Table

This table holds more granular data on the timestamps of the <code>NextSong</code> events held in the <code>songplays</code> table - e.g. broken down into columns for <code>day</code> and <code>month</code>

The output table is partitioned in S3 by the <code>year</code> and <code>month</code> columns, to allow for more even distribution of data and optimised querying.


