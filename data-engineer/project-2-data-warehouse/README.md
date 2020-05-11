# Data Modelling with Postgres

## Introduction and Context
This project builds a Redshift and ETL pipeline for sparkify a muisc streaming service. As their userbase has grown Sparkify needs to scale up their database. Using original log files in JSON format this project builds the schema and ingestion scripts with python and SQL to propigate the new Redshift service.

### Project Objectives

1. Implement star schema in AWS Redshift
2. Stage the data in Redshift
3. Load data from stage into star schema.

### Technologies Used
- AWS Redshift
- python
- Sql

## Database Schema
Below is the entity relation schema for the sparkify databse. Names of the fact and dimension tables are also provided. The design is     a star schema and optimized for analytic queries. In this sense few joins are needed in order to pull insights from the underlying dat    a.

<img src="img/sparkify-er-diagram.png" align="middle">

 ### Table Descrptions

|Table Name | Type|
|---|---|
|songplays | fact |
|users | dimension|
|---|---|
|songplays | fact |
|users | dimension|
|songs | dimension |
|artists | dimension|
|time | dimension |

## ETL Processing

 34 The original data is stored in JSON format and must be converted before being inserted into the database. The main processing steps ar    e:
 35
 36 1. The ts column from the logs file must be converted from a millisecond based timestamp to a datetime.
 37 2. Values for hour, day, month, year, and day of the week must be extracted from the ts datetime value
 38 3. Logs file must be filtered to contain only records of 'NextSong' value
 39 4. Values from song_id and artist_id must be queried in order to complete absent columns in the songplays table.
README.md

