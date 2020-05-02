# Data Modelling with Postgres

## Introduction and Context
This project builds a Postgres database and ETL process that prepares and loads data for the startup Sparkify. Sparkify is a music streaming app, and wants to use the new database to understand what songs users are listening to. Currently Sparkify stores their data in json files and has no easy way to query.

### Project Objectives

1. Setup database with star schema
2. Determine ETL processes
3. Implement ETL pipeline and ingest data into database

### Technologies Used
- postgres database
- python
- pandas

## Database Schema
Below are the schema tables for both fact and dimension tables. The design is a start schema and optimized for analytic queries. In this sense few joins are needed in order to pull insights from the underlying data.

### Fact Tables

Name: songplays
|Column Name | Type|
|---|---|
|songplay_id | int | 
|start_time | TIMESTAMP| 
|user_id | int |
|level | varchar|
|song_id | varchar | 
|artist_id | varchar |
|session_id | int |
|location | varchar |
|user_agent | varchar |

### Dimension Tables

Name:users
|Column Name | Type|
|---|---|
| user_id | int |
| first_name | varchar |
| last_name | varchar |
| gender | varchar |
| level | varchar |

Name: songs
|Column Name | Type|
|---|---|
|song_id | varchar|
|title | varchar | 
|artist_id | varchar | 
|year | int |
|duration | float |

Name: artists
|Column Name | Type|
|---|---|
|artist_id |varchar |
|name | varchar |
|location | varchar |
|latitude | float |
|longitude | float |

Name: time
|Column Name | Type|
|---|---|
|start_time | timestamp |
|hour | int |
|day | int |
|week | int |
|month | int |
|year | int |
|weekday | int |

## ETL Processing

The original data is stored in JSON format and must be converted before being inserted into the database. The main processing steps are:

1. The ts column from the logs file must be converted from a millisecond based timestamp to a datetime.
2. Values for hour, day, month, year, and day of the week must be extracted from the ts datetime value
3. Logs file must be filtered to contain only records of 'NextSong' value
4. Values from song_id and artist_id must be queried in order to complete absent columns in the songplays table.
