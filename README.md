# Sparkify Data Warehouse - Redshift
Repository used for Data Warehouse Modeling with Redshift project.

## Introduction 

This project contains an ETL pipeline that extracts Sparkify data from S3, stages them in Redshift, 
and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

Also attached to the project are example scripts needed to create and delete necessary AWS resources. 

This is a learning project and not a real-world application. Use with caution. Sparkify does not exist.

## Tables and schemas 


Input data is taken from S3 and loaded into staging tables. 
Afterwards, data is transformed and loaded into tables relevant for analytics: 

1. songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

Sortkey(start_time) was chosen to ease analysing user sessions.
Distkey(user_id) was chosen to keep behaviour info of same user together. 

![songplays table](https://imgur.com/87Vt9Tv.jpeg)

2. users (user_id, first_name, last_name, gender, level)

Sortkey(level) was chosen to speed up analytics for paying vs free users.

![users table](https://imgur.com/DtmbGWW.jpeg)

3. songs (song_id, title, artist_id, year, duration)

Sortkey(artist_id) was chosen to speed up analytics based on artist. 

![songs Table](https://imgur.com/x3vCiji.jpeg)

4. artists (artist_id, name, location, latitude, longitude)

Sortkey(artist_id) was chosen to speed up analytics based on artist. 

![artists Table](https://imgur.com/qY309ky.jpeg)

5. time (start_time, hour, day, week, month, year, weekday)

Sortkey(start_time) was chosen to speed up analytics within same timeframe. 

![time table](https://imgur.com/1YbLlOj.jpeg)

While tables 2-5 are used to organise data in dimensions (which users do we have, which songs, etc.), 
table 1. is a fact table designed for analytics. 

Staging tables are not mentioned in this README, as they are only an intermediary process to get data from S3 and load into relevant tables. 

Example of analytical queries can also be found below. 

## Running the project 
### Pre-requisites 
1. Make sure you have AWS account and it's credentials at hand. 
2. Make sure you have Python 3 installed, it's required to run the scripts. 

### Process
1. **WARNING: Never commit AWS Credentials to Github or share them with others.**
   Put relevant AWS credentials into `dwh.cfg`, as it's necessary for all next steps. 
2. Run `create_aws_infrastructure.py` to create required AWS infrastructure. This will only work once you did step 1. 
3. Check the ouput for Redshift cluster hostname and IAM Role ARN, you need to put it into `dwh.cfg` too. 
3. Run `create_tables.py` that will take care of cleaning up (if any) and recreating tables in Redshift cluster. 
4. Run `etl.py` that will populate data into tables. 
5. Try and explore cluster tables to see what queries you can run with the data. 
6. **WARNING!** Only run this after you're sure you don't need to run any queries soon and want to save the costs. 
   Finally, run `cleanup_aws_infrastructure.py` to shutdown Redshift cluster. 


# License
Please refer to `LICENSE.md` to understand how to use this project for personal purposes.
