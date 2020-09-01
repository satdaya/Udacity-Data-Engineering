Sparkify, a fictional music streaming company, is migrating its data from a Redshift data warehouse to a data lake. The task: build an ETL pipeline to extract data from S3, process the data using Spark, and load back into S3 in the form of easily queryable a set of dimensional tables.

There are two datasets in JSON format.

**1. Song Dataset:** A subset of the Million Song Dataset containing song and artist metadata. The data is partitioned by the first two letters of the track id. The following are examples of the file paths: 1. song_data/A/B/C/TRABCEI128F424C983.json 2. song_data/A/A/B/TRAABJL12903CDCF1A.json

Below is the sample data for one song:
{"num_songs": 1,
 "artist_id": "ARJIE2Y1187B994AB7",
 "artist_latitude": null, 
 "artist_longitude": null,
 "artist_location": "",
 "artist_name": "Line Renaud",
 "song_id": "SOUPIRU12A6D4FA1E1",
 "title": "Der Kleine Dompfaff",
 "duration": 152.92036,
 "year": 0}
 
 **2. Log Dataset:** Simulated user listening data. Data partitioned by year and month. The following are examples of file paths: 1. log_data/2018/11/2018-11-12-events.json 2. log_data/2018/11/2018-11-13-events.json
 
 Below is the sample data for one song:
{”artist”:	0
 ”auth”:	None
 ”firstName”:	Logged In
 ”gender”:	Walter
 ”itemInSession”:	M
 ”lastName”:	0
 ”length”:	Frye
 ”level”:	NaN
 ”location”:	free
 ”method”:	San Francisco-Oakland-Hayward, CA
 ”page”:	GET
 ”registration”:	Home
 ”sessionId”:	1.54092E+12
 ”song”:	38
 ”status”:	None
 ”ts”:	200
 ”userAgent”:	1.54111E+12
 ”userId”:	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4...	39"
 }
 
 The data is transformed into the star schema (attached as DL - ERD).
 
 All code is located in the etl.py script, which can executed in the terminal. Ensure the presence of a config file to store credentials.
 
 The code is designed to be run on an AWS EMR cluster. 
 
