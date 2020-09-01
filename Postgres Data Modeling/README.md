Sparkify is a music streaming service looking to determine what songs users are listening to. The data is sitting in logs and song metadata info, both in JSON format. This project entails building a Postgres data warehouse that will allow the data to be queryable.

I created a star schema in Postgres. The songplays fact table contains all relevant data regarding each song played. The dimension tables are as follows:  
    * user - Sparkify user data  
    * artist - Artist data  
    * songs - Song data  
    * time - Timestamp data broken into different time units of measure

To start the process,  
    ***a.*** execute create_tables.py in the terminal. This will wipe any existing tables and create fresh ones.  
    ***b.*** execute etl.py in the terminal. 

ETL Process: 
The song log files house all the data that populates the song and artist tables. The process_song_file function loads the data into the table.

The process log_file function extracts data form the log file and uses it to populate the user, time, and songplay tables. The songplay load requires querying data from the song and artist tables.
    
