# Peak Taxi

Determine the peak hour of taxi trips, using parquet files as input.

##Usage

    run Main with 2 args: inputDirectory and outputDirectory

###Input

    - directory with .parquet files to process

###Output

    - result.json - a file with the peak hour
    - .parquet file with all taxi trips in peak hour

##Assumptions
    
    - given the datetime and zone id of a taxi we can figure out where it is.
    - if multiple taxi are in the same time in the same zone -> consider it participating traffic
    - get into consideration both pick up and drop off times

##Method

    1. get all taxi trips
    2. select relevant columns:
        - pickup datetime and zone id
        - dropoff datetime and zone id
        - trip id
    3. format datetime: yyyy-MM-dd HH (as minutes are not interesting)
    4. union pickups and dropoffs into 2 columns:
        - date
        - zone
    5. merge date and zone into one column
    6. group by date_zone column
    7. orderBy count descending
    8. get the peak hour and zone 
        - and write it in json file
    9. get all trips in peak date_zone
    10. write trips in .parquet files

##Technical decisions

    - used spark dataframes due to its catalyst optimizer and low garbage collection (GC) overhead.
    - run Spark locally with as many worker threads as logical cores on machine.

###Use case

    used New York Taxi Data 2009-2016 in Parquet Format
    from https://academictorrents.com/details/4f465810b86c6b793d1c7556fe3936441081992e

###Results

    {"peakHour":"2009-10-25 03","zoneId":"79"} (Manhattan)

###Time
    run on a 6 cores CPU
    10 files:
        writing result: 17s, scales liniarly
        writing results: 17s, scales ~liniarly