
--- Create a table 
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);


--- Create a file format for data loading
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';


--- Create an external stage to an existing open S3 bucket
create or replace stage citibike_trips
    url = 's3://snowflake-workshop-lab/citibike-trips/';

-- Execute a directory listing
list @citibike_trips pattern='.*csv.*';

-- ~1 minute 370 files
copy into trips from @citibike_trips file_format=CSV pattern = '.*csv.*';

-- 61 M rows
select count (*) from trips;

-- let's redo the loading
truncate table trips;

-- Switching from a XSmall single node to a Medium 4 nodes cluster
-- Replace XXXX with your team name and team number
use warehouse team_XXXX_XXXX_2_medium_wh;

-- ~30 seconds 370 files
copy into trips from @citibike_trips file_format=CSV pattern = '.*csv.*';

-- Checking 61 M rows
select count(*) from trips;

-- Getting a sample of the trips data
select * from trips limit 20;

-- Sample query
select start_station_name, end_station_name, count(*) as trips_count from trips 
group by start_station_name, end_station_name
order by trips_count desc
limit 5;

-- Let's switch over to our datasets
use database ml_workspace;

select * from search_trends_google;

select * from dengue_cases;

select * from temperature_rain;

