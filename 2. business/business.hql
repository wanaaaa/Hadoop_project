-- source data: https://www.yelp.com/dataset/download


-- hdfs dfs -rm -r yelpInput
-- hdfs dfs -mkdir yelpInput
-- hdfs dfs -rm -r yelp
-- hdfs dfs -mkdir yelp
-- hdfs dfs -put business.json yelpInput
-- hdfs dfs -put business.json yelp
-- hdfs dfs -put json-serde-1.3.8-jar-with-dependencies.jar yelpInput
-- hdfs dfs -ls yelpInput

-- beeline
-- !connect jdbc:hive2://babar.es.its.nyu.edu:10000/after
-- hjc441
-- password
-- use hjc441;
-- ADD JAR hdfs://babar.es.its.nyu.edu:8020/user/hjc441/yelpInput/json-serde-1.3.8-jar-with-dependencies.jar;


-----------------------------------------
-- Step-1: Create external table "business"
-----------------------------------------
DROP TABLE IF EXISTS business;
CREATE EXTERNAL TABLE business (
business_id string,
name string, 
neighborhood string, 
address string, 
city string, 
state string, 
postal_code string, 
latitude string, 
longitude string, 
stars float, 
review_count int, 
is_open int, 
attributes array<string>, 
categories array<string>, 
hours string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("dots.in.keys" = "true" )
STORED AS TEXTFILE
LOCATION '/user/hjc441/yelpInput/business';

-------------------------------------------------------------
-- Step-2: Copy input data manually to HDFS location '/user/hjc441/yelpInput/business' as given below.
-------------------------------------------------------------
$ hadoop fs -copyFromLocal /home/hjc441/project/yelp/business.json /user/hjc441/yelpInput/business


-------------------------------------------------------------
-- Step-3: Now, You can query the table 'business' from hive
-------------------------------------------------------------
-- test query
SELECT * from business limit 5;


-------------------------------------------------------------
-- Step-4: Create talbe from average_income.txt
-- hdfs dfs -mkdir hiveInput
-- hdfs dfs -put average_income.txt hiveInput
-------------------------------------------------------------

-- Create irs table 
DROP TABLE IF EXISTS irs;
CREATE EXTERNAL TABLE irs (zipcode string, average int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION  '/user/hjc441/hiveInput/';

-- test query
SELECT * from irs limit 5;

-- Create TABLE joined1 (Join irs and business tables)
DROP TABLE IF EXISTS joined1;
CREATE TABLE joined1 AS 
SELECT i.zipcode, i.average, b.city, b.state, b.business_id, b.name, b.stars, b.review_count, b.categories, b.latitude, b.longitude
FROM irs i JOIN business b
ON (i.zipcode = b.postal_code);

-- test query
SELECT * FROM joined1 WHERE zipcode = "15024";
