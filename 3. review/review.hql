-- source data: https://www.yelp.com/dataset/download

-- hdfs dfs -rm -r yelpReviewInput
-- hdfs dfs -mkdir yelpReviewInput
-- hdfs dfs -rm -r yelpReview
-- hdfs dfs -mkdir yelpReview
-- hdfs dfs -put review.json yelpReviewInput
-- hdfs dfs -put review.json yelpReview
-- hdfs dfs -ls yelpReviewInput


-------------------------------------------------------------
-- Step-1: Create external table "review"
-------------------------------------------------------------
ADD JAR hdfs://babar.es.its.nyu.edu:8020/user/hjc441/yelpInput/json-serde-1.3.8-jar-with-dependencies.jar;
DROP TABLE IF EXISTS review;
CREATE EXTERNAL TABLE review (
review_id string,
user_id string, 
business_id string, 
stars int, 
date string, 
text string, 
useful int, 
funny int, 
cool int)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("dots.in.keys" = "true" )
STORED AS TEXTFILE
LOCATION '/user/hjc441/yelpInput/review';


-------------------------------------------------------------
-- Step-2: Copy input data manually to HDFS location '/user/hjc441/yelpReview/review' as given below.
-------------------------------------------------------------
$ hadoop fs -copyFromLocal /home/hjc441/project/yelp/review.json /user/hjc441/yelpInput/review


-------------------------------------------------------------
-- Step-3: Now, You can query the table 'review' from hive
SELECT * FROM review limit 5;
-------------------------------------------------------------


-------------------------------------------------------------
-- Step-4: Create review1 table and join with business table
-------------------------------------------------------------
-- join 
DROP TABLE IF EXISTS review1;
CREATE TABLE review1 AS
SELECT j.zipcode, j.average, city, state, business_id, name, latitude, longitude, categories, r.stars, SUM(cool) AS coolValue
FROM review r 
JOIN joined1 j ON (j.business_id = r.business_id)
WHERE array_contains(j.categories, "Restaurants") and r.stars >= 4
GROUP BY zipcode, j.average, city, state, r.business_id, name, latitude, longitude, categories, r.stars
ORDER BY coolValue DESC, j.average;

-- query
SELECT * FROM review1 LIMIT 10;


-------------------------------------------------------------
-- Step-5: Create review2 table
-- top 500 Restaurants that have highest cool value and favorable stars
-------------------------------------------------------------
DROP TABLE IF EXISTS review2;
CREATE TABLE review2 AS
SELECT * FROM review1 LIMIT 500;

-- query
SELECT * FROM review2 LIMIT 5;


-------------------------------------------------------------
-- Step-6: Create topRest table(Most poular Restaurants category)
-- Top 30 Categories of Restaurants from yelp entire dataset
-------------------------------------------------------------
DROP TABLE IF EXISTS topRest;
CREATE TABLE topRest AS
SELECT count(*) AS wordcount, count
FROM business
LATERAL VIEW explode(categories) t1 AS count
WHERE array_contains(categories, "Restaurants")
GROUP BY count
ORDER BY wordcount DESC, count
LIMIT 30;

-- query
SELECT * FROM topRest;

-- Output to local file system
$ hive -e 'use hjc441; select * from topRest' | sed 's/[\t]/,/g'  > /home/hjc441/hiveInput/topRest.txt


-------------------------------------------------------------
-- Step-7: Create topRest1 table(Most poular Restaurants category)
-- Top 30 Categories of Restaurants from Top 500 restaurants
-------------------------------------------------------------
DROP TABLE IF EXISTS topRest1;
CREATE TABLE topRest1 AS
SELECT count(*) AS wordcount, count
FROM review2
LATERAL VIEW explode(categories) t1 AS count
WHERE array_contains(categories, "Restaurants")
GROUP BY count
ORDER BY wordcount DESC, count
LIMIT 30;

-- query
SELECT * FROM topRest1;

-- Output to local file system
$ hive -e 'use hjc441; select * from topRest1' | sed 's/[\t]/,/g'  > /home/hjc441/hiveInput/topRest1.txt


-------------------------------------------------------------
-- Step-8: Finding median value from review2 and irs table
-------------------------------------------------------------
SELECT percentile(average, 0.5) FROM irs;     -- $51122.0
SELECT percentile(average, 0.5) FROM review2; -- $81121.0 


-------------------------------------------------------------
-- Step-9: Return an array of NB histogram bins, where the x value is
--         the center and the y value is the height of the bin.
--         histogram_numeric(col, NB)
-------------------------------------------------------------


-- From irs table
DROP TABLE IF EXISTS irsHistogram;
CREATE TABLE irsHistogram AS
SELECT CAST(hist.x AS int) AS center_average_income, CAST(hist.y AS int) AS height
FROM (SELECT histogram_numeric(average, 15) AS average_histogram
      FROM irs) a
LATERAL VIEW explode(average_histogram) exploded_table AS hist;

-- query
SELECT * FROM irsHistogram;

-- Output to local file system
$ hive -e 'use hjc441; select * from irsHistogram' | sed 's/[\t]/,/g'  > /home/hjc441/hiveInput/irsHistogram.txt


-- From review2 table
DROP TABLE IF EXISTS review2Histogram;
CREATE TABLE review2Histogram AS
SELECT CAST(hist.x AS int) AS center_average_income, CAST(hist.y AS int) AS height
FROM (SELECT histogram_numeric(average, 15) AS average_histogram
      FROM review2) a
LATERAL VIEW explode(average_histogram) exploded_table AS hist;

-- query
SELECT * FROM review2Histogram;

-- Output to local file system
$ hive -e 'use hjc441; select * from review2Histogram' | sed 's/[\t]/,/g'  > /home/hjc441/hiveInput/review2Histogram.txt

