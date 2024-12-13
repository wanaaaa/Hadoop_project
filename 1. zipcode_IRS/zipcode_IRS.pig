-- source data: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-2015-zip-code-data-soi

-- hdfs dfs -mkdir irsInput
-- hdfs dfs -put 15zpallnoagi.csv /user/hjc441/irsInput

lines = LOAD '/user/hjc441/irsInput/15zpallnoagi.csv' USING PigStorage(',');
clean0 = FILTER lines BY $0 != 'STATEFIPS';
clean1 = FOREACH clean0 GENERATE $2 AS zipcode, (float) $4 AS num, (float) $18 AS agi;
clean2 = FILTER clean1 BY zipcode != 99999;
clean3 = FILTER clean2 BY zipcode != 0;
clean4 = FOREACH clean3 GENERATE zipcode, agi * 1000 / num;
sorted = ORDER clean4 BY zipcode;
STORE sorted INTO '/user/hjc441/irsInput/output' USING PigStorage ();

-- Check output file in HDFS
-- $ hdfs dfs -ls /user/hjc441/irsInput/output

-- COPY output from HDFS to Local
-- $ hadoop fs -copyToLocal /user/hjc441/irsInput/output/part-r-00000 average_income.txt

-- Delete output if output is already exists before run pig
-- $ hdfs dfs -rm -r /user/hjc441/irsInput/output
