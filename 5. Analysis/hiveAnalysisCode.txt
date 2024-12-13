//Transpose
insert overwrite local directory '/home/wl1622/project/yelp/code/hiveOut'
row format delimited fields terminated by ',' 
collection items terminated by '/'
map keys terminated by ':'
select zip, income, numBusiness, businessType, value from zipIncomeBusiness
lateral view explode(categories) xxxTable as businessType, value;

//add ranking column
insert overwrite local directory '/home/wl1622/project/yelp/code/hiveOut'
row format delimited fields terminated by ',' 
select *, rank() over (partition by zip order by value) from zipDataTrans; 

//Normalize ranking and value
create table zipNormalTable as 
select zip, income, numbusiness, businessType, value, 
(value/numbusiness) as normalValue, ranking, (ranking/numbusiness) 
as normalRanking from ziprank;

insert overwrite local directory '/home/wl1622/project/yelp/code/Out'
row format delimited fields terminated by ',' 
select * from zipnormaltable;

/////////////////////////////////////
//add location information
create table zipLocation 
(zip string, city string, state string, lati double, longi double) 
row format delimited fields terminated by ',' 
location '/user/wl1622/hiveInput/';

//get average of latitude and longitude
create table zipLatiLongi as
select zip, avg(lati) as latiAvg, avg(longi) as longiAvg from ziplocation group by zip;

//join zip, city, state and average coordinate
create table zipCityStateCoordi as
select zip, city, state, latiAvg, longiAvg from (
select distinct ziplatilongi.zip, zipLocation.city, zipLocation.state, ziplatilongi.latiAvg, ziplatilongi.longiAvg , row_number() over (partition by ziplatilongi.zip) as row_num
from ziplatilongi join ziplocation on(ziplatilongi.zip == ziplocation.zip) 
) xxxTable where row_num = 1;
////////////////////////////////////////////
//join  zipNormalTable with zipCityStateCoordi

create table zipComplete as 
select zipNormalTable.*, zipCityStateCoordi.city, 
zipCityStateCoordi.state, zipCityStateCoordi.latiAvg, zipCityStateCoordi.longiAvg
from zipNormalTable join zipCityStateCoordi on (zipNormalTable.zip == zipCityStateCoordi.zip);

insert overwrite local directory '/home/wl1622/project/yelp/code/hiveOut'
row format delimited fields terminated by ',' 
select * from zipComplete;
//////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
//incomeLevel
create table incomeLevelRaw (level string, numZip int, numBusiness int, categories map<string, double>)
row format delimited fields terminated by ',' 
collection items terminated by '/'
map keys terminated by ':'
location '/user/wl1622/hiveInput/';

select level, businessType, value from incomeLevel
lateral view explode(categories) dummy_table as businessType, value
where value > 100;

select level, businessType, value from incomeLevel 
lateral view explode(categories) dummy_table as businessType, value
where level = "45_50" limit 30;

/////////////////////////////////
create table incomeLevelTranspose AS
select level, numzip, numbusiness, businessType, value from incomeLevelRaw 
lateral view explode(categories) dummy_table as businessType, value;

create table incomeLevelTrans30 as
select * from (
    select level, numzip, numbusiness, businessType, value, 
    rank() over ( partition by level order by value desc) as rank 
    from incomeLevelTranspose) xxxTable where rank < 30;

insert overwrite local directory '/home/wl1622/project/yelp/code/hiveOut'
row format delimited fields terminated by ',' 
select * from incomeLevelTrans30;
///////////////////////////////////

insert overwrite local directory '/home/wl1622/project/yelp/code/hiveOut'
row format delimited fields terminated by ',' 
select * from zipComplete;
////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////
///////////////////////////////////////////////////////
//Analysis
select zip, income, numbusiness, normalranking, city, state from zipcomplete 
where businesstype = 'Burgers' and numbusiness > 100
order by normalranking;

select zip, count(*) from zipcomplete
where businessType = 'Burgers'
group by zip;
order by count(zip); 

select zip,  sum(Burgers_or_not) as sumAA from (
select zip, businessType, case when businessType = 'Burgers' then 1 else 0 
end as Burgers_or_not from zipcomplete
) ttt group by zip order by sumAA;
////////////////////////////////////////////////
//working
select zip, sumAA from (
select zip,  sum(Burgers_or_not) as sumAA from (
select zip, businessType, case when businessType = 'Burgers' then 1 else 0 
end as Burgers_or_not from zipcomplete
) ttt group by zip order by sumAA
) xxxTable where sumAA = 0;
//
//working
select zip, income, numBusiness, city, state, sumAA from (
select zip, income, numBusiness, city, state, sum(Burgers_or_not) as sumAA from (
select zip, income, numBusiness, city, state, case when businessType = 'Burgers' then 1 else 0 
end as Burgers_or_not from zipcomplete
) ttt group by zip, income, numBusiness, city, state order by sumAA
) xxxTable where sumAA = 0 order by income;