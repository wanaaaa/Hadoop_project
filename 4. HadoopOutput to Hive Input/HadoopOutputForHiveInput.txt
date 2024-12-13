create table zipIncomeBusiness (zip string, income double, numBusiness int, categories map<string, double>) 
row format delimited fields terminated by ',' 
collection items terminated by '/'
map keys terminated by ':'
location '/user/wl1622/hiveInput/';
//
create table zipDataTrans(zip string, income double, numBusiness int, businessType string, value double) 
row format delimited fields terminated by ','
location '/user/cloudera/hiveInput/';
-- location '/user/wl1622/hiveInput/';
////////////////////////////////////////////
//show businessType and value as column
select zip, income, numBusiness, businessType, value from zipIncomeBusiness
lateral view explode(categories) xxxTable as businessType, value

select level, businessType, value from incomeLevel
lateral view explode(categories) dummy_table as businessType, value
where value > 100;