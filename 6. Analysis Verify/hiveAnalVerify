//Select Top 10 city 
select city, count(*) as countOne
from zipComplete group by city order by countOne desc ;

--Phoenix 11186 and Charlotte 5966 selected - Las Vegs has too many tour
//Las Vegs data
select * from (
   select *, rank() over (order by sumV desc) as rank_over from (
      select businesstype, sum(value) as sumV from (
         select * from zipComplete
      where city = 'Las Vegas') fTable
   where income < 60000 group by businesstype order by sumV ) xxtable
) xTable where businesstype = 'Burgers';
//////////////////////////////////

create table zipIncomeLevel as 
select zip, city, state, income, case when income > 60000 then '>60000' else '<60000' end as incomeLevel,
businesstype, value from zipComplete;
////////////////////////////
//working
select city, incomeLevel, businesstype, sum(value) as sumValue from zipIncomeLevel 
group by city, incomeLevel, businesstype; 
-------------------------------------------------
select * from (
select *, rank() over (order by incomeLevel, sumValue desc) as rank_over from (
select incomeLevel, city, businesstype, sum(value) as sumValue from zipIncomeLevel 
where city = 'Phoenix'  group by incomeLevel, city,  businesstype order by incomeLevel, sumValue desc
) tabel1
) tabel2 where businesstype = 'Burgers';

===============
select incomeLevel, city, businesstype, sum(value) over (partition by incomeLevel) as sumValue from zipIncomeLevel 
where city = 'Phoenix' order by sumValue desc limit 10;