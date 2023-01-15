select * from dsitrict_data;
select * from state_data;
select * from time_series_india_data;
select * from timeseries_data;

with t1 as (
SELECT *,
CASE
WHEN total_tested/meta_population BETWEEN 0.05 AND 0.1 THEN 'Category A'
WHEN total_tested/meta_population BETWEEN 0.10 AND 0.3 THEN 'Category B'
WHEN total_tested/meta_population BETWEEN 0.30 AND 0.5 THEN 'Category C'
WHEN total_tested/meta_population BETWEEN 0.50 AND 0.75 THEN 'Category D'
WHEN total_tested/meta_population > 0.75 THEN 'Category E'
END AS Testing_ratio_category,
total_tested/meta_population as Testing_ratio,
total_deceased/meta_population as death_ratio,
total_vaccinated1/meta_population as '% _of_population_vaccinated1',
total_vaccinated2/meta_population as '% _of_population_vaccinated2'
from dsitrict_data
)
select Testing_ratio_category,sum(total_deceased) as Total_Death,sum(total_deceased)/sum(meta_population)*100 as Death_Ratio  from t1
where Testing_ratio_category is not null
group by Testing_ratio_category



---Now perform an analysis of number of deaths across all category. 
---Example, what was the number / % of deaths in Category A district as compared for Category E districts

alter table state_data
alter column 
total_deceased int



select * from timeseries_data;


select td.*,tds.total_confirmed as Total_con,tds.total_deceased as Total_dec,tds.total_recovered as Total_rec
from timeseries_data as td inner join time_series_india_data as tds on td.date=tds.date 
order by date,state


---Weekly evolution of number of confirmed cases, recovered cases, deaths, tests. For instance, 
---your dashboard should be able to compare Week 3 of May with Week 2 of August 




--insight one: month trend for both year

noted : same trend can be seen in both year for same months

SELECT top 12 DATEPART(Year,date) as Year,DATEPART(month,date) as month,sum(delta_confirmed) AS confirmed_2020 
, lead(sum(delta_confirmed)) over(partition by  DATEPART(month,date) order by DATEPART(Year,date),DATEPART(month,date)) as confirmed_2021,
sum(delta_deceased) as deceased_2020,
lead(sum(delta_deceased)) over(partition by  DATEPART(month,date) order by DATEPART(Year,date),DATEPART(month,date)) as deceased_2021,
sum(delta_recovered) as recovered,
lead(sum(delta_recovered)) over(partition by  DATEPART(month,date) order by DATEPART(Year,date),DATEPART(month,date)) as recovered_2021
from timeseries_data
group by DATEPART(Year,date) ,DATEPART(month,date)
order by year,month;

---insight two : test posivity ratio vs no of test
select date,total_tested,total_confirmed,(total_confirmed/total_tested)*100 as test_posivity_ratio,total_deceased/total_confirmed*100 as Death_ratio_from_confirm from time_series_india_data
where delta_tested is not null
order by date

select date,delta7_tested,delta7_confirmed,(delta7_confirmed/delta7_tested)*100 as test_posivity_ratio from time_series_india_data
where delta7_tested is not null
order by date

---insight three : effect of vaccination onf corona 


select date,td.total_vaccinated1/meta_population*100 AS '% of population vacinated1',td.delta7_confirmed/td.delta7_tested*100 as 'Test Positivity Ratio',td.delta_confirmed,td.delta_deceased,td.delta7_deceased/td.delta7_confirmed*100 as '% of death/confirm '
from time_series_india_data as td 
join state_data as st on st.state=td.state
where td.total_vaccinated1 is not null
order by date

select date,td.total_vaccinated2/meta_population*100 AS '% of population vacinated2',td.delta7_confirmed/td.delta7_tested*100 as 'Test Positivity Ratio',td.delta_confirmed,td.delta_deceased,td.delta7_deceased/td.delta7_confirmed*100 as '% of death/confrim'
from time_series_india_data as td 
join state_data as st on st.state=td.state
where td.total_vaccinated2 is not null
order by date

notes : it can be also be because of trends there is no surity that 
 by end of the 31 oct 2021 25% was fully vaccinated 


 ---year wise comparison of severity of deases vs spread of deseas

 as we know in year 2021 there were more deaths but also were more cases?

 SELECT top 12 DATEPART(Year,date) as Year,DATEPART(month,date) as month,
 sum(delta_confirmed) AS confirmed_2020 
, lead(sum(delta_confirmed)) over(partition by  DATEPART(month,date) order by DATEPART(Year,date),DATEPART(month,date)) as confirmed_2021,
sum(delta_deceased) as deceased_2020,
lead(sum(delta_deceased)) over(partition by  DATEPART(month,date) order by DATEPART(Year,date),DATEPART(month,date)) as deceased_2021,
sum(delta_deceased)/sum(delta_confirmed)*100 as '% of death from confirmed_2020',
lead(sum(delta_deceased)/sum(delta_confirmed)*100) over(partition by  DATEPART(month,date) order by DATEPART(Year,date),DATEPART(month,date)) as '% of death from confirmed_2021'
from timeseries_data
group by DATEPART(Year,date) ,DATEPART(month,date)
order by year,month;

---Compare delta7 confirmed cases with respect to vaccination

select date,state,delta7_confirmed,total_vaccinated2 from timeseries_data
where total_vaccinated2 is not null
order by date,total_vaccinated1

----in which state vaccincation was more effective 

select date,st.state,ts.total_vaccinated2/meta_population*100 as '% of people vaccinated',ts.delta7_confirmed
from timeseries_data as ts 
join state_data as st on ts.state = st.state
where ts.total_vaccinated2 is not null

---2 kpi to measure severity of cases

1.)
select * from state_data
select state,cast(total_deceased as float)/cast(total_confirmed as float )*100 as death_ratio from state_data

2.)
select state,cast(total_deceased as float)/meta_population *100 as '% of people died' from state_data

---Categorise total number of confirmed cases in a state by Months and come up with that one month 
which was worst for India in terms of number of cases