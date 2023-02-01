---- Reading the required tables
select * from districts_data;
select * from state_data;
select * from timeseries_data;


--- Creating a view for the pivot of table from timeseries_data for future references
create view covid as(
select * from(
select State,year(Date)as Year,DATENAME(MONTH,date) as Month,Date,
Type,COALESCE(Tested,0)as Tested,COALESCE(Confirmed,0)as Confirmed,
COALESCE(Recovered,0)as Recovered,COALESCE(Deceased,0)as Deceased,COALESCE(Vaccinated1,0)as First_Vaccine,
COALESCE(Vaccinated2,0)as Second_Vaccine,Datepart(Month,date) as Month_Num
from(
select * from
(select * from timeseries_data) as d
pivot
(sum([Count]) for [Status] in([Tested],[Confirmed],[Recovered],[Deceased],[Vaccinated1],[Vaccinated2]))as e
) as f
) as g
group by State,Year,Month,Date,Type,Tested,confirmed,recovered,Deceased,First_Vaccine,Second_Vaccine,Month_num
);

select * from covid;


--- State Total: Creating and storing State Aggregate Data in View State_total
create view state_total as(
select State as State_code,type,Tested,Confirmed,Recovered,Deceased,First_Vaccine as Vaccinated1,Second_Vaccine as Vaccinated2
from(
select * , DENSE_RANK()over(Partition by state order by date desc) as rank
from covid
) as d
where rank=1
);

select * from state_total;


--- Creating and storing view for districts data


-------------------------------------- ANALYSIS-------------------------------------------------

---Analysis 1: The number of deaths across all categories. 
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
from districts_data
)
select Testing_ratio_category,sum(total_deceased) as Total_Death,sum(total_deceased)/sum(meta_population)*100 as Death_Ratio  from t1
where Testing_ratio_category is not null
group by Testing_ratio_category
order by Total_Death;


---Analysis 2: Weekly evolution of number of confirmed cases, recovered cases, deaths, tests.
select Year, Month,week,Tested,Confirmed,Deceased,Recovered from(
select Year,Month,Month_Num,ceiling (cast(datepart(dd,date)as numeric(38,8))/7) as week
,Sum(Tested) as Tested ,sum(Confirmed) as Confirmed,Sum(Recovered) as Recovered,Sum(Deceased) as Deceased
from covid where state='TT' and type='Delta7'
group by Year,Month,ceiling (cast(datepart(dd,date)as numeric(38,8))/7),Month_Num
)as d
order by year,Month_Num,Month,week;


--Analysis 3: Worst Month With Respect to Highest confirmed Cases
select Top 1 Year, Month,Tested,Confirmed,Deceased,Recovered from(
select Year,Month,Month_Num
,Sum(Tested) as Tested ,sum(Confirmed) as Confirmed,Sum(Recovered) as Recovered,Sum(Deceased) as Deceased
from covid where state='TT' and type='Delta'
group by Year,Month,Month_Num
)as d
order by Confirmed desc;


---Analysis 4 : State Data with First and Second Vaccine Percentage and Death and Recovery rate of Each State
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate
from(
select *,convert(decimal(5,2),(convert(float,Vaccinated1)/convert(float,population)))*100 as First_Vaccine_Percent,
convert(decimal(5,2),(convert(float,Vaccinated2)/convert(float,population)))*100 as Second_Vaccine_Percent
from(
select s.*,population from state_total as s
Join population as p
on p.state_code=s.State_code
) as d
where type='Total'
) as e
order by First_Vaccine_Percent desc;


----Analysis 5: India Data of Total Tested, Confirmed,Recovered, Deceased
select State,Sum(Tested) as Tested ,sum(Confirmed) as Confirmed,Sum(Recovered) as Recovered,Sum(Deceased) as Deceased
from covid where state='TT' and type='Delta'
group by State
order by State;


---Analysis 6: State Data With current Active Cases
select *,(Confirmed-(Recovered+Deceased)) as Active from(
select State,Sum(Tested) as Tested ,sum(Confirmed) as Confirmed,Sum(Recovered) as Recovered,Sum(Deceased) as Deceased
from covid where state!='TT' and type='Delta'
group by state
) as d
order by Confirmed desc;


---Analysis 7: India Death rate Overall Before Vaccine
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate
from(
select State,Sum(Tested) as Tested,sum(Confirmed) as Confirmed,sum(Recovered) as recovered
,sum(Deceased) as Deceased from covid where type='Delta' and First_Vaccine=0 and Confirmed!=0 and recovered !=0 and Tested !=0 and Deceased!=0
and state='TT'
group by State
) as d
order by Death_Rate desc;


---Analysis 8: India Death rate Overall After Vaccine
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate
from(
select State,Sum(Tested) as Tested,sum(Confirmed) as Confirmed,sum(Recovered) as recovered
,sum(Deceased) as Deceased from covid where type='Delta' and First_Vaccine!=0 and Confirmed!=0 and recovered !=0 and Tested !=0 and Deceased!=0
and state='TT'
group by State
) as d
order by Death_Rate DEsc;


--- Analysis  9: Monthly Evolution of Death Rate before Vaccine
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate
from(
select State,Year,Month,Sum(Tested) as Tested,sum(Confirmed) as Confirmed,sum(Recovered) as recovered
,sum(Deceased) as Deceased 
from covid 
where type='Delta' and First_Vaccine=0 and Confirmed!=0 and recovered !=0 and Tested !=0 and Deceased!=0
and state='TT'
group by State,Year,Month
) as d
order by Death_Rate DESC;


--- Analysis 10: Monthly Evolution of Death Rate After Vaccine
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate
from(
select State,Year,Month,Sum(Tested) as Tested,sum(Confirmed) as Confirmed,sum(Recovered) as recovered
,sum(Deceased) as Deceased from covid where type='Delta' and First_Vaccine!=0 and Confirmed!=0 and recovered !=0 and Tested !=0 and Deceased!=0
and state='TT'
group by State,Year,Month
) as d
order by Death_Rate DESC;


--- Analysis 11: Monthly Evolution of Death Rate After Vaccine For each State
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate
from(
select State,Year,Sum(Tested) as Tested,sum(Confirmed) as Confirmed,sum(Recovered) as recovered
,sum(Deceased) as Deceased from covid where type='Delta' and First_Vaccine!=0 and Confirmed!=0 and recovered !=0 and Tested !=0 and Deceased!=0
group by State,Year
) as d
order by Death_Rate DESC;


--- Analysis 12: Monthly Evolution of Death Rate Before Vaccine For each State
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate
from(
select State,Year,Sum(Tested) as Tested,sum(Confirmed) as Confirmed,sum(Recovered) as recovered
,sum(Deceased) as Deceased from covid where type='Delta' and First_Vaccine=0 and Confirmed!=0 and recovered !=0 and Tested !=0 and Deceased!=0
group by State,Year
) as d
order by Death_Rate DESC;


--- Analysis 13: Overall Death Rate After Vaccine For each State
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate
from(
select State,Sum(Tested) as Tested,sum(Confirmed) as Confirmed,sum(Recovered) as recovered
,sum(Deceased) as Deceased from covid where type='Delta' and First_Vaccine!=0 and Confirmed!=0 and recovered !=0 and Tested !=0 and Deceased!=0
group by State
) as d
order by Death_Rate DESC;


--- Analysis 14: Districts With highest testing Ratio
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
from districts_data
)
select * from t1 order by Testing_Ratio desc;


----Analysis 15: Top 5 District with Highest Death Rate
select Top 5 *,
(convert(float,total_deceased)/convert(float,total_confirmed))*100 as Death_Rate,
(convert(float,total_recovered)/convert(float,total_confirmed))*100 as Recovery_Rate
from districts_data
order by Death_Rate desc


----Analysis 16: Top 5 District with highest testing ratio
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
from districts_data
)
select Top 5 *,
(convert(float,total_deceased)/convert(float,total_confirmed))*100 as Death_Rate,
(convert(float,total_recovered)/convert(float,total_confirmed))*100 as Recovery_Rate
from t1
order by Testing_Ratio desc;


--- Analysis 17: Top 5 District with percent of Population Got Covid
select Top 5 * from(
select *,
(convert(float,total_confirmed)/convert(float,meta_population))*100 as Case_Rate
from districts_data
) as d
order by Case_Rate desc;


---Analysis 18: Top Performing Districts with Lowest Death rate and Highest recovery Rate
select Top 5 *,
(convert(float,total_deceased)/convert(float,total_confirmed))*100 as Death_Rate,
(convert(float,total_recovered)/convert(float,total_confirmed))*100 as Recovery_Rate
from districts_data
where (convert(float,total_deceased)/convert(float,total_confirmed))*100 is not NULL
order by Recovery_Rate DESC;


---Analysis 19: Top 5 States with highest vaccination 
select Top 5 State,
convert(decimal(5,2),(convert(float,Vaccinated1)/convert(float,population)))*100 as First_Vaccine_Percent,
convert(decimal(5,2),(convert(float,Vaccinated2)/convert(float,population)))*100 as Second_Vaccine_Percent,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate
from(
select state,sum(delta7_vaccinated1) as Vaccinated1,sum(delta7_vaccinated2) as Vaccinated2,sum(meta_population) as population,
sum(total_confirmed) as Confirmed,sum(total_deceased) as deceased 
from districts_data
group by state
) as d
order by Second_Vaccine_Percent desc;


----Analysis 20: Best Performing State
select Top 5 * 
, avg(First_Vaccine_Percent) over() as AVG_First_Vaccine_Rate,
avg(Second_Vaccine_Percent) over() as AVG_Second_Vaccine_Rate
from(
select State,district,delta7_vaccinated1,delta7_vaccinated2,meta_population,
convert(decimal(5,2),(convert(float,delta7_vaccinated1)/convert(float,meta_population)))*100 as First_Vaccine_Percent,
convert(decimal(5,2),(convert(float,delta7_vaccinated2)/convert(float,meta_population)))*100 as Second_Vaccine_Percent
from districts_data
) as d
where First_Vaccine_Percent is not null
order by First_Vaccine_Percent DESC;


---Analysis 21: Worst Performing State
select Top 5 * 
, avg(First_Vaccine_Percent) over() as AVG_First_Vaccine_Rate,
avg(Second_Vaccine_Percent) over() as AVG_Second_Vaccine_Rate
from(
select State,district,delta7_vaccinated1,delta7_vaccinated2,meta_population,
convert(decimal(5,2),(convert(float,delta7_vaccinated1)/convert(float,meta_population)))*100 as First_Vaccine_Percent,
convert(decimal(5,2),(convert(float,delta7_vaccinated2)/convert(float,meta_population)))*100 as Second_Vaccine_Percent
from districts_data
) as d
where First_Vaccine_Percent is not null
order by First_Vaccine_Percent;


---Analysis 22: Top 5 District with Highest death rate vs Average Death Rate for State Vs District Death Rate
select *,
Avg(Death_Rate) over(Partition by State) as AVG_DEATH_Rate
from(
select State,District,total_deceased,
(convert(float,total_deceased)/convert(float,total_confirmed))*100 as Death_Rate
from districts_data
where (convert(float,total_deceased)/convert(float,total_confirmed))*100 is not NULL
) as d
order by Death_Rate DESC;


---Analysis 23: Top 5 DIstrict with Highest case rate vs Avg India Case rate
select Top 5  *,
avg(Case_rate) over(Partition by State) as Avg_Case_Rate
from(
Select State,District, total_confirmed, meta_population,
(convert(float,total_confirmed)/convert(float,meta_population))*100 as Case_rate
from districts_data
where total_confirmed is not Null and meta_Population is not null
) as d
order by Case_rate desc;


---Analysis 24: India's Vaccination Rate
select (convert(float,vaccination_1)/convert(float,population))*100 as First_Vaccine_Percent,
(convert(float,vaccination_2)/convert(float,population))*100 as Second_Vaccine_Percent
from(
select sum(meta_population) as Population, Sum(delta7_vaccinated1) as Vaccination_1, Sum(delta7_vaccinated2) as Vaccination_2
from districts_data
) as d;


---Analysis 25: Testing Category data
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
from districts_data
)
select *,
(convert(float,Deceased)/convert(float,Confirmed))*100 as Death_Rate,
(convert(float,Recovered)/convert(float,Confirmed))*100 as Recovery_Rate,
convert(decimal(5,2),(convert(float,Vaccinated1)/convert(float,population)))*100 as First_Vaccine_Percent,
convert(decimal(5,2),(convert(float,Vaccinated2)/convert(float,population)))*100 as Second_Vaccine_Percent,
(convert(float,Confirmed)/convert(float,Population))*100 as Case_Rate
from(
select Testing_ratio_category as Category,sum(total_tested) as Tested
,sum(total_confirmed) as Confirmed
,sum(total_deceased) as Deceased
,sum(total_recovered) as recovered
,sum(delta7_Vaccinated1) as Vaccinated1
,sum(delta7_Vaccinated2) as Vaccinated2
,sum(meta_Population)as population
from t1
group by Testing_ratio_category
) as d
where Category is not null
order by First_Vaccine_Percent desc;



---Analysis 26: Tetsing category Death rate and Decesed Number
with t1 as (
SELECT *,
CASE
WHEN total_tested/meta_population BETWEEN 0.05 AND 0.1 THEN 'Category A'
WHEN total_tested/meta_population BETWEEN 0.10 AND 0.3 THEN 'Category B'
WHEN total_tested/meta_population BETWEEN 0.30 AND 0.5 THEN 'Category C'
WHEN total_tested/meta_population BETWEEN 0.50 AND 0.75 THEN 'Category D'
WHEN total_tested/meta_population > 0.75 THEN 'Category E'
END AS Category,
total_tested/meta_population as Testing_ratio,
total_deceased/meta_population as death_ratio,
total_vaccinated1/meta_population as '% _of_population_vaccinated1',
total_vaccinated2/meta_population as '% _of_population_vaccinated2'
from districts_data
)
select Category,deceased,
(convert(float,deceased)/convert(float,confirmed))*100 as Death_Rate
from(
select Category,sum(total_tested) as Tested
,sum(total_confirmed) as Confirmed
,sum(total_deceased) as Deceased
,sum(total_recovered) as recovered
,sum(delta7_vaccinated1) as Vaccinated1
,sum(delta7_vaccinated2) as Vaccinated2
,sum(meta_population)as population
from t1
group by Category
) as d
where Category is not null;
