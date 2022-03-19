-- Data Exploration
-- Queries Utilized in Tableau Project 1

CREATE DATABASE CovPortfolio1;
USE CovPortfolio1;

-- Table constraints
-- Columns marked with 'int' are as such because display width specification has no bearing on whether or not a number displays correctly, nor the storage or performance
-- Columuns marked as double are as such because hard approximations are not needed to understand the dataset

CREATE TABLE CovidDeaths 
                        (isocode VARCHAR(10), 
		                 continent VARCHAR(13),
                          location VARCHAR(25),
                          date_date char(10),
                          population bigint,
                          total_cases int,
                          new_cases int,
                          new_cases_smoothed double,
                          total_deaths int,
                          new_deaths int,
                          new_deaths_smoothed double,
                          total_cases_per_million double,
                          total_deaths_per_million double,
                          new_deaths_per_million decimal(6,3),
                          new_deaths_smoothed_per_million double,
                          reproduction_rate double,
                          icu_patients int,
                          icu_patients_per_million double,
                          hosp_patients int,
                          hosp_patients_per_million double,
                          weekly_icu_admissions int,
                          weekly_icu_admissions_per_million double,
                          weekly_hosp_admissions int,
                          weekly_hosp_admissions_per_million double);

load data local infile 'C:/Users/khema/Downloads/CovidDeaths2.csv' 
INTO TABLE coviddeaths
FIELDS TERMINATED by ','
ENCLOSED BY ' '
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

USE CovPortfolio1;
CREATE TABLE CovidVaccinations
						(isocode VARCHAR(10), 
						 continent VARCHAR(13),
						 location VARCHAR(25),
						 date_date date,
                         new_tests int,
                         total_tests_per_thousand double,
                         new_tests_per_thousand double,
                         new_tests_smoothed int,
                         new_tests_smoothed_per_thousand double,
                         positive_rate double,
                         tests_per_case double,
                         tests_units VARCHAR(15),
                         total_vaccinations int,
                         people_vaccinated int,
                         people_fully_vaccinated int,
                         total_boosters int,
                         new_vaccinations int,
                         new_vaccinations_smoothed int,
                         total_vaccinations_per_hundred double,
                         people_vaccinated_per_hundred double,
                         people_fully_vaccinated_per_hundred double, 
                         total_boosters_per_hundred double,
                         new_vaccinations_smoothed_per_million int,
                         new_people_vaccinated_smoothed int,
                         new_people_vaccinated_smoothed_per_hundred double,
                         stringency_index decimal(4,2),
                         population int,
                         population_density double,
                         median_age decimal(3,1),
                         aged_65_older double,
                         aged_70_older double,
                         gdp_per_capita decimal(8,2),
                         extreme_poverty double,
                         cardiovasc_death_rate decimal(6,3),
                         diabetes_prevalence decimal(4,2),
                         female_smokers double,
                         male_smokers double,
                         handwashing_facilities decimal(5,2),
                         hospital_beds_per_thousand double,
                         life_expectancy decimal(4,2),
                         human_development_index double,
                         excess_mortality_cumulative_absolute double,
                         excess_mortality_cumulative double,
                         excess_mortality double,
                         excess_mortality_cumulative_per_million double);
                         
                         
load data local infile 'C:/Users/khema/Downloads/CovidVaccinations2.csv' 
INTO TABLE covidvaccinations
FIELDS TERMINATED by ','
ENCLOSED BY ' '
LINES TERMINATED BY '\n'
IGNORE 1 LINES;     


-- Looking at Total Cases vs. Total Deaths (Below)
-- Likelihood of Death By Country

SELECT 
location,
date(date_date) as date_date,
total_cases,
total_deaths,
(total_deaths/total_cases)*100 as Death_Percentage
FROM coviddeaths
WHERE location = 'United States'
ORDER BY location, date_date;


-- Total Cases vs. Population (Below)
-- Percentage of Total Cases in Population
SELECT 
location,
date(date_date) as date_date,
total_cases,
new_cases,
population,
(total_cases/population)*100 as Pop_Case_Perc
FROM coviddeaths
WHERE location = 'United States';


-- Countries w/ Highest Infection Rates Compared to their Population (Below)

SELECT 
location,
population,
MAX(total_cases) as Highest_Infec_Rate,
MAX((total_cases/population))*100 as Pop_Perc_Infec
FROM coviddeaths
GROUP BY location, population
ORDER BY Pop_Perc_Infec DESC;


-- Highest Death Count Per Population (Below)

SELECT
location,
MAX(total_deaths) as Total_Death_Count
FROM coviddeaths
WHERE location NOT IN
	(SELECT 
    continent
    FROM coviddeaths)
group by location
ORDER BY Total_Death_Count DESC;

-- Highest Death Count Per Continent (Below)

SELECT
continent,
MAX(total_deaths) As Total_Death_Count
FROM coviddeaths
GROUP BY continent;

-- Total Population vs. Vaccinations  (Below)

WITH tp_vs_v (continent, location, date_date, population, new_vaccinations, rolling_vaccinations)
AS 
(
	SELECT 
		cd.continent, 
		cd.location,
		date(cd.date_date) as date_date, 
		cd.population,
		cv.new_vaccinations,
		SUM(cv.new_vaccinations) over (order by location) as rolling_vaccinations
	FROM coviddeaths cd
	JOIN covidvaccinations cv 
	ON cd.location = cv.location and cd.date_date = cv.date_date
	WHERE cd.continent is not null and cd.location NOT IN
		(SELECT 
		continent
		FROM coviddeaths)
	ORDER BY cd.location, cd.date_date)
    
SELECT *, (rolling_vaccinations/population)*100 as rolling_percentage
FROM tp_vs_v;



-- Comparing level of stringency within the stringency_index (enforcement of closures, travel and the like) (Below)
-- to it's previous index number (down-up 2,1,3,2,4,3,5,4, etc.) . . . 
-- in countries within north america to measure whether or not relaxing said measures . . . 
-- had a positive effect on the positive rate going down; as we want the positive rate to gown down with each degeneration in stringency
-- In this exercise, numbers under column stringency_effectiveness that are less than 0, are considered to have positive correlation
-- Meaning that as stringency is is lowered, so is the positive rate/Relaxation  = lower positive rate

WITH stringency_ratings(location, avg_positive_rate, Total_NewVac_By_Stringency, stringency_index)
AS 
(
SELECT
location,
AVG(positive_rate) as avg_positive_rate,
SUM(new_vaccinations) AS Total_NewVac_By_Stringency,
stringency_index
FROM covidvaccinations
WHERE location IN ('United States', 'Canada', 'Mexico') and people_vaccinated > 0
GROUP BY stringency_index, location 
ORDER BY stringency_index DESC
)

SELECT *, 
avg_positive_rate - coalesce(lag(avg_positive_rate) over (order by location), 0) AS stringency_effectiveness,
IF (avg_positive_rate - coalesce(lag(avg_positive_rate) over (order by location), 0) < 0, 'Positive Correlation', 'Negative Correlation') AS new_stringency_ratings
FROM stringency_ratings;



SELECT
location,
date(date_date) as date_date,
SUM(excess_mortality) as total_excess_mortality,
CASE
    WHEN date_date BETWEEN '2020-04-01' AND '2020-12-01' THEN '1st Check'
    WHEN date_date BETWEEN '2020-12-01' AND '2021-03-01' THEN '2nd Check'
    WHEN date_date BETWEEN '2021-03-01' AND '2022-03-14' THEN '3rd Check'
    ELSE 'Inapplicable'
    END AS Stimulus_Records
FROM covidvaccinations
WHERE location LIKE 'United States'
GROUP BY 
	CASE
    WHEN date_date BETWEEN '2020-04-01' AND '2020-12-01' THEN '1st Check'
    WHEN date_date BETWEEN '2020-12-01' AND '2021-03-01' THEN '2nd Check'
    WHEN date_date BETWEEN '2021-03-01' AND '2022-03-14' THEN '3rd Check'
    ELSE 'Inapplicable'
    END
ORDER BY date_date;

















