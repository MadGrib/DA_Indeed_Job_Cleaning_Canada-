/*
First of all, we need to import the csv that we got through Python BS4 web-scraping.
by creating a table in Postgres SQL and manually selecting the column name and data type.
*/

--1. Creating a duplicate table for the original table so we can clean it and don't worry about losing any important data

CREATE TABLE job_project_clean AS 
(SELECT * FROM job_project); 

--2. Creating a new row_num helps us identify duplicate values.

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY job_title, company, location) as row_num
FROM job_project_clean;

--3. Putting this into CTE, identify all rows that are not unique (row_num is not 1).

WITH duplicate_values AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY job_title, company, location) as row_num
FROM job_project_clean
)
DELETE 
FROM duplicate_values
WHERE row_num > 1;

--4. Creating a table so we can delete rows

CREATE TABLE job_project_clean_v2 AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY job_title, company, location) as row_num
FROM job_project_clean
)

--5. Deleting dublicates

DELETE 
FROM job_project_clean_v2
WHERE row_num > 1;



-- Standardizing data --

--1. Removing extra whitespace from the left and right of the string using the TRIM function

UPDATE job_project_clean_v2
SET company = TRIM(company),
    job_title = TRIM (job_title),
    location = TRIM (location);

--2. Hybrid work is based on location, so we will change all 'hybrid' to the actual city name.

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work', 'Remote'); --We don't know where hybrid is so we will nmake it 'remote'

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in North York, ON', 'North York, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Mississauga, ON', 'Mississauga, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Etobicoke, ON', 'Etobicoke, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Toronto, ON', 'Toronto, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Headquarters, BC', 'Headquarters, BC');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Calgary, AB', 'Calgary, AB');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Halton, ON', 'Halton, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Edmonton, AB', 'Edmonton, AB');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Ottawa, ON', 'Ottawa, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Newfoundland and Labrador', 'Newfoundland and Labrador');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Waterloo, ON', 'Waterloo, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Greater Toronto Area, ON', 'Greater Toronto Area, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in St. Catharines, ON', 'St. Catharines, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Halifax, NS', 'Halifax, NS');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Brampton, ON', 'Brampton, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Burnaby, BC', 'Burnaby, BC');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Kitchener, ON', 'Kitchener, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Oakville, ON', 'Oakville, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Burlington, ON', 'Burlington, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Montréal, QC', 'Montréal, QC');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Milton, ON', 'Milton, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in North Vancouver, BC', 'North Vancouver, BC');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Saint-Laurent, QC', 'Saint-Laurent, QC');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in British Columbia', 'British Columbia');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Surrey, BC', 'Surrey, BC');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Guelph, ON', 'Guelph, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Winnipeg, MB', 'Winnipeg, MB');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Metro Vancouver Regional District, BC', 'Metro Vancouver Regional District, BC');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Hamilton, ON', 'Hamilton, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Dieppe, NB', 'Dieppe, NB');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Aurora, ON', 'Aurora, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Vaughan, ON', 'Vaughan, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Unionville, ON', 'Unionville, ON');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Vancouver, BC', 'Vancouver, BC');

UPDATE job_project_clean_v2
SET location = REPLACE(location, 'Hybrid work in Markham, ON', 'Markham, ON');

--Adding the new column 'province'

ALTER TABLE job_project_clean_v2 ADD COLUMN province TEXT;

--Creating a backup 'v3' just in case :) 
CREATE TABLE job_project_clean_v3 AS (
    SELECT * 
    FROM job_project_clean_v2
)

--Filling the province column

UPDATE job_project_clean_v3
SET province = 
  CASE 
    WHEN location LIKE '%, BC' THEN 'BC'
    WHEN location LIKE '%, ON' THEN 'ON'
    WHEN location LIKE '%, QC' THEN 'QC'
    WHEN location LIKE '%, NS' THEN 'NS'
    WHEN location LIKE '%, AB' THEN 'AB'
    WHEN location LIKE '%, MB' THEN 'MB'
    WHEN location LIKE '%, SK' THEN 'SK'
    WHEN location LIKE '%, NB' THEN 'NB'
    WHEN location LIKE '%, NL' THEN 'NL'
    WHEN location LIKE '%, PE' THEN 'PE'
    WHEN location LIKE '%, NT' THEN 'NT'
    WHEN location LIKE '%, YT' THEN 'YT'
    WHEN location LIKE '%, NU' THEN 'NU'
    ELSE 'REMOTE'
  END;

SELECT DISTINCT province
FROM job_project_clean_v3

--Creating v4 where all remote jobs have locations Remote

UPDATE job_project_clean_v4
SET location = 'Remote'
WHERE location LIKE 'Remote%'

-- Checking for NULL values in the location column
SELECT *
FROM job_project_clean_v4
WHERE location IS NULL;

--Removing column row_num

ALTER TABLE job_project_clean_v4
DROP COLUMN row_num


-- New table with city name as separate column
CREATE TABLE job_project_clean_v5 AS
SELECT 
    job_title, 
    company, 
    location, 
    SPLIT_PART(location, ', ', 1) AS city,
    SPLIT_PART(location, ', ', 2) AS province
FROM job_project_clean_v4;

/*
While grouping, I noticed that several job postings listed the province name 
instead of the city name in the city field. To maintain consistency, I replaced 
these entries with 'remote'. As these instances are negligible, this adjustment will 
not impact the overall results
*/


UPDATE job_project_clean_v5
SET city = 'Remote'
WHERE city = 'Canada'

UPDATE job_project_clean_v5
SET city = 'Remote'
WHERE city = 'Newfoundland and Labrador'

UPDATE job_project_clean_v5
SET city = 'Remote'
WHERE city = 'Ontario'

UPDATE job_project_clean_v5
SET city = 'Remote'
WHERE city = 'New Brunswick'


-- Final edit and preparation of the dataset for Tableau visualization

CREATE TABLE dataset_for_viz AS 

SELECT city, province, COUNT(*) as num_of_postings
FROM job_project_clean_v5
GROUP BY city, province
ORDER BY num_of_postings DESC


