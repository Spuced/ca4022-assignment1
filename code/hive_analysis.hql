-- Drop the tables
 DROP TABLE job_listings;
 DROP TABLE country_codes;

-- Create the table that the CSV will load into, skipping the header row
-- function is a restricted name in hive so it has to be surrounded by backticks: `function`
-- The original TSV was parsed incorrectly by Hive so I used the SERDE CSV parser instead which can handle quoted commas
CREATE EXTERNAL TABLE IF NOT EXISTS job_listings (job_id INT, title STRING, location STRING, country STRING, department STRING, salary_range STRING, low_salary INT, high_salary INT, salary_midpoint DOUBLE, company_profile STRING, description STRING, requirements STRING, benefits STRING, telecommuting INT, has_company_logo INT, has_questions INT, employment_type STRING, required_experience STRING, required_education STRING,industry STRING,`function` STRING, fraudulent INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('skip.header.line.count'='1');

-- Overwrite the CSV into the table using the **local** file path
LOAD DATA LOCAL INPATH '/home/eddie/Documents/ca4022/assignment_1/ca4022-assignment1/output/clean_job_listings/part-r-00000'
OVERWRITE INTO TABLE job_listings;

-- Show the entire table
SELECT * FROM job_listings;

-- Simple Queries

-- Q1 Most frequent fraudulent job titles
SELECT title, COUNT(job_id) as count
FROM job_listings
WHERE fraudulent = 1
GROUP BY title
HAVING count > 1
ORDER BY count DESC, title
LIMIT 20;

-- Q2 Top 10 real average salaries by industry in the US
SELECT industry, AVG(salary_midpoint) AS average_salary, COUNT(salary_midpoint) AS salary_count
FROM job_listings 
WHERE salary_midpoint IS NOT NULL AND salary_midpoint != '' AND industry IS NOT NULL AND industry != '' AND country = "US" AND fraudulent = 0
GROUP BY industry
HAVING salary_count >= 5
ORDER BY average_salary DESC
LIMIT 10;

-- Complex Queries

-- Q3 Word count of descriptions
SELECT LOWER(word), COUNT(*) as word_count
FROM job_listings
LATERAL VIEW explode(SPLIT(description, ' ')) wordTable as word
WHERE description IS NOT NULL AND description != ''
GROUP BY LOWER(word)
ORDER BY word_count DESC;

-- Q4 Show the percentage of fradulent jobs by country with full country details

-- Create the country code
CREATE EXTERNAL TABLE IF NOT EXISTS country_codes (country_name STRING, alpha_2 STRING, alpha_3 STRING, country_code INT, iso_3166_2 STRING, region STRING, sub_region STRING, intermediate_region STRING, region_code INT, sub_region_code INT, intermediate_region_code INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('skip.header.line.count'='1');

-- Overwrite with CSV data using the local file patj
LOAD DATA LOCAL INPATH '/home/eddie/Documents/ca4022/assignment_1/ca4022-assignment1/data/country_codes.csv'
OVERWRITE INTO TABLE country_codes;

-- Import the country codes
SELECT jl.country, MAX(cc.country_name),MAX(cc.sub_region), MAX(cc.region), COUNT(jl.job_id) AS total_jobs, (SUM(jl.fraudulent) / COUNT(jl.job_id) * 100) AS percentage_fraudulent
FROM job_listings jl
JOIN country_codes cc
ON jl.country = cc.alpha_2
GROUP BY jl.country
ORDER BY percentage_fraudulent DESC, total_jobs DESC;

-- Q5 Sample 10% of the table and break it down by required experience
-- OVER() is needed to run the aggregate across the grouped table
SELECT required_experience, COUNT(required_experience) AS count, (COUNT(required_experience) / SUM(COUNT(required_experience)) OVER()) * 100 AS percentage_breakdown
FROM job_listings TABLESAMPLE (BUCKET 1 OUT OF 10 ON RAND())
GROUP BY required_experience
ORDER BY count DESC;