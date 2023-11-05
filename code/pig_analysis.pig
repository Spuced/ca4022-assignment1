-- Load the CSV
job_listings = LOAD '../output/clean_job_listings/part-r-00000' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (
	job_id:int,
	title:chararray,
	location:chararray,
	country:chararray,
	department:chararray,
	salary_range:chararray,
	low_salary:int,
	high_salary:int,
	salary_midpoint:double,
	company_profile:chararray,
	description:chararray,
	requirements:chararray,
	benefits:chararray,
	telecommuting:int,
	has_company_logo:int,
	has_questions:int,
	employment_type:chararray,
	required_experience:chararray,
	required_education:chararray,
	industry:chararray,
	function:chararray,
	fraudulent:int);

-- Q1 Most frequent fraudulent job titles

-- Filter the fraudulent jobs
fraudulent_jobs = FILTER job_listings by fraudulent==1;

-- Group the titles
grouped_titles = GROUP fraudulent_jobs BY title;

-- Count the jobs with the title
job_with_title = FOREACH grouped_titles GENERATE group AS title, COUNT(fraudulent_jobs) AS count;

-- Filter the titles that occur more than once
recurrent_title = FILTER job_with_title by count > 1;

-- Order by counts
sorted_titles = ORDER recurrent_title BY count DESC, title;

-- Limit to the top 20
top_titles = LIMIT sorted_titles 20;

-- DUMP the results
DUMP top_titles

-- Q2 Top 10 real average salaries by industry in the US

-- Filter rows where salary_midpoint is not null, country is "US", and fraudulent is 0
filtered_jobs = FILTER job_listings BY salary_midpoint IS NOT NULL AND industry IS NOT NULL AND industry != '' AND country == 'US' AND fraudulent == 0;

-- Group data by industry
grouped_industries = GROUP filtered_jobs BY industry;

-- Calculate average salary and count of salary_midpoint for each industry
average_salary = FOREACH grouped_industries GENERATE group AS industry, AVG(filtered_jobs.salary_midpoint) AS average_salary, COUNT(filtered_jobs.salary_midpoint) AS salary_count;

-- Filter industries with more than 5 salary counts
filtered_industries = FILTER average_salary BY salary_count >= 5;

-- Order the results by average_salary in descending order
sorted_industries = ORDER filtered_industries BY average_salary DESC;

-- Limit the output to the top 10 records
top_10_industries = LIMIT sorted_industries 10;

-- DUMP the results
DUMP top_10_industries;