-- This version of the script works in MapReduce mode

-- Load in the job listings dataset
--job_listings = LOAD 'data/fake_job_postings.csv' USING PigStorage(',') AS (job_id:int, title:chararray, location:chararray, department:chararray, salary_range:chararray, company_profile:chararray, description:chararray, requirements:chararray, benefits:chararray, telecommuting:int, has_company_logo:int, has_questions:int, employment_type:chararray, required_experience:chararray, required_education:chararray, industry:chararray, function:chararray, fraudulent:int);

-- Check the first entry
--check_row = FILTER job_listings BY job_id==1;

-- Examine each field
--check_column = FOREACH job_listings Generate [field];

-- Delete the output folder so the script can run
fs -rm -r -f ../output/clean_job_listings;

-- Commas in quotes were causing problems and the headers were loaded as the first tuple

-- Load the csv with quoted commas and without headers
job_listings = LOAD './data/fake_job_postings.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (
	job_id:int,
	title:chararray,
	location:chararray,
	department:chararray,
	salary_range:chararray,
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

-- Remove \ from the location field
-- \\\\ is the REGEX representation of \
remove_escape_char = FOREACH job_listings GENERATE *, REPLACE(location,'\\\\','') AS (clean_location:chararray);

-- Extract the country from location using STRSPLIT
extract_country = FOREACH remove_escape_char GENERATE *, FLATTEN(STRSPLIT(location, ',')) AS (country:chararray);

/* Extract the salary figures from the range
Occasionally this field contains random dates instead of salaries, these need to be removed*/

-- Filter the tuples that match the salary_range format e.g. 2000-4000
filter_salary_range = FILTER extract_country BY salary_range MATCHES '\\d+-\\d+';

-- Extract the upper and lower bound
salary_figures = FOREACH filter_salary_range GENERATE job_id, FLATTEN(STRSPLIT(salary_range, '-')) AS (low_salary, high_salary);

/* Some of the figures appear to be in thousands so should be multiplied accordingly
In this case if the value is less than 1000 it is multiplied by 1000 */
fixed_thousands = FOREACH salary_figures GENERATE 
	job_id, 
	(low_salary<1000 AND high_salary<1000 ? low_salary * 1000 : low_salary) AS (low_salary:int), 
	(low_salary<1000 AND high_salary<1000 ? high_salary * 1000 : high_salary) AS (high_salary:int);

-- Change the upper and lower bounds to integers and calculate the midpoint
calculate_midpoint = FOREACH fixed_thousands GENERATE
	job_id,
	(int)low_salary,
	(int)high_salary,
	(low_salary + high_salary)/2 AS salary_midpoint:double;

-- Join the cleaned salary figures with the full dataset
combined_job_listings = JOIN extract_country BY job_id LEFT OUTER, calculate_midpoint BY job_id;

-- Drop the unnecessary fields and disambiguate the schema
clean_job_listings = FOREACH combined_job_listings GENERATE 
	extract_country::job_id AS job_id,
    extract_country::title AS title,
    extract_country::clean_location AS location,
    extract_country::country AS country,
    extract_country::department AS department,
    extract_country::salary_range AS salary_range,
    calculate_midpoint::low_salary AS low_salary,
    calculate_midpoint::high_salary AS high_salary,
    calculate_midpoint::salary_midpoint AS salary_midpoint,
    extract_country::company_profile AS company_profile,
    extract_country::description AS description,
    extract_country::requirements AS requirements,
    extract_country::benefits AS benefits,
    extract_country::telecommuting AS telecommuting,
    extract_country::has_company_logo AS has_company_logo,
    extract_country::has_questions AS has_questions,
    extract_country::employment_type AS employment_type,
    extract_country::required_experience AS required_experience,
    extract_country::required_education AS required_education,
    extract_country::industry AS industry,
    extract_country::function AS function,
    extract_country::fraudulent AS fraudulent;

-- Store the data with the headers
STORE clean_job_listings INTO './output/clean_job_listings' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');