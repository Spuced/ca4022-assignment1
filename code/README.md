# Code Subfolder

This subfolder contains the Pig and Hive code used for the assignment:

- **cleaning_job_listings.pig:** Use this script to clean the job listing file. Ensure the CSV files are placed in the data folder before running. Execute the script using the following command: `$ pig -x local cleaning_job_listings.pig`

- **hive_analysis.hql:** Contains Hive queries for loading and analysing the cleaned job listings data. These can be executed individually in the hive terminal, or can run all together using `$ $HIVE_HOME/bin/hive -f hive_analysis.hql`.
**Remeber to change the file path to the local locations of the files on your machine.**

- **pig_analysis.pig:** This script replicates the two simple Hive queries. Run it locally using `$ pig -x local pig_analysis.pig`.
