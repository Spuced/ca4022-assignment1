# Cloud Deployment Subfolder

This subfolder contains modified versions of the pig script that can be run with the mapreduce framework.

- **hdfs_setup.sh:** Once the hadoop nodes are running, execute this script to create the required folders and upload the input files to the hdfs: `$ ./hdfs_setup.sh`

- **pseudo_cleaning_job_listings.pig:** Use this script to clean the job listing file. Ensure the CSV files are placed in the data folder before running. Execute the script using the following command: `$ pig -x mapreduce pseudo_cleaning_job_listings.pig`

- **pseudo_pig_analysis.pig:** This script replicates the two simple Hive queries. Run it using `$ pig -x mapreduce pig_analysis.pig`.
