# Cloud Deployment Subfolder

This subfolder contains modified versions of the pig script that can be deployed on the cloud with the mapreduce framework.

To run these files on Google DataProc create a **data** and **output** folder within a Google Cloud Storage Bucket.

The videos show the process of running these files on Google DataProc, while the images show the setup of the master and worker nodes.
Add the fake_job_postings and country_code CSVs to this bucket then initiate the cluster.

- **cloud_cleaning_job_listings.pig:** Use this script to clean the job listing file. Execute the script using the following command: `$ gcloud dataproc jobs submit pig --cluster=assignment1 --region=europe-west4 --file=cloud_cleaning_job_listings.pig`

- **cloud_hive_analysis.hql:** This script runs the hive queries. Execute after the cleaning script using: `gcloud dataproc jobs submit hive --cluster=assignment1 --region=europe-west4 --file=cloud_hive_analysis.hql`

- **cloud_pig_analysis.pig:** This script replicates the two simple Hive queries. Run it using `gcloud dataproc jobs submit pig --cluster=assignment1 --region=europe-west4 --file=cloud_pig_analysis.pig`.
