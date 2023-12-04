$HADOOP_HOME/bin/hdfs dfs -mkdir -p output
$HADOOP_HOME/bin/hdfs dfs -mkdir -p data
$HADOOP_HOME/bin/hdfs dfs -put ../data/fake_job_postings.csv data
