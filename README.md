# buzztracker
Tracking the social-media buzz around your favorite TV shows!


# Overview

Over-the-top (OTT) streaming platforms like Hulu, Amazon Prime, and Netflix collect their own data on how watched their TV shows are. But if they also had access to information about how talked-about (or "buzzy") their TV shows are, it would guide short-term business decisions (e.g. investing social media marketing dollars in buzzy TV shows will likely have more virality, word-of-mouth spread, and ROI) and long-term business decisions (e.g. deciding what type of content to produce in the future, identifying culturally influential content that could contend for prestigious awards like Emmys or Academy Awards, improve brand recognition/equity).

BuzzTracker is a data platform that takes custom inputs specifying TV shows and associated search terms from business users (via Google Sheets), calculates Twitter volumes matching those search terms (using Spark for preprocessing at scale and Elasticsearch for string matching at scale), and visualizes those Twitter volumes in Kibana for appropriate date windows (i.e. one week before TV show launch date to four weeks after TV show launch date). The inputs (i.e. both TV shows and/or search terms of interest) can continually be updated, and the platform will dynamically handle and re-produce results, which makes it a useful tool for ongoing tracking and analysis as opposed to simply a one-time ad hoc analysis.


# Setup
### Spark Setup
To set up the Spark (PySpark v. 2.4.5) cluster, I follow instructions found [here](https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88).

Notably, I set up 1 master node and 5 worker nodes on AWS--all of m5.large EC2 instance type (on each machine: 18.04 Ubuntu, 2 vCPUs, 8GiB).

Alternatively, you can run the following Bash scripts found in the `Setup` folder to set up the master and worker nodes, respectively (notably, you may have to update some of the varibles referenced in the scripts):
`spark_setup_master_node.sh`
`spark_setup_worker_node.sh`

### Elasticsearch Setup
Provision a separate m4.xlarge EC2 instance in AWS (18.04 Ubuntu, 4 vCPUs, 16 GiB). To set up Elasticsearch on that machine, follow instructions [here](https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-elasticsearch-on-ubuntu-18-04). (remember to select "Ubuntu 18.04" in the dropdown for version)

### Kibana Setup
On the same m4.xlarge EC2 where you've installed Elasticsearch, install Kibana (the data visualization tool that naturally pairs with Elasticsearch) by following instructions in the "Installing Kibana on AWS" section of [this tutorial](https://logz.io/blog/install-elk-stack-amazon-aws/).


# Ingestion
Initiate Spark cluster by running following in CLI: `sh /usr/local/spark/sbin/start-all.sh`

In the `Ingestion` folder, you'll find the `get_data.py` script, which we want to run for our date range of interest (e.g. July 2018 to July 2019). The script does the following:
* Downloads compressed Twitter files for specified date range
* Decompresses said files (.tar -> .bz2)
* Decompresses .bz2 files (.bz2 -> json)
* Stores decompressed files to specified S3 bucket

Note: If you are unsure of how to create an S3 bucket, you can find step-by-step instructions [here](https://docs.aws.amazon.com/AmazonS3/latest/gsg/CreatingABucket.html).

### JSON to Spark to ES
First, install the following packages in your master Spark node:
`sudo wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.7/hadoop-aws-2.7.7.jar`
`sudo wget https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar`
`sudo wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.30/aws-java-sdk-1.11.30.jar`

At this point, we have a huge repo of Twitter JSON data (i.e., 3TB+ for one year's worth of Twitter data from July 2018 to July 2019!) sitting in our S3 bucket, but we want to preprocess it in Spark before loading it into Elasticsearch (ES) so that we don't overload ES and so that we can enable ES to run more quickly. We will use ES as our database and search/querying engine. ES is a great tool for this use case because ES is designed to do all sorts of string/textual analyses at scale--and very quickly!

In the `Ingestion` folder, you'll also find the `json_to_spark_to_es.py` script, which we want to run for a specified date range of interest  (see file itself for more specifics on how to execute with specific arguments/flags in CLI). This script does a few things for each day of your specified date range:
* Reads Twitter JSON files to Spark DataFrame
* Preprocessing in Spark: filters DataFrame based on specified conditions and returns only select fields that we care about
* Stores preprocessed DataFrame in S3 (in case we ever need to re-access it, in which case we can access it without having to re-process all the original raw data via Spark)
* Write the DataFrame to Elasticsearch index called `twitter`, which I created separately prior to running this script


# Processing
This directory stores scripts that process data in Spark and Elasticsearch. Also includes Google Apps Script that auto-triggers whenever key Google Sheet columns are updated by user.

### Set up Google Sheets API
To install and set up Google Sheets API, follow instructions [here](https://developers.google.com/sheets/api/quickstart/python).
For more comprehensive instructions, you can also reference [this](https://towardsdatascience.com/accessing-google-spreadsheet-data-using-python-90a5bc214fd2).

`onedit_google_sheets_trigger.gs` is the Google Apps script which I added to my Google Sheet so that it auto-populates a field whenever certain columns in the sheet are updated.

### Preprocessing in Spark
`load_agg_es_index.py` queries the preprocessed data (which now already resides in ES) to load a new index called with aggregated stats (i.e. pre-rolled aggs). Notably, the grain of this agg index is `TV Show` / `Season Number` / `Days Since Launch` . Note: ES's equivalent of "tables" in RDBMS is called an "index".

### Working with Elasticsearch/Kibana
Once these pre-rolled agg data are in our new ES index, we can access that data via Kibana to create charts and dashboards. Typically, you can access Kibana by going to `<ec2_instance_ip_address>:5601` in a web browser. In my case, Kibana was having issues planting itself on port 5601 of the EC2 machine, so I updated the Kibana config file so that Kibana used port 5602 instead, in which case I could access Kibana by going to `<ec2_instance_ip_address>:5602`.


# Airflow
Airflow job configured here to run daily.
Installing Airflow by following [these instructions](https://medium.com/@abraham.pabbathi/airflow-on-aws-ec2-instance-with-ubuntu-aff8d3206171).

### REMEMBER, if you can't access the Airflow web UI via `<ec-ip-address>:8080`, then you have to configure firewall (UFW)
See more info on UFW [here](https://linuxconfig.org/how-to-configure-firewall-in-ubuntu-18-04).
Run following commands in CLI:
```
sudo ufw status
sudo ufw allow 8080
sudo ufw allow 5432
```
