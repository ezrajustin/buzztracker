#!/usr/bin/python3

# To run script, need to feed it `start_date` and `end_date` arguments
# Command below to run in CLI:
# time spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,net.java.dev.jets3t:jets3t:0.9.4,org.elasticsearch:elasticsearch-hadoop:7.7.1 --master spark://<MASTER_EC2_IP_ADDRESS>:7077 --conf spark.dynamicAllocation.enabled=false --executor-memory 4G --num-executors 10 --executor-cores 2 <name_of_pyspark_script_to_run>.py '<pyspark_script_argument' '20181001' '20181031'

import json, os, sys, time
import dateutil.parser as dup
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, LongType, StringType
from pyspark.sql.functions import col, udf


# parse out year/month/day from arguments fed to the script 
start_date = sys.argv[1]
start_year = int(start_date[0:4])
start_month = int(start_date[4:6])
start_day = int(start_date[6:8])

end_date = sys.argv[2]
end_year = int(end_date[0:4])
end_month = int(end_date[4:6])
end_day = int(end_date[6:8])

# convert arguments fed to the script into Date objects
start = date(start_year, start_month, start_day)
end = date(end_year, end_month, end_day)
delta = end - start

# s3 location where script should find raw date-partition file(s)
s3_url = "s3a://buzztracker/Twitter/{year}/{month:02d}/{day:02d}/*/*.json"

# Create Spark Session
spark = SparkSession \
	.builder \
	.appName("jsonToSparkToES") \
	.config('spark.default.parallelism', '100') \
	.getOrCreate()

# Create Spark Context
sc = spark.sparkContext
sc.setLogLevel("WARN")

# UNCOMMENT AND RUN FOLLOWING LINES IF FIRST TIME RUNNING SCRIPT
# FOLLOWING SETS UP CREDENTIALS TO ENABLE ACCESS TO s3 FOLDER
access_id = os.getenv('AWS_ACCESS_KEY_ID')
access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3a.awsSecretAccessKey", access_key)


# Define function to extract Tweet date
@udf(returnType=DateType())
def get_tweet_date(x):
	parsed_date = dup.parse(x)
	return parsed_date

# filter DF for lang = English and posts != 'deleted'
def filterDF(input_df):
	# Filter out any deleted posts, non-english posts, and retain only fields we care about.
	global df 
	df = input_df.where( "delete IS NULL" ) \
		.where( "lang = 'en'" ) \
		.select(
			"created_at",
			get_tweet_date("created_at").alias("tweet_date"),
			col("user.id").alias("user_id"),
			col("user.screen_name").alias("user_screen_name"),
			"text",
			"lang",
			"retweet_count",
			"favorite_count"
			)

# Store filtered JSON file(s) to S3 "Twitter_filtered_repo" directory
def writeDFToS3(df, day):
	df.write.format("json") \
		.mode("overwrite") \
		.save("s3a://buzztracker/Twitter_filtered_repo/{year}/{month:02d}/{day:02d}/" \
			.format(year=day.year, month=day.month, day=day.day) )

# Export filtered JSON data to Elasticsearch (note: `spark.es.nodes.wan.only` needed for AWS)
def writeDFToES(input_df):
	global es_df
	# Get Elasticsearch machine IP
	elasticsearch_ip = os.getenv('ES_IP')
	es_df = input_df.write.format('org.elasticsearch.spark.sql') \
				.option('es.nodes', elasticsearch_ip ) \
				.option('es.port','9200') \
				.option('spark.es.nodes.wan.only','true') \
				.option('es.resource', '%s/%s' % ('twitter', 'tweets')).save(mode="append")


if __name__ == "__main__":

	# For each day from `start_date` to `end_date`, store filtered DF in S3 and then export to ElasticSearch
	for k in range(delta.days + 1):
		day = start + timedelta(days=k)

		print( "COMMENCING processing of data for: {year}-{month}-{day}".format(year=day.year, month=day.month, day=day.day), end='' )
		
		# read all JSON files in s3 respective date's partition into PySpark DF
		df = spark.read.json( s3_url.format(year=day.year, month=day.month, day=day.day) )

		filterDF(df)
		writeDFToS3(df, day)
		writeDFToES(df)
		
		print( "\t\tCOMPLETED processing of data for: {year}-{month}-{day}".format(year=day.year, month=day.month, day=day.day) )
		
