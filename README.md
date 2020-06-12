# buzztracker
Tracking the social-media buzz around your favorite TV shows!

# Setup
Set up 1 master node and 5 worker nodes for Spark. Set up 1 machine for Elasticsearch/Kibana.

# Ingestion
This directory stores the scripts used to import the original zipped files into s3.

# Processing
This directory stores scripts that process data in Spark and Elasticsearch. Also includes Google Apps Script that auto-triggers whenever key Google Sheet columns are updated by user.
