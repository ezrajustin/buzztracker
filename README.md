# buzztracker
Tracking the social-media buzz around your favorite TV shows!

# Setup
Set up 1 master node and 5 worker nodes for Spark. Set up 1 machine for Elasticsearch/Kibana.

# Ingestion
This directory stores the scripts used to import the original zipped files into s3.

# Processing
This directory stores scripts that process data in Spark and Elasticsearch. Also includes Google Apps Script that auto-triggers whenever key Google Sheet columns are updated by user.

# Scheduling updates
Airflow job configured here to run daily.
Installing Airflow by following these instructions:
https://medium.com/@abraham.pabbathi/airflow-on-aws-ec2-instance-with-ubuntu-aff8d3206171

### REMEMBER, if you can't access the web UI via <ec-ip-address>:8080, then you have to configure firewall (UFW)
See more info on UFW here: https://linuxconfig.org/how-to-configure-firewall-in-ubuntu-18-04
Run following commands in CLI:
```
sudo ufw status
sudo ufw allow 8080
sudo ufw allow 5432
```
