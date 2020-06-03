#!/usr/bin/env python3

# When you run script in Terminal,
# feed it `start` and `end` arguments as follows
# `python3 get_data.py '<start_date>' '<end_date>'`
# For example: `python3 get_data.py '20180701' '20180731'`

from datetime import date, timedelta
import csv
import glob
import os
import sys

# parse out year/month/day of from arguments fed to the script 
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

# URL to download Twitter files from
custom_url = "https://archive.org/download/archiveteam-twitter-stream-{year}-{month:02d}/twitter_stream_{year}_{month:02d}_{day:02d}.tar"

# filepath of the .tar file (after we download from above)
tar_file = "twitter_stream_{year}_{month:02d}_{day:02d}.tar"

# s3 location where script should dump files
s3_url = "s3://buzztracker/Twitter/"

# This function downloads all the .tar files for specified date range 
def downloadTarFilesForCustomDateRange(start, end, delta):
	for k in range(delta.days + 1):
		day = start + timedelta(days=k)
		command = 'wget '+ custom_url.format(year=day.year, month=day.month, day=day.day)
		os.system(command)

# This function decompresses each .tar file for specified date range and then deletes itself
def decompressTarFilesInDateRange(start, end, delta):
	for k in range(delta.days + 1):
		day = start + timedelta(days=k)
		command = 'tar -xvf ' + tar_file.format(year=day.year, month=day.month, day=day.day)
		os.system(command)

		# remove original .tar file after decompressed
		command = 'rm ' + tar_file.format(year=day.year, month=day.month, day=day.day)
		os.system(command)

		# move decompressed files to appropriate folder
		datefilepath = '{year}/{month:02d}/{day:02d}/'.format(year=day.year, month=day.month, day=day.day)
		command = 'mkdir -p ' + datefilepath + ' && mv ' + '{day:02d} '.format(day=day.day) + '{year}/{month:02d}/'.format(year=day.year, month=day.month)
		os.system(command)


# FOR ALL bz files in `2018/07/03/00/` (aka `{year}/{month:02d}/{date:02d}/*`):
# decompress .bz files from result: `bzip2 -d 31.json.bz2` (aka `bzip2 -d *.json.bz2`)
def decompressBzFiles(start, end, delta):
	for k in range(delta.days + 1):
		day = start + timedelta(days=k)
		datefilepath = '{year}/{month:02d}/{day:02d}/'.format(year=day.year, month=day.month, day=day.day)
		files = glob.glob( datefilepath + '/**/*.json.*', recursive = True)

		for file in files:
			command = 'bzip2 -dv ' + file
			os.system(command)

# move decompressed .json file to AWS: `aws s3 mv *.json s3://buzztracker/Twitter/{year}/{month:02d}/{date:02d}/`
# and then deletes folders from sending machine
def moveFilesToS3(start, end, delta):
	for k in range(delta.days + 1):
		day = start + timedelta(days=k)
		datefilepath = '{year}/{month:02d}/{day:02d}/'.format(year=day.year, month=day.month, day=day.day)
		files = glob.glob( datefilepath + '/**/*.json', recursive = True)

		for file in files:
			command = 'aws s3 mv ' + file + ' ' + s3_url + file
			os.system(command)

		command = 'rm -r ' + datefilepath
		os.system(command)

		print("==========COMPLETED MIGRATION OF FILES FOR " + datefilepath + "==========")

'''
NOT SURE IF I NEED THIS LAST FUNCTION ANY MORE.
# Run this function to generate list of filenames in date range and add to "files.txt"
def getCustomDateRange(start, end, delta):
	output_file = "files.txt"
	with open(output_file, 'w') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')
		for k in range(delta.days + 1):
			day = start + timedelta(days=k)
			line = "twitter-{year}-{month:02d}-{day:02d}.tar".format(year=day.year, month=day.month, day=day.day)
			csv_writer.writerow([line])
'''

if __name__ == "__main__":
	downloadTarFilesForCustomDateRange(start,end,delta)
	decompressTarFilesInDateRange(start,end,delta)
	decompressBzFiles(start, end, delta)
	moveFilesToS3(start, end, delta)