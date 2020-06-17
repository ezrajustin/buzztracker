#!/usr/bin/python3

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# indicate Google Sheets API (GS API)
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

# collect credentials to access GS API
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/ubuntu/BuzzTracker-fe105c17a282.json', scope)

# create client authorizing those creds
client = gspread.authorize(creds)

# open spreadsheet
sheet = client.open('buzztracker_shows_data').sheet1

# get values from `num_days_since_search_terms_last_edited` column in Google Sheet
num_days_since_last_updated = sheet.col_values(14)


if __name__ == "__main__":
	# we only want this to complete successfully and thus trigger subsequent `load_agg_es_index.py` code if search terms have been edited in last 24 hours
	if not '0' in num_days_since_last_updated:
		raise Exception("The user-input search terms have not been updated in at least 24 hours. No need to execute script.")
	else:
		print("User-search terms have been updated in past 24 hours. Executing `load_agg_es_index.py`.")
