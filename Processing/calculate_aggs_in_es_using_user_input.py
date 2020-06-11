#!/usr/bin/python3

import gspread, json, pprint, requests
from oauth2client.service_account import ServiceAccountCredentials
from elasticsearch import Elasticsearch 
from datetime import datetime as dt, timedelta

# indicate Google Sheets API (GS API)
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
# collect credentials to access GS API
creds = ServiceAccountCredentials.from_json_keyfile_name('BuzzTracker-fe105c17a282.json', scope)
# create client authorizing those creds
client = gspread.authorize(creds)
# open spreadsheet
sheet = client.open('buzztracker_shows_data').sheet1
# get number of TV shows in sheet
num_tv_shows = len( sheet.col_values(1)) - 1
# create PrettyPrinter object
pp = pprint.PrettyPrinter()


# function to calculate agg stats and load agg stats into ES agg index
def calculateLoadAggsToES(index_name):
	# get all records from sheet 
	tv_shows_data = sheet.get_all_records()

	# calculate aggs for each TV show's "-7 days to +28 days since launch" window
	for i in range(num_tv_shows):
		
		tv_shows_json_agg = {}

		launch_date = tv_shows_data[i]['launch_date']
		launch_date = dt.strptime( launch_date, '%Y-%m-%d' )

		start_date = tv_shows_data[i]['analysis_date_window_start']
		start_date = dt.strptime( start_date, '%Y-%m-%d' )

		end_date = tv_shows_data[i]['analysis_date_window_end']
		end_date = dt.strptime( end_date, '%Y-%m-%d' )

		days_since_launch_start = start_date - launch_date
		days_since_launch_end = end_date - launch_date

		# calculate aggs for each day_since_launch of tv_show
		for j in range(days_since_launch_start.days, days_since_launch_end.days):
			
			# input fields into new json object
			tv_shows_json_agg['show_name'] = tv_shows_data[i]['show_name']
			tv_shows_json_agg['season_num'] = tv_shows_data[i]['season']
			tv_shows_json_agg['launch_date'] = tv_shows_data[i]['launch_date']
			tv_shows_json_agg['days_since_launch'] = j
			len_man_search_terms=0
			len_opt_search_terms=0

			# organize mandatory search terms
			if tv_shows_data[i]['mandatory_search_terms'] != '':
				mandatory_search_terms = tv_shows_data[i]['mandatory_search_terms']
				mandatory_search_terms = mandatory_search_terms.replace(" ","")
				mandatory_search_terms = mandatory_search_terms.split(",")
				len_man_search_terms = len(mandatory_search_terms)
				tv_shows_json_agg['mandatory_search_terms'] = mandatory_search_terms

			# organize optional search terms
			if tv_shows_data[i]['optional_search_terms'] != '':	
				optional_search_terms = tv_shows_data[i]['optional_search_terms']
				optional_search_terms = optional_search_terms.replace(" ","")
				optional_search_terms = optional_search_terms.split(",")
				len_opt_search_terms = len(optional_search_terms)
				tv_shows_json_agg['optional_search_terms'] = optional_search_terms

			# start constructing custom SQL query for given show/days_since_launch
			lower_unixtime_ms = date_to_unixtime_ms(start_date + timedelta(days=j))
			upper_unixtime_ms = date_to_unixtime_ms(start_date + timedelta(days=j + 1))
			sql_query = "SELECT COUNT(tweet_date) AS count FROM twitter WHERE tweet_date >= {lower_bound} AND tweet_date < {upper_bound}".format(lower_bound=lower_unixtime_ms, upper_bound=upper_unixtime_ms)
			where_clause_search_term = "MATCH(text, '{search_term}', 'fuzziness=AUTO:5,10;minimum_should_match=1')"

			# append mandatory search terms (if any) to where clause
			if len_man_search_terms:
				if len_man_search_terms == 1:
					sql_query = sql_query + " AND " + where_clause_search_term.format(search_term=mandatory_search_terms[0])
				if len_man_search_terms >= 2:
					for k in range(len_man_search_terms):
						sql_query = sql_query + " AND " + where_clause_search_term.format(search_term=mandatory_search_terms[k])

			# append optional search terms (if any) to where clause
			if len_opt_search_terms:
				if len_opt_search_terms == 1:
					sql_query = sql_query + " AND " + where_clause_search_term.format(search_term=optional_search_terms[0])
				if len_opt_search_terms >= 2:
					sql_query = sql_query + " AND (" + where_clause_search_term.format(search_term=optional_search_terms[0]) 
					for k in range(1,len_opt_search_terms):
						sql_query = sql_query + " OR " + where_clause_search_term.format(search_term=optional_search_terms[k])
					sql_query = sql_query + ")"

			# translate SQL query into Elasticsearch-compatible query
			url = "http://localhost:9200/_sql/translate"
			headers = {'content-type': 'application/json'}
			data = {  "query": sql_query }

			# run ES query and set 'total_tweets' in tv_shows_json_agg
			data = json.dumps(data)
			response = requests.get(url, headers=headers, data=data)
			res= es.search(index='twitter', body=response.text)
			total_count = res['aggregations']['groupby']['buckets'][0]['doc_count']
			tv_shows_json_agg['total_tweets']= total_count
			
			# create empty list for mandatory/optional_search_terms fields if no search terms input by user
			if tv_shows_data[i]['mandatory_search_terms'] == '':
				tv_shows_json_agg['mandatory_search_terms'] = ['']

			if tv_shows_data[i]['optional_search_terms'] == '':
				tv_shows_json_agg['optional_search_terms'] = ['']

			# DELETE LATER
			pp.pprint(tv_shows_json_agg)

			# insert document into Agg index in ES
			res=es.index(index=index_name,doc_type='_doc',body=tv_shows_json_agg)
			print( res['result'],":", tv_shows_json_agg['show_name'],"season", tv_shows_json_agg['season_num'], "-- days since launch: ", tv_shows_json_agg['days_since_launch'] )

# function to convert date 'YYYY-MM-DD' to unixtime in milliseconds
def date_to_unixtime_ms(date):
	unixtime = int(date.timestamp()) * 1000
	return unixtime

# function to connect to Elasticsearch
def connect_to_es():
	es = None
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=60)
	if es.ping():
		print("Connected to Elasticsearch!")
	else: 
		print("COULD NOT CONNECT TO ELASTICSEARCH!")
	return es

# create index in Elasticsearch for aggs
def create_index(es_object, index_name):
	created = False
	# index settings
	settings = {
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 1
		},
		"mappings": {
			"members": {
				"dynamic": "strict",
				"properties": {
					"show_name": {
						"type": "text"
					},
					"season_num": {
						"type": "text"
					},
					"launch_date": {
						"type": "date"
					},
					"days_since_launch": {
						"type": "integer"
					},
					"mandatory_search_terms": {
						"type": "text"
					},
					"optional_search_terms": {
						"type": "text"
					},
					"total_tweets": {
						"type": "long"
					},                    
				}
			}
		}
	}
	try:
		# create index if not already exists
		if not es_object.indices.exists(index_name):
			# Ignore 400 means to ignore "Index Already Exist" error.
			es_object.indices.create(index=index_name, ignore=400, body=settings)
			print('Created Elasticsearch index:', index_name)
		created = True
	except Exception as ex:
		print(str(ex))
	finally:
		return created


if __name__ == '__main__':
	
	index_name = "test_show_season_days_twitter_agg"
	es = connect_to_es()
	if es is not None:
		create_index(es,index_name)
	calculateLoadAggsToES(index_name)

