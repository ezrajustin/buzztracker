#!/usr/bin/python3

# Run this script as follows in Terminal `<script_name>.py 'index_name'`

import gspread, json, pprint, requests, sys
from oauth2client.service_account import ServiceAccountCredentials
from elasticsearch import Elasticsearch 
from datetime import datetime as dt, timedelta


# function to connect to Elasticsearch
def connect_to_es():
	es = None
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=60)
	if es.ping():
		print("Connected to Elasticsearch!")
	else: 
		print("COULD NOT CONNECT TO ELASTICSEARCH!")
	return es

# create index in Elasticsearch for aggs; notably, this setup has standard stopwords implemented
def create_index(es_object, index_name):
	created = False
	settings = {
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 1,
			"analysis":{
				"analyzer":{
					"my_analyzer":{
						"type":"standard",
						"stopwords":"_english_"
					}
				}
			}
		},
		"mappings": {
			"dynamic": "strict",
			"properties": {
				"studio_or_network": {
					"type": "keyword"
				},
				"show_name": {
					"type": "keyword"
				},
				"season_num": {
					"type": "integer"
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
	try:
		if not es_object.indices.exists(index_name):
			es_object.indices.create(index=index_name, body=settings)
			print('Created Elasticsearch index:', index_name)
		created = True
	except Exception as ex:
		print(str(ex))
	finally:
		return created

# helper function to convert date 'YYYY-MM-DD' to unixtime in milliseconds
def date_to_unixtime_ms(date):
	unixtime = int(date.timestamp()) * 1000
	return unixtime

# extract variables from Google Sheet input
def extract_gs_row_data(row):
	global launch_date, start_date, end_date, days_since_launch_start, days_since_launch_end, show, season_num, studio_or_network, mandatory_search_terms, optional_search_terms

	launch_date = dt.strptime( row['launch_date'], '%Y-%m-%d' )
	start_date = dt.strptime( row['analysis_date_window_start'], '%Y-%m-%d' )
	end_date = dt.strptime( row['analysis_date_window_end'], '%Y-%m-%d' )
	days_since_launch_start = start_date - launch_date
	days_since_launch_end = end_date - launch_date
	show = row['show_name']
	season_num = row['season']
	studio_or_network = row['studio_or_network']

	if row['mandatory_search_terms'] == '':
		mandatory_search_terms = ['']
	else:
		mandatory_search_terms = row['mandatory_search_terms']
		mandatory_search_terms = list( map( str.strip, mandatory_search_terms.split(",") ) )

	if row['optional_search_terms'] == '':
		optional_search_terms = ['']
	else:
		optional_search_terms = row['optional_search_terms']
		optional_search_terms = list( map( str.strip, optional_search_terms.split(",") ) )

# create SQL query out of Google Sheet row parameters
def create_sql_query(show, season_num, mandatory_search_terms, optional_search_terms, specific_date):
	lower_unixtime_ms=date_to_unixtime_ms(specific_date)
	upper_unixtime_ms=date_to_unixtime_ms(specific_date + timedelta(days=1))
	sql_query = "SELECT COUNT(tweet_date) AS count FROM twitter WHERE tweet_date >= {lower_bound} AND tweet_date < {upper_bound}".format(lower_bound=lower_unixtime_ms, upper_bound=upper_unixtime_ms)

	where_clause_search_term = "MATCH(text, '{search_term}', 'fuzziness=AUTO:5,10;minimum_should_match=1')"
	if mandatory_search_terms[0] != '':
		for i in mandatory_search_terms:
			sql_query = sql_query + " AND " + where_clause_search_term.format(search_term=i)
	
	len_ost = len(optional_search_terms)
	if optional_search_terms[0] != '':
		sql_query = sql_query + " AND (" + where_clause_search_term.format(search_term=optional_search_terms[0])
		for i in range(1, len_ost):
			sql_query = sql_query + " OR " + where_clause_search_term.format(search_term=optional_search_terms[i])
		sql_query = sql_query + ")"
	return sql_query

# function to translate SQL query into Elasticsearch-compatible query
def translate_sql_to_es_query(sql_query):
	url = "http://localhost:9200/_sql/translate"
	headers = {'content-type': 'application/json'}
	data = {  "query": sql_query }
	data = json.dumps(data)
	response = requests.get(url, headers=headers, data=data)
	return response.text

# function to calculate and return tweet_count
def get_tweet_count(index_name, es_query):
	# run ES query and return results
	res= es.search(index='twitter', body=es_query)
	# store count agg data from ES query results
	tweet_count= res['aggregations']['groupby']['buckets'][0]['doc_count']
	return tweet_count

# create document with all necessary fields populated
def create_agg_doc(show, season_num, launch_date, studio_or_network, mandatory_search_terms, optional_search_terms, days_since_launch, tweet_count):
	agg_doc={}
	agg_doc['show_name']=show
	agg_doc['season_num']=season_num
	agg_doc['launch_date']=launch_date
	agg_doc['studio_or_network']=studio_or_network
	agg_doc['mandatory_search_terms']=mandatory_search_terms
	agg_doc['optional_search_terms']=optional_search_terms 
	agg_doc['days_since_launch']=days_since_launch
	agg_doc['total_tweets']=tweet_count
	return agg_doc

# insert doc into respective ES index
def insert_agg_doc_into_index(agg_doc, index_name):
	res=es.index(index=index_name,doc_type='_doc',body=agg_doc)
	print("\nInserted following document into index " + index_name + ":")
	pp.pprint(agg_doc)

# delete all docs related to show and season_num
def delete_show_from_index(show, season_num, index_name):
	url = "http://localhost:9200/{index_name}/_delete_by_query".format(index_name=index_name)
	headers = {'content-type': 'application/json'}
	sql_query = "SELECT * FROM {index_name} WHERE show_name='{show}' AND season_num={season_num}".format(index_name=index_name, show=show, season_num=season_num)
	es_query = translate_sql_to_es_query(sql_query)
	response = requests.post(url, headers=headers, data=es_query)
	if response.status_code >=200 and response.status_code < 300:
		print("\nSUCCESSFULLY Deleted documents from " + index_name + " related to " + show + " season " + str(season_num) + ".")
	else:
		print('\nDeletion attempt FAILED. Response code: ' + str(response.status_code))

# retrieve mandatory search terms for specific show in ES index
def get_es_mandatory_search_terms(show, season_num, index_name):
	sql_query = "SELECT mandatory_search_terms FROM {index_name} WHERE show_name='{show}' AND season_num={season_num} LIMIT 1".format(index_name=index_name, show=show, season_num=season_num)
	es_query = translate_sql_to_es_query(sql_query)
	res= es.search(index=index_name, body=es_query)
	if len( res['hits']['hits'] ) == 0:
		print("WARNING: This show/season_num combo({show} Season {season_num}) does not exist in {index_name}".format(index_name=index_name, show=show, season_num=season_num))
	else:
		return res['hits']['hits'][0]['_source']['mandatory_search_terms']

# retrieve optional search terms for specific show in ES index
def get_es_optional_search_terms(show, season_num, index_name):
	sql_query = "SELECT optional_search_terms FROM {index_name} WHERE show_name='{show}' AND season_num={season_num} LIMIT 1".format(index_name=index_name, show=show, season_num=season_num)
	es_query = translate_sql_to_es_query(sql_query)
	res= es.search(index=index_name, body=es_query)
	if len( res['hits']['hits'] ) == 0:
		print("WARNING: This show/season_num combo({show} Season {season_num}) does not exist in {index_name}".format(index_name=index_name, show=show, season_num=season_num))
	else:
		return res['hits']['hits'][0]['_source']['optional_search_terms']

# returns boolean indicating whether show/season combo exists in given index
def tv_show_exists_in_es_index(show, season_num, index_name):
	sql_query = "SELECT * FROM {index_name} WHERE show_name='{show}' AND season_num={season_num} LIMIT 1".format(index_name=index_name, show=show, season_num=season_num)
	es_query = translate_sql_to_es_query(sql_query)
	json_es_query = json.loads(es_query)
	if 'error' in json_es_query:
		return False
	res= es.search(index=index_name, body=es_query)
	if len( res['hits']['hits'] ) == 0:
		return False
	else:
		return True

# returns boolean indicating whether new search terms for show match previous search terms
def search_terms_match(show, season_num, index_name):
	es_man_search_terms = get_es_mandatory_search_terms(show, season_num, index_name)
	es_opt_search_terms = get_es_optional_search_terms(show, season_num, index_name)
	if set(mandatory_search_terms) == set(es_man_search_terms) and set(optional_search_terms) == set(es_opt_search_terms):
		return True
	else:
		return False

# returns boolean indication where data exists for a given day
def data_exists_for_specific_day(show, season_num, index_name, days_since_launch):
	sql_query = "SELECT optional_search_terms FROM {index_name} WHERE show_name='{show}' AND season_num={season_num} AND days_since_launch={days_since_launch} LIMIT 1".format(index_name=index_name, show=show, season_num=season_num, days_since_launch=days_since_launch)
	es_query = translate_sql_to_es_query(sql_query)
	res= es.search(index=index_name, body=es_query)
	if len( res['hits']['hits'] ) == 0:
		return False
	else:
		return True


if __name__ == "__main__":
	# indicate Google Sheets API (GS API)
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	# collect credentials to access GS API
	creds = ServiceAccountCredentials.from_json_keyfile_name('BuzzTracker-fe105c17a282.json', scope)
	# create client authorizing those creds
	client = gspread.authorize(creds)
	# open spreadsheet
	sheet = client.open('buzztracker_shows_data').sheet1
	# get all records for Google Sheet
	tv_shows_data = sheet.get_all_records()
	# get number of TV shows in sheet
	num_tv_shows = len( sheet.col_values(1)) - 1
	# create PrettyPrinter object
	pp = pprint.PrettyPrinter()
	# set index_name based on argument input in Terminal upon execution
	index_name = sys.argv[1]
	
	# connect to ES
	es = connect_to_es()
	if es is not None:
		create_index(es,index_name)

	# go through TV shows listed in Google Sheets
	for i in range(num_tv_shows):
		extract_gs_row_data(tv_shows_data[i])

		# if tv show doesn't exist in ES index yet, calc aggs for entire date range for show and insert into ES index
		if not tv_show_exists_in_es_index( show, season_num, index_name ):
			for j in range(days_since_launch_start.days, days_since_launch_end.days):
				sql_query = create_sql_query(show, season_num, mandatory_search_terms, optional_search_terms, (launch_date + timedelta(days=j)) )
				es_query = translate_sql_to_es_query(sql_query)
				tweet_count = get_tweet_count(index_name, es_query)
				agg_doc = create_agg_doc(show, season_num, launch_date, studio_or_network, mandatory_search_terms, optional_search_terms, j, tweet_count)
				insert_agg_doc_into_index(agg_doc, index_name)

		# if show exists in ES index but at time of this script's execution the new search terms do not match previous ones, new aggs must be re-calculated and replace old ones
		elif not search_terms_match(show, season_num, index_name):
			delete_show_from_index(show, season_num, index_name)
			for j in range(days_since_launch_start.days, days_since_launch_end.days):
				sql_query = create_sql_query(show, season_num, mandatory_search_terms, optional_search_terms, (launch_date + timedelta(days=j)) )
				es_query = translate_sql_to_es_query(sql_query)
				tweet_count = get_tweet_count(index_name, es_query)
				agg_doc = create_agg_doc(show, season_num, launch_date, studio_or_network, mandatory_search_terms, optional_search_terms, j, tweet_count)
				insert_agg_doc_into_index(agg_doc, index_name)

		else: 
			# if show already exists in ES index and search terms match but certain `days_since_launch` in its date window are missing data, calc aggs for those days and insert into ES
			for j in range(days_since_launch_start.days, days_since_launch_end.days):
				if not data_exists_for_specific_day(show, season_num, index_name, j):
					sql_query = create_sql_query(show, season_num, mandatory_search_terms, optional_search_terms, (launch_date + timedelta(days=j)) )
					es_query = translate_sql_to_es_query(sql_query)
					tweet_count = get_tweet_count(index_name, es_query)
					agg_doc = create_agg_doc(show, season_num, launch_date, studio_or_network, mandatory_search_terms, optional_search_terms, j, tweet_count)
					insert_agg_doc_into_index(agg_doc, index_name)
