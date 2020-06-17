#!/usr/bin/python3

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 16),
    'retries': 3,
    'retry_delay': timedelta(seconds=300)
}

dag = DAG(
  dag_id='buzztracker',
  description='buzztracker',
  schedule_interval='0/10 * * * *',
  max_active_runs=1,
  concurrency=1,
  catchup=False,
  default_args=default_args)

check_search_terms = BashOperator(
	task_id='check_search_terms',
	bash_command='python3 /home/ubuntu/Airflow/check_if_search_terms_updated.py',
	dag=dag)

update_es_index = BashOperator(
	task_id='update_es_index',
	bash_command="python3 /home/ubuntu/Processing/load_agg_es_index.py 'twitter_agg'",
	dag=dag)

check_search_terms >> update_es_index
