from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from scraper_func import scrape
from db_func import insert_hourly_data
from model_func import predict

COORDS = { # city: [lat, long]
    "BKK": [13.723, 100.536]
}
FEATS = {
    'co':  'cosc',
    'so2': 'so2smass',
    'no2': 'no2',
    'pm': 'pm2.5'
  }

default_args = {
    'owner': 'tarmkalavan',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

with DAG('pm2.5-predictor',
        schedule_interval = '15 * * * *',
        start_date=datetime(2022, 4, 20, 0, 0),
        catchup=True,
        max_active_runs=1,
        default_args=default_args) as dag:
    start = DummyOperator(task_id='start_task')
    city = "BKK"
    lat, long = COORDS[city]
    scraper_co = PythonOperator(task_id='scraper_co',
                                python_callable=scrape,
                                op_args=['co', lat, long]
                                )
    scraper_no2 = PythonOperator(task_id='scraper_no2',
                                python_callable=scrape,
                                op_args=['no2', lat, long]
                                )
    scraper_so2 = PythonOperator(task_id='scraper_so2',
                                python_callable=scrape,
                                op_args=['so2', lat, long]
                                )
    scraper_pm = PythonOperator(task_id='scraper_pm',
                                python_callable=scrape,
                                op_args=['pm', lat, long]
                              )
    save_real = PythonOperator(task_id='save_real',
                               python_callable=insert_hourly_data,
                               op_args=[scraper_pm.output, 
                                        scraper_co.output, 
                                        scraper_so2.output, 
                                        scraper_no2.output]
                              )
    predict_worker = PythonOperator(task_id='predict_worker',
                                    python_callable=predict)

    start >> scraper_co >> scraper_no2 >> scraper_so2 >> scraper_pm >> save_real >> predict_worker