import pandas as pd
import psycopg2
from datetime import timedelta
import os
from dotenv import load_dotenv

load_dotenv()
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_SCRAPE = os.getenv('PG_SCRAPE')
PG_PREDICT = os.getenv('PG_PREDICT')

def insert_hourly_data(pm, co, so2, no2, **kwargs):
    connection = psycopg2.connect(user=PG_USER,
                                  password=PG_PASSWORD,
                                  host=PG_HOST,
                                  port=PG_PORT,
                                  database=PG_DATABASE)
    cursor = connection.cursor()
    timestamp = kwargs['execution_date'].replace(microsecond=0, second=0, minute=0) # UTC
    timestamp = (timestamp + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S') # +7 to TH timezone

    # query -> insert or update existing row if exists
    query = f"""INSERT INTO public."{PG_SCRAPE}"(date, "pm2.5", "co", "so2", "no2") 
            VALUES ('{timestamp}', {pm}, {co}, {so2}, {no2})
            ON CONFLICT (date) 
            DO 
                UPDATE SET date='{timestamp}', "pm2.5"={pm}, "co"={co}, "so2"={so2}, "no2"={no2};"""
    
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(f'inserted into "real" table: {timestamp}, {pm}, {co}, {so2}, {no2}')
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    cursor.execute(query)

    connection.commit()
    connection.close()

def get_prev_5_hours(timestamp):
    connection = psycopg2.connect(user=PG_USER,
                                  password=PG_PASSWORD,
                                  host=PG_HOST,
                                  port=PG_PORT,
                                  database=PG_DATABASE)

    start = timestamp - pd.DateOffset(hours=4)
    end = timestamp

    query = f""" SELECT * FROM public."{PG_SCRAPE}" 
                WHERE date >= '{start}' AND date <= '{end}' 
                ORDER BY date ASC """

    df = pd.read_sql(query, connection)
    df.columns = ['id', 'date', 'PM2.5', 'CO', 'SO2', 'NO2']
    df = df.drop(columns='id', axis=1)
    connection.close()

    return df

def insert_hourly_pred(cur_datetime,pred_datetime,pm,co,so2,no2):
    connection = psycopg2.connect(user=PG_USER,
                                  password=PG_PASSWORD,
                                  host=PG_HOST,
                                  port=PG_PORT,
                                  database=PG_DATABASE)
    pm = round(pm, 2)
    co = round(co, 2)
    so2 = round(so2, 2)
    no2 = round(no2, 2)

    # init cursor
    cursor = connection.cursor()
    cur_datetime = (cur_datetime).strftime('%Y-%m-%d %H:%M:%S')
    pred_datetime = (pred_datetime).strftime('%Y-%m-%d %H:%M:%S')

    # example: INSERT insert_hourly_pred('2020-08-24 00:00:00', '2020-08-24 00:00:00', 200, 200, 200, 200)
    query = f""" INSERT INTO public."{PG_PREDICT}" ( date, pred_date,"pm2.5", "co", "so2", "no2") 
            VALUES ('{cur_datetime}', '{pred_datetime}', {pm}, {co}, {so2}, {no2})
            ON CONFLICT (date, pred_date) 
	        DO 
		        UPDATE SET date = '{cur_datetime}', pred_date = '{pred_datetime}', "pm2.5"={pm}, "co"={co}, "so2"={so2}, "no2"={no2};
            """

    cursor.execute(query)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(f'inserted into "{PG_PREDICT}" table: {cur_datetime}, {pred_datetime}, {pm} ,{co}, {so2}, {no2}')
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    connection.commit()
    connection.close()