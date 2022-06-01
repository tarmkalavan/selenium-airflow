import numpy as np
import pandas as pd
import joblib
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.metrics import mean_squared_error, RootMeanSquaredError
from tensorflow.keras.optimizers import Adam
from db_func import get_prev_5_hours, insert_hourly_pred
from os import listdir
import os
from datetime import timedelta

def build_model(prev_times, n_features):
    p = 0.2
    model = Sequential()

    model.add(LSTM(256, input_shape=(prev_times, n_features), return_sequences=True))
    model.add(Dropout(p))

    model.add(LSTM(256, input_shape=(prev_times, n_features), return_sequences=False))
    model.add(Dropout(p))

    model.add(Dense(128, activation='relu'))
    model.add(Dense(1, activation='relu'))

    return model

def predict(prev_times=5, next_times=24, **kwargs):
  print(f' cur working dir: {os.getcwd()}')
  print([f for f in listdir(os.path.dirname('./'))])

  feature_columns = ['SO2', 'NO2', 'CO', 'PM2.5']
  norm_columns = ['norm_'+ col for col in ['SO2', 'NO2', 'CO', 'PM2.5']]
  all_columns = ['date'] + feature_columns

  # build and load models
  models = {}
  for col in feature_columns:
    models[col] = build_model(prev_times, len(feature_columns)) 
    models[col].load_weights(rf'./dags/model_weights/model_{col}.hdf5')
    models[col].compile(loss=mean_squared_error, optimizer=Adam(learning_rate=0.001), metrics=[RootMeanSquaredError(name='rmse')])
  
  # get data for scraping
  cur_datetime = kwargs['execution_date'].replace(microsecond=0, second=0, minute=0) # UTC
  cur_datetime = pd.to_datetime(cur_datetime.strftime('%Y-%m-%d %H:%M:%S'))
  cur_datetime = cur_datetime + timedelta(hours=7) # Convert to TH timezone
  print(f'cur_datetime: {cur_datetime}')
  df = get_prev_5_hours(cur_datetime)
  if (len(df) != prev_times): # abort if not enough data exist
    print('aborting, not enough data')
    return 
  df['date'] = pd.to_datetime(df['date'])

  # scale data
  scaler = joblib.load(rf'./dags/scaler.gz')
  df[norm_columns] = scaler.transform(df[feature_columns])

  for i in range(next_times):
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print(f'running i: {i}')

    # use latest data (5 consecutive timesteps)
    tmp = df.tail(prev_times)
    start_date = tmp['date'].iloc[0]
    date_range = pd.date_range(start_date, periods=prev_times, freq='H')
    
    # if a timestep is missing then stop predicting
    if np.sum(tmp['date'] == date_range) != len(tmp):
      print('aborting, wrong timestep')
      print(tmp['date'])
      print(date_range)
      return

    # use normalized features
    X = tmp[norm_columns].values.reshape(1, prev_times, -1)
    
    # make next timestep
    pred_datetime = cur_datetime + pd.DateOffset(hours=i+1)
    print(f'pred_datetime: {pred_datetime}')
    predictions = {}
    for col in feature_columns:
      predictions[col] = models[col].predict(X, batch_size=1).item()
    print(f'predictions: {predictions}')

    # append predicted data to predict next timestep
    tmp_df = pd.DataFrame([[pred_datetime, predictions['SO2'], predictions['NO2'], predictions['CO'], predictions['PM2.5']]], columns=all_columns)
    tmp_df[norm_columns] = scaler.transform(tmp_df[feature_columns])
    df = df.append(tmp_df, ignore_index=True)

    # add predicted data to db
    insert_hourly_pred(cur_datetime, pred_datetime, predictions['PM2.5'], predictions['CO'], predictions['SO2'], predictions['NO2'])