from flask import jsonify, request
from server import app
import pandas as pd
import numpy as np
import time
import datetime
from pkg.common import logger, LinktoInfluxDB
import os
from datetime import date, datetime



influxdb_client = LinktoInfluxDB(host='157.230.120.158',
                                 port='8086',
                                 database='m-metrics',
                                 measurement='all_data',
                                 user='admin',
                                 password='admin')


def flow(path):
    try:
        df = _prep_df(path)
        _generate_data(influxdb_client, df)
        logger.debug()
    except Exception as error:
        logger.error(error)


def _generate_data(influxdb_client, df):
    rows_df, col_df = df.shape

    for row_of_df in range(rows_df):

        # replace win new dattime
        date_time = df.index.values[row_of_df]
        ts = (date_time - np.datetime64('1970-01-01T00:00:00'))/np.timedelta64(1, 's')
        dt = datetime.utcfromtimestamp(ts)
        dt_now = date.today()
        new_datetime = dt.replace(dt_now.year, dt_now.month, dt_now.day)
        new_date = np.datetime64(new_datetime)

        # new datafram to write
        df_new = pd.DataFrame(data=[df.iloc[row_of_df]])
        df_new.index = [new_date]

        while (datetime.today()-new_datetime).total_seconds() < 10:
            logger.debug('upload all data up to now')
            time.sleep(5)
        else:
            try:
                influxdb_client.put_data(data=df_new, columns=df_new.columns)
            except Exception as e:
                logger.error(e)


def _prep_df(path):

    folders = os.listdir(path)

    list_fo_df = []
    for folder in folders:
        if folder != '.DS_Store':
            for file in os.listdir(f'{path}/{folder}'):
                if file.endswith(".csv"):
                    path_to_file = os.path.join(f'{path}/{folder}', file)

                    df = pd.read_csv(path_to_file, sep=',', low_memory=False, encoding='unicode_escape')
                    df_columns = list(df.columns)
                    df_columns.pop()

                    df['ts'] = pd.to_datetime(df['Time'], format='%Y.%m.%d %H:%M:%S.%f')
                    df['ts'] = df['ts'].apply(lambda x: pd.Timestamp(x))
                    df.set_index('ts', inplace=True)

                    df.fillna(method='ffill', inplace=True)
                    df.fillna(method='backfill', inplace=True)
                    df.fillna(value=0, inplace=True)
                    df_clean = df[df_columns].copy()
                    list_fo_df.append(df_clean)
    result_df = pd.concat(list_fo_df)
    logger.debug(result_df.describe())
    return result_df


@app.route('/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'GET':
        path = 'tmp'
        flow(path)
        logger.debug('up tread')
    if request.method == 'POST':
        pass
    return jsonify('thread started')