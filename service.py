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
        # df =pd.read_csv(path)
        _generate_data(influxdb_client, df)
        logger.debug()
    except Exception as error:
        logger.error(error)


def _generate_data(influxdb_client, df):
    rows_df, col_df = df.shape

    for row_of_df in range(rows_df):

        # replace win new dattime
        # df.set_index('ts', inplace=True)
        date_time = df.index.values[row_of_df]
        ts = (date_time - np.datetime64('1970-01-01T00:00:00'))/np.timedelta64(1, 's')
        dt = datetime.utcfromtimestamp(ts)
        dt_now = date.today()
        new_datetime = dt.replace(dt_now.year, dt_now.month, dt_now.day)
        new_date = np.datetime64(new_datetime)

        # new datafram to write
        df_new = pd.DataFrame(data=[df.iloc[row_of_df]])
        df_new.index = [new_date]
        columns = df_new.columns
        logger.debug(df_new.shape)
        logger.debug(df_new.columns)
        while (datetime.today()-new_datetime).total_seconds() < 10:
            logger.debug('upload all data up to now')
            time.sleep(5)
        else:
            try:
                influxdb_client.put_data(data=df_new, columns=columns)
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


                    df_clean_columns = list(df.columns)
                    df_clean = df[df_clean_columns].copy()
                    list_fo_df.append(df_clean)
    result_df = pd.concat(list_fo_df)
    # delete columns with all NaN values
    result_df.dropna(axis=1, how='all',inplace=True)
    # filling NaN
    result_df.fillna(method='ffill', inplace=True)
    result_df.fillna(method='backfill', inplace=True)
    all_columns = list(result_df.columns)
    logger.debug(result_df.columns)
    logger.debug(result_df.describe)
    logger.debug(result_df.shape)
    logger.debug(result_df.iloc[0])
    # result_df.to_csv('tmp/data/alldata.csv')
    return result_df

while True:
    flow(path='tmp')
