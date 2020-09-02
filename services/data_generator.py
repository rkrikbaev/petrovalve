import pandas as pd
import numpy as np
import time
import datetime
from pkg.common import logger, LinktoInfluxDB
import os
from datetime import date, datetime

JW_1101 = ['Time']
JW_1101_number =  \
            '190-AT1001-AirTemp_(C),' \
            '990-PT-1104A_(barg),' \
            '990-PT-1104B_(barg),' \
            '990-PT-1104C_(barg),' \
            '990-LT-1131_(mm),' \
            '990-LT-1132_(mm),' \
            '990-LT-1133_(mm),' \
            '990-LT-1134_(mm),' \
            '990-LT-1135_(mm),' \
            '990-PT-1109_(barg),' \
            '190-ZSI-1102,' \
            '190-ZT-1102_(%),' \
            '990-PT-1129_(barg),' \
            '190-ZSI-1101,' \
            '190-ZT-1101_(%)'.split(',')
JW_1101_state = '990-PC-1101A-H1,' \
                '990-PC-1101B-H2,' \
                '990-PC-1102-H1,' \
                '990-PSHL-1100,' \
                '190-ZSL-1102,' \
                '190-ZSH-1102,' \
                '190-ZSL-1101,' \
                '190-ZSH-1101'.split(',')

JW_1201 = ['Time']
JW_1201_number = \
            '990-PT-1204A_(barg),' \
            '990-PT-1204B_(barg),' \
            '990-PT-1204C_(barg),' \
            '990-LT-1231_(mm),' \
            '990-LT-1232_(mm),' \
            '990-LT-1233_(mm),' \
            '990-LT-1234_(mm),' \
            '990-LT-1235_(mm),' \
            '990-PT-1209_(barg),' \
            '190-ZSI-1202,' \
            '190-ZT-1202_(%),' \
            '990-PT-1229_(barg),' \
            '190-ZSI-1201,' \
            '190-ZT-1201_(%)'.split(',')
JW_1201_state = '990-PC-1201A-H1,' \
                '990-PC-1201B-H2,' \
                '990-PC-1202-H1,' \
                '990-PSHL-1200,' \
                '190-ZSL-1202,' \
                '190-ZSH-1202,' \
                '190-ZSL-1201,' \
                '190-ZSH-1201'.split(',')


JW_1202_time = ['Time']
JW_1202_number = \
            '990-PT-1254A_(barg),' \
            '990-PT-1254B_(barg),' \
            '990-PT-1254C_(barg),' \
            '990-LT-1281_(mm),' \
            '990-LT-1282_(mm),' \
            '990-LT-1283_(mm),' \
            '990-PT-1259_(barg),' \
            '190-ZSI-1203,' \
            '190-ZT-1203_(%),' \
            '990-PT-1279_(barg),' \
            '190-ZSI-1204,' \
            '190-ZT-1204_(%)'.split(',')
JW_1202_state = \
            '990-PC-1206A-H1,' \
            '990-PC-1206B-H2,' \
            '990-PC-1207-H1,' \
            '990-PSHL-1250,' \
            '190-ZSL-1203,' \
            '190-ZSH-1203,' \
            '190-ZSL-1204,' \
            '190-ZSH-1204'.split(',')

JW_2101_time = ['Time']
JW_2101_number = 'Time,190-AT2001-AirTemp_(C),' \
          '990-PC-2101A-H1,' \
          '990-PC-2101B-H2,' \
          '990-PC-2102-H1,' \
          '990-PT-2104A_(barg),' \
          '990-PT-2104B_(barg),' \
          '990-PT-2104C_(barg),' \
          '990-PSHL-2100,' \
          '990-LT-2131_(mm),' \
          '990-LT-2132_(mm),' \
          '990-LT-2133_(mm),' \
          '990-LT-2134_(mm),' \
          '990-LT-2135_(mm),' \
          '990-PT-2109_(barg),' \
          '190-ZSL-2101,' \
          '190-ZSH-2101,' \
          '190-ZSI-2101,' \
          '190-ZT-2101_(%),' \
          '990-PT-2129_(barg)'.split(',')
JW_2101_state = 'Time,190-AT2001-AirTemp_(C),' \
          '990-PC-2101A-H1,' \
          '990-PC-2101B-H2,' \
          '990-PC-2102-H1,' \
          '990-PT-2104A_(barg),' \
          '990-PT-2104B_(barg),' \
          '990-PT-2104C_(barg),' \
          '990-PSHL-2100,' \
          '990-LT-2131_(mm),' \
          '990-LT-2132_(mm),' \
          '990-LT-2133_(mm),' \
          '990-LT-2134_(mm),' \
          '990-LT-2135_(mm),' \
          '990-PT-2109_(barg),' \
          '190-ZSL-2101,' \
          '190-ZSH-2101,' \
          '190-ZSI-2101,' \
          '190-ZT-2101_(%),' \
          '990-PT-2129_(barg)'.split(',')

JW_2301_time = ['Time']
JW_2301_number =  \
          '990-PT-2304A_(barg),' \
          '990-PT-2304B_(barg),' \
          '990-PT-2304C_(barg),' \
          '990-LT-2331_(mm),' \
          '990-LT-2332_(mm),' \
          '990-LT-2333_(mm),' \
          '990-LT-2334_(mm),' \
          '990-LT-2335_(mm),' \
          '990-PT-2309_(barg),' \
          '190-ZSI-2201,' \
          '190-ZT-2201_(%),' \
          '990-PT-2329_(barg)'.split(',')
JW_2301_state = \
    '990-PC-2301A-H1,' \
    '990-PC-2301B-H2,' \
    '990-PC-2302-H1,' \
    '990-PSHL-2300,' \
    '190-ZSL-2201,' \
    '190-ZSH-2201'.split(',')

# valves = [{'JW-1101':[], 'JW-1201', 'JW-1202', 'JW-2101', 'JW-2301']

valve_states = JW_1101_state+JW_1201_state+JW_1202_state+JW_2101_state+JW_2301_state

influxdb_client = LinktoInfluxDB(host='157.230.120.158',
                                 port='8086',
                                 database='m-metrics',
                                 measurement='all_data',
                                 user='admin',
                                 password='admin',
                                 time_presicion='ms')


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

        # receive customer date as unix time
        ts = df.index.values[row_of_df]

        # replace with current date
        dt_now = date.today()
        dt = datetime.utcfromtimestamp(ts)
        datetime_new = dt.replace(dt_now.year, dt_now.month, dt_now.day)
        # to timestamp
        ts_sec = datetime_new.timestamp()
        # new dataframe object with new datetime as index
        df_new = pd.DataFrame(data=[df.iloc[row_of_df]])

        df_new.index = [np.datetime64(datetime_new)]
        columns = df_new.columns

        logger.debug(df_new.index)
        logger.debug(datetime.today().timestamp())
        logger.debug(ts_sec)

        while (datetime.today().timestamp()-ts_sec) < 10:

            time.sleep(5)
        else:
            try:
                influxdb_client.put_data(data=df_new, columns=columns)
            except Exception as e:
                logger.error(e)


def _prep_df(path):

    folders = os.listdir(path)

    list_of_df = []

    for folder in folders:
        if folder != '.DS_Store':
            for file in os.listdir(f'{path}/{folder}'):
                if file.endswith(".csv"):
                    logger.debug(f'process .csv file {file}')
                    path_to_file = os.path.join(f'{path}/{folder}', file)

                    frame = pd.read_csv(path_to_file, sep=',', low_memory=False, encoding='unicode_escape')
                    df_columns = list(frame.columns)
                    df_columns.pop()

                    frame['ts'] = pd.to_datetime(frame['Time'], format='%Y.%m.%d %H:%M:%S.%f')
                    # df['dt'] = df['dt'].apply(lambda x: pd.Timestamp(x))
                    frame['ts'] = frame['ts'].apply(lambda x: pd.Timestamp(x).timestamp())
                    # logger.debug(df.describe)
                    frame.set_index('ts', inplace=True)

                    # select data boolean dtype
                    # logger.debug(frame.columns)

                    columns_state = list(filter(lambda x: x in valve_states, list(frame.columns)))
                    frame_state = frame[columns_state].copy()
                    frame_state.columns = [f'{name}_state' for name in list(frame_state.columns)]
                    # logger.debug(frame_state.columns)
                    # logger.debug(frame_state.head)
                    # logger.debug(frame_state.describe())

                    # filling NaN for a number dtypes
                    frame.fillna(method='ffill', inplace=True)
                    frame.fillna(method='backfill', inplace=True)

                    df_full = pd.concat([frame, frame_state])
                    # logger.debug(df_full.columns)

                    df_clean_bool_columns = list(df_full.columns)
                    df_clean_bool = df_full[df_clean_bool_columns].copy()
                    list_of_df.append(df_clean_bool)

    result_df = pd.concat(list_of_df)
    # delete columns with all NaN values

    result_df.dropna(axis=1, how='all',inplace=True)
    # filling NaN
    # result_df.fillna(method='ffill', inplace=True)
    # result_df.fillna(method='backfill', inplace=True)
    all_columns = list(result_df.columns)
    # logger.debug(result_df.columns)
    # logger.debug(result_df.describe)
    # logger.debug(result_df.shape)
    # logger.debug(result_df.iloc[0])
    # result_df.to_csv('local/alldata.csv')
    return result_df

