from influxdb import DataFrameClient, exceptions
import logging
import pandas as pd

class LinktoInfluxDB:

    def __init__(self, host, port, user, password, database, measurement):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.measurement = measurement

        self.client = DataFrameClient(self.host, self.port, self.database)

    def get_data(self, query):
        try:
            list_of_df = self.client.query(query)
            logging.debug(f'get some data {list_of_df.head()}')
            return list_of_df
        except Exception as e:
            logging.error(e)

    def put_data(self, data):
        try:
            self.client.write_points(dataframe=data,
                                     measurement=self.measurement,
                                     tags={'a':'b'},
                                     field_columns='data',
                                     time_precision='s',
                                     protocol='line')
            logging.debug(f'put some data')
        except Exception as e:
            logging.error(e)



influxdb_client = LinktoInfluxDB(host='157.230.120.158',
                                 port='8086',
                                 database='m-metrics',
                                 measurement='valve',
                                 user='admin',
                                 password='6fcf2a66ac6a38d40ec18f9ce8ea7534e76d877359f22a57')

# Найти и собрать в один Dataframe все данные
import os
path='/Users/rustamkrikbayev/PycharmProjects/sirio-valve/tmp'
list_of_frames = []
df = pd.DataFrame
folders = os.listdir(path)
for folder in folders:
    for file in os.listdir(f'{path}/{folder}'):
        if file.endswith(".csv"):
            path_to_file = os.path.join(f'{path}/{folder}', file)

            frame = pd.read_csv(path_to_file, sep=',', lo_memory=False)
            frame.set_index('Time', inplace=True)
            print(frame.head())

            list_of_frames.append(frame)

df = pd.concat(list_of_frames)

# df_clean = frame.where(pd.notnull(frame), None)
df.fillna(method='ffill', inplace=True)
df.fillna(method='backfill', inplace=True)
df.fillna(value=0, inplace=True)
print(df.head(25))
print(df.columns)

# Убрать данные с не подписаных полей
columns = ['190-AT1001-AirTemp', '990-PC-1101A-H1', '990-PC-1101B-H2',
           '990-PC-1102-H1', '990-PT-1104A (barg)', '990-PT-1104B (barg)',
           '990-PT-1104C (barg)', '990-PSHL-1100', '990-LT-1131 (mm)',
           '990-LT-1132 (mm)', '990-LT-1133 (mm)', '990-LT-1134 (mm)',
           '990-LT-1135 (mm)', '990-PT-1109 (barg)', '190-ZSL-1102',
           '190-ZSH-1102', '190-ZSI-1102', '190-ZT-1102 (%)', '990-PT-1129 (barg)',
           '190-ZSL-1101', '190-ZSH-1101', '190-ZSI-1101', '190-ZT-1101 (%)']
df_prod = df[columns].copy



# df = pd.DataFrame(data=df.values,
#                   index=pd.date_range(start='2020-08-23',
#                                       periods=30, freq='H'), columns=['data'])
try:
    influxdb_client.put_data(data=df)
except Exception as e:
    logging.error(e)