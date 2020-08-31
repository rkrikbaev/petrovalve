from influxdb import DataFrameClient, exceptions

from  pkg.logging import logger
import pandas as pd
import numpy as np
from datetime import datetime, date
import time
import os

class LinktoInfluxDB():

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
            logger.debug(f'get some data {list_of_df.head()}')
            return list_of_df
        except Exception as e:
            logger.error(e)

    def put_data(self, data, columns):
        try:
            self.client.write_points(dataframe=data,
                                     database=self.database,
                                     measurement=self.measurement,
                                     field_columns=columns,
                                     batch_size=5000,
                                     protocol='line')
            logger.debug(f'put some data')
        except Exception as e:
            logger.error(e)