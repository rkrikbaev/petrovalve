from flask import jsonify, request
from werkzeug.utils import secure_filename
from server import app
import pandas as pd
import threading
import time
import datetime
import logging
from pkg.common import LinktoInfluxDB

influxdb_client = LinktoInfluxDB(host='http://165.232.73.158/',
                                 port='8086',
                                 database='m-metrics',
                                 measurement='valve',
                                 user='admin',
                                 password='6fcf2a66ac6a38d40ec18f9ce8ea7534e76d877359f22a57')

@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['file']
        f.save(secure_filename(f.filename))
        data = request.json()
        df = pd.read_json(data)
        logging.debug(f'get some data {df.head()}')
        yield print(df.head())

        influxdb_client.put_data(data=df)

