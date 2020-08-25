
from flask import jsonify
from server import app
import threading
import time
import datetime
import logging
import pandas as pd

def func():
    logging.info('thread started')




@app.route("/model")
def model():
    """health route"""
    filename=''
    with open(filename) as file:
        df = pd.read_csv(file)
    while True:
        state = {"datetime": datetime.datetime.now()}
        time.sleep(1)
        return jsonify(state['datetime'])