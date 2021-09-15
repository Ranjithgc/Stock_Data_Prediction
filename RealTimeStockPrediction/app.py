"""
@Author: Ranjith G C
@Date: 2021-09-12
@Last Modified by: Ranjith G C
@Last Modified time: 2021-09-12 
@Title : Program Aim is to work with Stock live data and producing by using kafkaproducer
"""

from flask import Flask, render_template, make_response
from datetime import datetime
import time
import json
import Consumer
import sys
from logging_handler import logger

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/data')
def data():
    try:
        pred_price, actual_price, date_time = Consumer.StockPricePrediction(Consumer.LoadModel)
        date_time = int(datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').strftime('%s')) * 1000
        data = [date_time, pred_price, actual_price]
        response = make_response(json.dumps(data))
        response.content_type = 'application/json'
        time.sleep(2)
        return response
    except Exception as e:
        logger.info(e)
        sys.exit(1)


if __name__ == ("__main__"):
    app.run(debug=True, host='127.0.0.1', port=5050, passthrough_errors=True)