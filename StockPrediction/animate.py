"""
@Author: Ranjith G C
@Date: 2021-09-12
@Last Modified by: Ranjith G C
@Last Modified time: 2021-09-12 
@Title : Program Aim is to work visualizing stock data using flask.
"""

from flask import Flask,render_template,url_for,request,redirect, make_response
import json
from flask import Flask, render_template, make_response
import pandas as pd
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('Stock Data').getOrCreate()
from pyspark.ml.regression import LinearRegressionModel
from datetime import datetime

print('count')
app = Flask(__name__)
model = LinearRegressionModel.load('lrm_model')
data=spark.read.parquet("test_data")
prediction=model.transform(data)
output=prediction.toPandas()
count=len(output["time"])
index_count = 0

def getData():   
    global count
    if index_count <= count:
        print(index_count)
        return int(datetime.strptime(output.loc[index_count, "time"], '%Y-%m-%d %H:%M:%S').strftime('%s')) * 1000, output.loc[index_count, "close"], output.loc[index_count, "prediction"]

@app.route('/', methods=["GET", "POST"])
def main():
    return render_template('index (1).html')

@app.route('/data', methods=["GET", "POST"])
def data():

    global index_count
    time,close,prediction = getData()

    data = [time, close, prediction]

    response = make_response(json.dumps(data))

    response.content_type = 'application/json'

    index_count += 1

    return response

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080)