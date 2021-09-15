"""
@Author: Ranjith G C
@Date: 2021-09-12
@Last Modified by: Ranjith G C
@Last Modified time: 2021-09-12 
@Title : Program Aim is to work with Stock live data using kafkaconsumer
"""

import pandas as pd
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from kafka import KafkaConsumer
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions
from logging_handler import logger

sc = SparkContext()
sqlContext = SQLContext(sc)

try:
    ModelPath = "lrm_model"
    LoadModel = LinearRegressionModel.load(ModelPath)
except Exception as e:
    logger.info(e)

try:
    Consumer = KafkaConsumer('stock_data')
except Exception as e:
    logger.info(e)

def StockPricePrediction(LoadModel):
    try:
        for message in Consumer:
            res = json.loads(message.value.decode('utf-8'))
            dlist = list(res.values())
            df = pd.DataFrame([dlist], columns=['Open', 'High','Low','Close','Volume'])
            df = df.astype(float)
            spark_df = sqlContext.createDataFrame(df)
            vectorAssembler = VectorAssembler(inputCols=['Open', 'High', 'Low'], outputCol='features')
            df_vect = vectorAssembler.transform(spark_df)
            df_vect_features = df_vect.select(['features', 'Close'])
            predictions = LoadModel.transform(df_vect_features)
            predictions.select("prediction", "Close", "features").show()
            predict_value = predictions.select(functions.round(predictions["prediction"], 2).alias("prediction")).collect()[0].__getitem__("prediction")
            close_value = predictions.select('Close').collect()[0].__getitem__('Close')
            print(message.key)
            date_time = message.key.decode('utf-8')
            return predict_value, close_value, date_time
    except Exception as e:
        logger.info(e)



ss = StockPricePrediction(LoadModel)

print(ss)