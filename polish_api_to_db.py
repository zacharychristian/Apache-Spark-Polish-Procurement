from pyspark.sql import SparkSession
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from deep_translator import GoogleTranslator
import findspark
import json
import os

#Set timezone variable to get rid of warnings
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


#Get Data for polish procurement data
url = "https://tenders.guru/api/pl/tenders"
try:
    r = requests.get(url)
except requests.ConnectionError as ce:
    logging.error(f"There was an error with the request, {ce}")
    sys.exit(1)


# Go through JSON data and put data I want in database into a list to be made into a pandas df
id = []
title = []
category = []
description = []
awarded_value_eur = []
awarded = []

#Translate Polish into English using deep_translator and load necessary data into its list
for key, value in r.json().items():

    # Skip parts of json that arent payload
    if(isinstance(value, int)== False):
        for sub_value in value:
            id.append(json.dumps(sub_value['id']))
            title.append(GoogleTranslator(source='auto', target='english').translate(text=json.dumps(sub_value['title'])))
            category.append(json.dumps(sub_value['category']))
            description.append(GoogleTranslator(source='auto', target='english').translate(text=json.dumps(sub_value['description'])))
            awarded_value_eur.append(json.dumps(sub_value['awarded_value_eur']))
            awarded.append(json.dumps(sub_value['awarded']))


#Put lists into one pandas dataframe
df = pd.DataFrame(list(zip(id,title,category,description, awarded_value_eur, awarded)), 
                  columns = ['id','title','category', 'description', 'awarded_value_eur', 'awarded'])


findspark.init()

#Make a session of Spark to load data into PostgreSQL database
spark = SparkSession.builder \
    .appName("PySpark_to_PostgreSQL") \
    .getOrCreate()


#Make spark dataframe out of pandas df
sdf = spark.createDataFrame(df)


#Database credentials
jdbc_url = "jdbc:postgresql://host.docker.internal:5432/Polish_Tenders"
connection_properties = {
    "user": "postgres",
    "password": "!@dEMOrANCH261948",############################
    "driver": "org.postgresql.Driver"
}


# Load data into db
sdf.write.jdbc(
    url=jdbc_url,
    table="Data",
    mode="append",  # Or "append", "ignore", "errorifexists", "overwrite"
    properties=connection_properties
)

spark.stop()