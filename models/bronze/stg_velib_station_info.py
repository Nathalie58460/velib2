import requests
import pandas as pd

def model(dbt, session):
    dbt.config(
        materialized="table"
    )
 
    url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()  # dict ou liste de dict
 
    # Exemple avec pandas → Spark
    pdf = pd.DataFrame(data)
    sdf = session.createDataFrame(pdf)  # session = SparkSession Databricks

    return sdf