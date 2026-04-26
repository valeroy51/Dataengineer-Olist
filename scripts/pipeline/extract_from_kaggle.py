import kaggle
import zipfile
import os
from confluent_kafka  import Producer
import json
import pandas as pd

import sys
sys.path.append("/app")

def scrapeKaggle(kagglePath):
    kaggle.api.authenticate()
    
    savePath="./data/bronze"
    os.makedirs(savePath, exist_ok=True)
    
    kaggle.api.dataset_download_files(kagglePath, path=savePath, unzip=False)
    
    zipPath=kagglePath.split("/")[-1]+".zip"
    name=os.path.join(savePath,zipPath)
    
    return name

def unzipData(dataPath):
    bronzePath = "./data/bronze"
    
    os.makedirs(bronzePath, exist_ok=True)
    
    with zipfile.ZipFile(dataPath,"r") as zip:
        zip.extractall(bronzePath)
    
    files=[]
    
    for file in os.listdir(bronzePath):
        if not file.endswith(".zip"):
            file = file.replace(".csv","")
            files.append(file)
            print(file)
        else:
            pass

    return files

def publishData(topicList,server):
    
    keyTopic = {"olist_customers_dataset":["customer_id"],
                "olist_geolocation_dataset":["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng"],
                "olist_orders_dataset":["order_id"],
                "olist_order_items_dataset":["order_item_id"],
                "olist_order_payments_dataset":["order_id", "payment_sequential"],
                "olist_order_reviews_dataset":["review_id"],
                "olist_products_dataset":["product_id"],
                "olist_sellers_dataset":["seller_id"],
                "product_category_name_translation":["product_category_name"]
                }
    
    def report(error,msg):
        if error is not None:
            print (f" Terjadi error : {error}\n")
        else:
            print(f"message berhasil dipublish ke {msg.topic()} partition {msg.partition()} offset {msg.offset()}\n")
    
    produce = Producer({
        'bootstrap.servers': server,
        'enable.idempotence': True,
    })
    
    bronzePath = "./data/bronze"
        
    dataPath = os.path.join(bronzePath,f"{topicList}.csv")
    data = pd.read_csv(dataPath)
    
    for row in data.itertuples(index=False):
        message= {
            column: (None if pd.isna(value) else value)
            for column, value in zip(data.columns, row)
        }
        columns = keyTopic.get(topicList,[])
        if columns:
            key = "_".join(str(row[col]) for col in columns if col in row)
        else:
            key = str(row.iloc[0])
        
        while True:
            try:
                produce.produce(topicList, 
                                key=key.encode(),
                                value=json.dumps(message).encode("utf-8"),
                                callback=report
                                )
                break
            except BufferError:
                produce.poll(1)
        
        produce.poll(0)
    
    produce.flush()