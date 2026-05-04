import kaggle
import zipfile
import os
from confluent_kafka  import Producer
import json
import pandas as pd

import sys
sys.path.append("/app")

from scripts.utils.logger import getLog

logger = getLog(__name__)

def scrapeKaggle(kagglePath):
    logger.info(f"[EXTRACT] Scraping data Kaggle  : {kagglePath}")
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
            logger.info(f"[EXTRACT] Found file data : {file}")
        else:
            pass

    return files

def publishData(topicList,server):
    logger.info(f"[EXTRACT] Start publish Data : {topicList}")
    
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
            logger.error(f"[EXTRACT] [ERROR] Delivery data failed : {error}")
        # else:
        #     logger.debug(f"[EXTRACT] Publish data to Topic {msg.topic()} | partition {msg.partition()} | offset {msg.offset()}")
    
    produce = Producer({
        'bootstrap.servers': server,
        'enable.idempotence': True,
        'batch.size': 32768,
        'linger.ms': 50
    })
    
    kagglePath = "./data/bronze"
        
    dataPath = os.path.join(kagglePath,f"{topicList}.csv")
    
    logger.info(f"[EXTRACT] [READ] Start reading data : {topicList}")
    data = pd.read_csv(dataPath)
    
    count = 0
    
    logger.info(f"[EXTRACT] [KAFKA] Publish data : {topicList}")
    for i, row in enumerate(data.itertuples(index=False)):
        message= {
            column: (None if pd.isna(value) else value)
            for column, value in zip(data.columns, row)
        }
        columns = keyTopic.get(topicList,[])
        if columns:
            key = "_".join(str(row[col]) for col in columns if col in row)
        else:
            key = str(row.iloc[0])
        
        try:
            produce.produce(topicList, 
                            key=key.encode(),
                            value=json.dumps(message).encode("utf-8"),
                            callback=report
                            )
            count += 1
        except BufferError:
            logger.warning(f"[EXTRACT] [KAFKA] Buffer full, Polling data")
            produce.poll(1)
            produce.produce(topicList, 
                            key=key.encode(),
                            value=json.dumps(message).encode("utf-8"),
                            callback=report
                            )
            count += 1
        
        if count % 1000 == 0:
            logger.info(f"[EXTRACT] publish {count} data")
            produce.poll(0)
            
    produce.flush()
    logger.info(f"[EXTRACT] Success publish data : {topicList}")