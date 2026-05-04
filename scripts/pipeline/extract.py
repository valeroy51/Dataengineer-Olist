import os
from confluent_kafka  import Producer
import json
import pandas as pd

import sys
sys.path.append("/app")

def pathData():
    Path = "./data/bronze"
    files=[]
    
    for file in os.listdir(Path):
        if not file.endswith(".zip"):
            file = file.replace(".csv","")
            files.append(file)
            print(file)
        else:
            pass

    return files

def publishData(topicList,server):
    print(f"data {topicList} dikirim ke kafka")
    
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
    
    # def report(error,msg):
    #     if error is not None:
    #         print (f" Terjadi error : {error}\n")
    #     else:
    #         print(f"message berhasil dipublish ke {msg.topic()} partition {msg.partition()} offset {msg.offset()}\n")
            
    def report(error, msg):
        if error is not None:
            print(f"Error: {error}")
    
    produce = Producer({
        'bootstrap.servers': server,
        'enable.idempotence': True,
        'batch.size': 32768,
        'linger.ms': 50
    })
    
    kagglePath = "./data/bronze"
        
    dataPath = os.path.join(kagglePath,f"{topicList}.csv")
    data = pd.read_csv(dataPath)
    
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
        except BufferError:
            produce.poll(1)
            produce.produce(topicList, 
                            key=key.encode(),
                            value=json.dumps(message).encode("utf-8"),
                            callback=report
                            )
        
        if i % 1000 == 0:
            produce.poll(0)
    
    produce.flush()