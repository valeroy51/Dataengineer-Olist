import os
import sys
sys.path.append("/app")

import pandas as pd

def cleanData(dataName):
    print(f"process data {dataName}")
    noDuplicateMap={"olist_customers_dataset":["customer_id","customer_unique_id"],
        "olist_geolocation_dataset":None,
        "olist_orders_dataset":["order_id","customer_id"],
        "olist_order_items_dataset":["order_id","order_item_id"],
        "olist_order_payments_dataset":["order_id", "payment_sequential"],
        "olist_order_reviews_dataset":["review_id","order_id"],
        "olist_products_dataset":["product_id"],
        "olist_sellers_dataset":["seller_id"],
        "product_category_name_translation":["product_category_name"]
    }
    
    bronzePath = "./data/bronze"
    silverPath = "./data/silver/pandas"
    os.makedirs(silverPath, exist_ok=True)
    
        
    dataPath = os.path.join(bronzePath,f"{dataName}.csv")

    newName = dataName.replace("olist_","")
    newName = newName.replace("_dataset","")
    if not newName.endswith("_items") and "order_" in newName:
        newName = newName.replace("order_","")
        print(newName,"\n")
    else:
        print(newName,"\n")
    
    newPath = os.path.join(silverPath,f"{newName}.csv")
    
    newdata = pd.read_csv(dataPath)
    
    totalNull = newdata.isnull().sum()
    print("Column File")
    print(totalNull,"\n")
    
    if noDuplicateMap[dataName]:
        before = len(newdata)
        newdata=newdata.drop_duplicates(subset=noDuplicateMap[dataName])
        after=len(newdata)
        print(before,"-",after,"\n")
    else:
        before = len(newdata)
        newdata=newdata.drop_duplicates()
        after=len(newdata)
        print(before,"-",after,"\n")
    
    for col in newdata.columns:
        if col.endswith("_date") or col.endswith("_timestamp") or col.endswith("_at"):
            newdata[col] = pd.to_datetime(newdata[col],errors="coerce")
            
    newdata.to_csv(newPath,index=False)
    
    print(newdata.dtypes,"\n")
    
cleanData("olist_customers_dataset")
cleanData("olist_geolocation_dataset")
cleanData("olist_orders_dataset")
cleanData("olist_order_items_dataset")
cleanData("olist_order_payments_dataset")
cleanData("olist_order_reviews_dataset")
cleanData("olist_products_dataset")
cleanData("olist_sellers_dataset")
cleanData("product_category_name_translation")