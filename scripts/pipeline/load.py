import os
from dotenv import load_dotenv
import sys

load_dotenv()

sys.path.append("/app")

def loadData(spark, DBName, schema):
    url = f"jdbc:postgresql://postgres:5432/{DBName}"
    
    tables = {
    "dimgeolocation" :"./data/gold/dim_geolocation/",
    "dimsellers" : "./data/gold/dim_sellers/",
    "dimcustomers" : "./data/gold/dim_customers/",
    "dimproducts" : "./data/gold/dim_products/",
    "dimpurchasedate" :"./data/gold/dim_purchase_date/",
    "factpayments" : "./data/gold/fact_payments/",
    "factreviews" : "./data/gold/fact_reviews/",
    "factsales" : "./data/gold/fact_sales/"
    }
    
    mapColumn = {
    "dimgeolocation" : {
        "geolocation_zip_code_prefix" : "zipCode",
        "geolocation_lat" : "latitude",
        "geolocation_lng" : "longitude",
        "geolocation_city" : "city",
        "geolocation_state" : "state"
        },
    "dimsellers" : {
        "seller_id" : "sellerId", 
        "seller_zip_code_prefix" : "zipCode", 
        "seller_city" : "city", 
        "seller_state" : "state"
        },
    "dimcustomers" : {
        "customer_id" : "customerId", 
        "customer_unique_id" : "uniqueId", 
        "customer_zip_code_prefix" : "zipCode", 
        "customer_city" : "city", 
        "customer_state" : "state"
        },
    "dimproducts" : {
        "product_id" : "productId", 
        "product_category_name" : "categoryName",
        "product_name_lenght" : "nameLength", 
        "product_description_lenght" : "descriptionLength",
        "product_photos_qty" : "photosQty",
        "product_weight_g" : "weightG",
        "product_length_cm" : "lengthCm",
        "product_height_cm" : "heightCm",
        "product_width_cm" : "widthCm"
        },

    "dimpurchasedate" : {
        "date_id" : "dateId",
        "purchase_date" : "datePurchase",
        "year" : "year",
        "month" : "month",
        "day" : "day"
    },
    "factpayments" : {
        "order_id" : "orderId",
        "payment_sequential" : "sequential",
        "payment_type" : "type",
        "payment_installments" : "installments",
        "payment_value" : "value"
    },
    "factreviews" : {
        "review_id" : "reviewId",
        "order_id" : "orderId",
        "review_score" : "score",
        "review_comment_title" : "commentTitle",
        "review_comment_message" : "commentMessage",
        "review_creation_date" : "creationDate",
        "review_answer_timestamp" : "answertimestamp",
    },
    "factsales" : {
        "order_id" : "orderId", 
        "order_item_id" : "itemId",
        "customer_id" : "customerId",
        "seller_id" : "sellerId",
        "product_id" : "productId",
        "date_id" : "dateId",
        "order_status" : "status",
        "price" : "price",
        "freight_value" : "freightValue"
    }
    }
    
    for table, path in tables.items():
        data = spark.read.parquet(path)
        
        if table in mapColumn:
            for old, new in mapColumn[table].items():
                data = data.withColumnRenamed(old, new)
        
        data.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", os.getenv("POSTGRES_USER")) \
            .option("password", os.getenv("POSTGRES_PASSWORD")) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", 10000) \
            .option("numPartitions", 6) \
            .mode("append") \
            .save()
        
        print(f"data table {table} berhasil dimasukkan")