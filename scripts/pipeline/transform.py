import os

from pyspark.sql.functions import count, from_json, col, to_timestamp, when
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DoubleType, StringType

def cleanData(dataName,spark):
    noDuplicateMap={
        "olist_customers_dataset":["customer_id","customer_unique_id"],
        "olist_geolocation_dataset":None,
        "olist_orders_dataset":["order_id","customer_id"],
        "olist_order_items_dataset":["order_id","order_item_id"],
        "olist_order_payments_dataset":["order_id", "payment_sequential"],
        "olist_order_reviews_dataset":["review_id","order_id"],
        "olist_products_dataset":["product_id"],
        "olist_sellers_dataset":["seller_id"],
        "product_category_name_translation":["product_category_name"]
    }
    
    silverPath = "./data/silver"
    os.makedirs(silverPath, exist_ok=True)
    
    print(f"process data {dataName}")
        
    kafka =(spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29093,kafka-3:29094")
            .option("subscribe", dataName)
            .option("startingOffsets", "earliest")
            .load())
    
    dataRaw = kafka.selectExpr("CAST(value AS STRING) as json_str")
    
    dataRaw.show(5, False)
    
    schemaMap={
        "olist_customers_dataset":StructType([
            StructField("customer_id",StringType()),
            StructField("customer_unique_id",StringType()),
            StructField("customer_zip_code_prefix",DoubleType()),
            StructField("customer_city",StringType()),
            StructField("customer_state",StringType())]),
        "olist_geolocation_dataset":StructType([
            StructField("geolocation_zip_code_prefix",DoubleType()),
            StructField("geolocation_lat",DoubleType()),
            StructField("geolocation_lng",DoubleType()),
            StructField("geolocation_city",StringType()),
            StructField("geolocation_state",StringType())]),
        "olist_orders_dataset":StructType([
            StructField("order_id",StringType()),
            StructField("customer_id",StringType()),
            StructField("order_status",StringType()),
            StructField("order_purchase_timestamp",StringType()),
            StructField("order_approved_at",StringType()),
            StructField("order_delivered_carrier_date",StringType()),
            StructField("order_delivered_customer_date",StringType()),
            StructField("order_estimated_delivery_date",StringType())]),
        "olist_order_items_dataset":StructType([
            StructField("order_id",StringType()),
            StructField("order_item_id",DoubleType()),
            StructField("product_id",StringType()),
            StructField("seller_id",StringType()),
            StructField("shipping_limit_date",StringType()),
            StructField("price",DoubleType()),
            StructField("freight_value",DoubleType())]),
        "olist_order_payments_dataset":StructType([
            StructField("order_id",StringType()),
            StructField("payment_sequential",DoubleType()),
            StructField("payment_type",StringType()),
            StructField("payment_installments",DoubleType()),
            StructField("payment_value",DoubleType()),]),
        "olist_order_reviews_dataset":StructType([
            StructField("review_id",StringType()),
            StructField("order_id",StringType()),
            StructField("review_score",DoubleType()),
            StructField("review_comment_title",StringType()),
            StructField("review_comment_message",StringType()),
            StructField("review_creation_date",StringType()),
            StructField("review_answer_timestamp",StringType())]),
        "olist_products_dataset":StructType([
            StructField("product_id",StringType()),
            StructField("product_category_name",StringType()),
            StructField("product_name_lenght",DoubleType()),
            StructField("product_description_lenght",DoubleType()),
            StructField("product_photos_qty",DoubleType()),
            StructField("product_weight_g",DoubleType()),
            StructField("product_length_cm",DoubleType()),
            StructField("product_height_cm",DoubleType()),
            StructField("product_width_cm",DoubleType())]),
        "olist_sellers_dataset":StructType([
            StructField("seller_id",StringType()),
            StructField("seller_zip_code_prefix",DoubleType()),
            StructField("seller_city",StringType()),
            StructField("seller_state",StringType())]),
        "product_category_name_translation":StructType([
            StructField("product_category_name",StringType()),
            StructField("product_category_name_english",StringType())])
    }
    
    schema = schemaMap[dataName]
    
    dfParse = dataRaw.select(
        from_json(col("json_str"), schema).alias("data")
    ).select("data.*")
    
    #clean name data
    newName = dataName.replace("olist_","")
    newName = newName.replace("_dataset","")
    if not newName.endswith("_items") and "order_" in newName:
        newName = newName.replace("order_","")
        print(newName,"\n")
    else:
        print(newName,"\n")
    
    newPath = os.path.join(silverPath,f"{newName}")
    
    dfClean = dfParse
    
    totalNull =  dfClean.select([
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in dfClean.columns
        ])
    
    print("Column File")
    totalNull.show()
    
    before = dfClean.select(count("*")).show()
    
    if noDuplicateMap[dataName]:
        dfClean=dfClean.dropDuplicates(subset=noDuplicateMap[dataName])
    else:
        dfClean=dfClean.dropDuplicates()
        
    after = dfClean.select(count("*")).show()
    
    for c in dfClean.columns:
        if c.endswith("_date") or c.endswith("_timestamp") or c.endswith("_at"):
            dfClean = dfClean.withColumn(c, to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss"))
            
    try:
        dfClean.write \
            .mode("overwrite") \
            .parquet(newPath)

        print(f"[SUCCESS] Saved to {os.path.abspath(newPath)}")

    except Exception as e:
        print(f"[ERROR] Failed to write parquet for {dataName}")
        raise e