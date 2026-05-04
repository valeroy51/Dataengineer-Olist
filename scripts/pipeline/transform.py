import os
import sys
sys.path.append("/app")
from pyspark.sql import Window
from unidecode import unidecode

from pyspark.sql.functions import coalesce, count, create_map, dayofmonth, first, month, round, from_json, col, lit, regexp_replace, row_number, to_date, to_timestamp, udf, when, year
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DoubleType, StringType

from scripts.utils.logger import getLog

logger = getLog(__name__)

def normalizeText(text):
    if text is None:
        return None
    
    try:
        text = text.encode('latin1').decode('utf-8')
    except:
        pass
        
    return unidecode(text.lower())

def silverData(dataName,spark):
    logger.info(f"[SILVER] Start processing Data : {dataName}")
    noDuplicateMap={
        "olist_customers_dataset":["customer_id","customer_unique_id"],
        "olist_geolocation_dataset":["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
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
    
    logger.info(f"[SILVER] [READ] Reading from Kafka Topic : {dataName}")
        
    kafka =(spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29093,kafka-3:29094")
            .option("subscribe", dataName)
            .option("startingOffsets", "earliest")
            .load())
    
    dataRaw = kafka.selectExpr("CAST(value AS STRING) as json_str")
    
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
        logger.info(f"[SILVER] [TRANSFORM] Output table name : {newName}")
    else:
        logger.info(f"[SILVER] [TRANSFORM] Output table name : {newName}")
        
    newPath = os.path.join(silverPath,f"{newName}")
    
    dfClean = dfParse
    
    totalNull =  dfClean.select([
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in dfClean.columns
        ])
    
    row = totalNull.collect()[0]
    
    nullLog = "\n".join([f"{col}: {row[col]}" for col in totalNull.columns])
    
    logger.info(f"[SILVER] [INFO] {dataName} - NULL summary : \n {nullLog}")
    
    normalize_udf = udf(normalizeText, StringType())
    
    logger.info(f"[SILVER] [TRANSFORM] Cleaning column Table : {dataName}")
    
    for c in dfClean.columns:
        if c.endswith("_date") or c.endswith("_timestamp") or c.endswith("_at"):
            dfClean = dfClean.withColumn(c, to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss"))
        elif c.endswith("_city") or c.endswith("_title") or c.endswith("_message"):
            dfClean = dfClean.withColumn(c, normalize_udf(col(c)))
        elif c == "product_category_name":
            dfClean = dfClean.withColumn(c, when(col(c).isNull(), "unknown").otherwise(col(c)))
        elif c == "product_name_lenght" or c == "product_description_lenght":
            dfClean = dfClean.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))
        elif c == "product_category_name_english" or c.endswith("_name") or c.endswith("_type"):
            dfClean = dfClean.withColumn(c, regexp_replace(col(c), "_", " "))
        elif c.endswith("_lat") or c.endswith("_lng"):
            dfClean = dfClean.withColumn(c, round(col(c),6))

    before = dfClean.count()
        
    if noDuplicateMap[dataName]:
        dfClean=dfClean.dropDuplicates(subset=noDuplicateMap[dataName])
    else:
        dfClean=dfClean.dropDuplicates()
        
    after = dfClean.count()
    
    logger.info(f"[SILVER] [INFO] Deduplication table {dataName} : before > {before}, after > {after}")
    
    try:
        logger.info(f"[SILVER] [WRITE] Writing Parquet to : {newPath}")

        dfClean.write \
            .mode("overwrite") \
            .parquet(newPath)

        logger.info(f"[SILVER] Success writing parquet table : {dataName}")

    except Exception as e:
        logger.error(f"[SILVER] [ERROR] Failed to write parquet : {dataName}")
        raise e
    
    logger.info(f"[SILVER] Finished processing: {dataName}")

def goldData(spark):
    customers = spark.read.parquet("./data/silver/customers/")
    sellers = spark.read.parquet("./data/silver/sellers/")
    products = spark.read.parquet("./data/silver/products/")
    product_category_name_translation = spark.read.parquet("./data/silver/product_category_name_translation/")
    orders = spark.read.parquet("./data/silver/orders/")
    order_items = spark.read.parquet("./data/silver/order_items/")
    payments = spark.read.parquet("./data/silver/payments/")
    reviews = spark.read.parquet("./data/silver/reviews/")
    geolocation = spark.read.parquet("./data/silver/geolocation/")
    
    stateMap = {
        "SP": "Sao Paulo",
        "RN": "Rio Grande do Norte",
        "AC": "Acre",
        "RJ": "Rio de Janeiro",
        "ES": "Espírito Santo",
        "MG": "Minas Gerais",
        "BA": "Bahia",
        "SE": "Sergipe",
        "PE": "Pernambuco",
        "AL": "Alagoas",
        "PB": "Paraíba",
        "CE": "Ceará",
        "PI": "Piaui",
        "MA": "Maranhao",
        "PA": "Para",
        "AP": "Amapa",
        "AM": "Amazonas",
        "RR": "Roraima",
        "DF": "Distrito Federal",
        "GO": "Goias",
        "RO": "Rondonia",
        "TO": "Tocantins",
        "MT": "Mato Grosso",
        "MS": "Mato Grosso do Sul",
        "RS": "Rio Grande do Sul",
        "PR": "Parana",
        "SC": "Santa Catarina"
    }
        
    mappingState = create_map([lit(x) for x in sum(stateMap.items(),())])
    
    logger.info(f"[GOLD] [TRANSFORM] Proceed data : dim_geolocation")
    dim_geolocation = (geolocation.orderBy("geolocation_zip_code_prefix","geolocation_lat")
                        .groupBy("geolocation_zip_code_prefix")
                        .agg(
                            first("geolocation_lat").alias("geolocation_lat"),
                            first("geolocation_lng").alias("geolocation_lng"),
                            first("geolocation_city").alias("geolocation_city"),
                            first(mappingState[col("geolocation_state")]).alias("geolocation_state")
                        ))
    
    dim_geolocation = dim_geolocation.withColumn(
                                        "geolocation_zip_code_prefix", col("geolocation_zip_code_prefix").cast("int")
                                    )

    logger.info(f"[GOLD] [TRANSFORM] Proceed data : dim_sellers")
    dim_sellers = sellers.select(
                            "seller_id", 
                            "seller_zip_code_prefix", 
                            "seller_city", 
                            mappingState[col("seller_state")].alias("seller_state")
                        )

    dim_sellers = dim_sellers.withColumn(
                                    "seller_zip_code_prefix", col("seller_zip_code_prefix").cast("int")
                                )
    
    logger.info(f"[GOLD] [TRANSFORM] Proceed data : dim_customers")
    dim_customers = customers.select(
                                "customer_id", 
                                "customer_unique_id", 
                                "customer_zip_code_prefix", 
                                "customer_city", 
                                mappingState[col("customer_state")].alias("customer_state")
                            )
    
    dim_customers = dim_customers.withColumn(
                                        "customer_zip_code_prefix", col("customer_zip_code_prefix").cast("int")
                                    )
    
    logger.info(f"[GOLD] [TRANSFORM] Proceed data : dim_products")
    dim_products = (products.join(product_category_name_translation, "product_category_name", "left")
                            .select(
                                "product_id", 
                                coalesce("product_category_name_english", "product_category_name").alias("product_category_name"),
                                "product_name_lenght", 
                                "product_description_lenght",
                                "product_photos_qty",
                                "product_weight_g",
                                "product_length_cm",
                                "product_height_cm",
                                "product_width_cm"
                            ))
    
    logger.info(f"[GOLD] [TRANSFORM] Proceed data : dim_purchase_date")
    orders = orders.withColumn("purchase_date",to_date("order_purchase_timestamp","yyyy-MM-dd"))
    
    dim_purchase_date = (orders.select(
                            "purchase_date").dropDuplicates()
                                .withColumn("day", dayofmonth("purchase_date"))
                                .withColumn("month", month("purchase_date"))
                                .withColumn("year", year("purchase_date"))
                        )
    
    window = Window.orderBy("purchase_date")
    
    dim_purchase_date = dim_purchase_date.withColumn("date_id", row_number().over(window))
    
    dim_purchase_date = dim_purchase_date.select(
                                            "date_id",
                                            "purchase_date",
                                            "year",
                                            "month",
                                            "day"
                                        )
    
    logger.info(f"[GOLD] [TRANSFORM] Proceed data : fact_payments")
    fact_payments = payments.select(
                            "order_id",
                            "payment_sequential",
                            "payment_type",
                            "payment_installments",
                            "payment_value"
                        )
    
    logger.info(f"[GOLD] [TRANSFORM] Proceed data : fact_reviews")
    reviews = reviews.withColumn("purchase_date",to_date("review_creation_date","yyyy-MM-dd"))
    
    fact_reviews = reviews.select(
                            "review_id",
                            "order_id",
                            "review_score",
                            "review_comment_title",
                            "review_comment_message",
                            "review_creation_date",
                            "review_answer_timestamp"
                        )
    
    logger.info(f"[GOLD] [TRANSFORM] Proceed data : fact_sales")
    fact_sales = (order_items
                    .join(orders.select("order_id", "customer_id", "order_status", "purchase_date"), "order_id", "left")
                    .join(dim_purchase_date, "purchase_date", "left")
                    .select(
                    "order_id",
                    "order_item_id",
                    "customer_id",
                    "seller_id",
                    "product_id",
                    "date_id",
                    "order_status",
                    "price",
                    "freight_value",
                    round(coalesce(col("price"), lit(0))+coalesce(col("freight_value"), lit(0)),2).alias("total_price")
                ))
    
    goldPath = "./data/gold"
    
    dimCustomerPath = os.path.join(goldPath,"dim_customers")
    dimGeolocationPath = os.path.join(goldPath,"dim_geolocation")
    dimProductsPath = os.path.join(goldPath,"dim_products")
    dimSellersPath = os.path.join(goldPath,"dim_sellers")
    dimPurchaseDate = os.path.join(goldPath,"dim_purchase_date")
    factSalesPath = os.path.join(goldPath,"fact_sales")
    factPayments = os.path.join(goldPath,"fact_payments")
    factReviews = os.path.join(goldPath,"fact_reviews")
    
    write = {
        "dim_geolocation": (dim_geolocation, dimGeolocationPath),
        "dim_sellers": (dim_sellers, dimSellersPath),
        "dim_customers": (dim_customers, dimCustomerPath),
        "dim_products": (dim_products, dimProductsPath),
        "dim_purchase_date": (dim_purchase_date, dimPurchaseDate),
        "fact_payments": (fact_payments, factPayments),
        "fact_reviews": (fact_reviews, factReviews),
        "fact_sales": (fact_sales, factSalesPath),
    }
    
    for name, (data, path) in write.items():
        try:
            logger.info(f"[GOLD] [WRITE] Writing Parquet {name} to : {path}")

            data.write.mode("overwrite").format("parquet").save(path)

            logger.info(f"[GOLD] Success writing parquet table : {name}")

        except Exception as e:
            logger.error(f"[GOLD] [ERROR] Failed to write parquet : {name}")
            raise e
    
    logger.info(f"[GOLD] Finished processing all Table")