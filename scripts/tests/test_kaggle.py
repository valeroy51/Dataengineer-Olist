import time
import sys
sys.path.append("/app")

from scripts.pipeline.load import loadData
from scripts.core.database import createDB, createSchemaTable
from scripts.pipeline.extract_from_kaggle import publishData, unzipData, scrapeKaggle
from scripts.pipeline.transform import cleanData, goldData
from scripts.utils.newTopic import newtopic
from scripts.utils.connection import sparkConnection

start = time.perf_counter()
spark = sparkConnection()
pathdataset="olistbr/brazilian-ecommerce"
test=scrapeKaggle(pathdataset)
list = unzipData(test)

for i in list:
    newtopic(i)
    
for i in list:  
    publishData(i,"kafka-1:29092,kafka-2:29093,kafka-3:29094")
    
for i in list: 
    cleanData(i,spark)
    
goldData(spark)

createDB("olistecommerce")

createSchemaTable("ecommerce","olistecommerce")

loadData(spark, "olistecommerce", "ecommerce")

end = time.perf_counter()

total_time = end - start

print(f"Total time: {total_time:.4f} seconds")

total_minutes = total_time / 60

print(f"Total time: {total_minutes:.4f} minutes")

# unzipData("./data/raw./brazilian-ecommerce.zip")
# cleanData("product_category_name_translation")


# # ekstract >> ctopic >> cdb >> kafka >> clean>>dim>>fact


# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("Read Parquet") \
#     .getOrCreate()

# path = "/app/data/silver/product_category_name_translation"

# df = spark.read.parquet(path)

# print("Total rows:", df.count())
# df.show(5, False)