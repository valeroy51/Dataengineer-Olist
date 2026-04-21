import sys
sys.path.append("/app")

from scripts.pipeline.extract import publishData, unzipData, scrapeKaggle
from scripts.pipeline.transform import cleanData
from scripts.utils.newTopic import newtopic
from scripts.utils.connection import sparkConnection

spark = sparkConnection()
pathdataset="olistbr/brazilian-ecommerce"
test=scrapeKaggle(pathdataset)
list = unzipData(test)
# unzipData("./data/raw./brazilian-ecommerce.zip")

for i in list:
    newtopic(i)
    
for i in list:  
    publishData(i,"kafka-1:29092,kafka-2:29093,kafka-3:29094")
    
for i in list: 
    cleanData(i,spark)
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