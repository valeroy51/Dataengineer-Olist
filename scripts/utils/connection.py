import psycopg2
import sqlalchemy
from pyspark import SparkContext
from pyspark.sql import SparkSession

def connection(HostDB, DBName, DBUser, DBPassword, DBPort):
    con = psycopg2.connect(
        host = HostDB,
        dbname = DBName,
        user = DBUser,
        password = DBPassword,
        port = DBPort
    )

    return con

def sparkConnection():
    spark = (
        SparkSession.builder
        .appName("ecommerce")
        .master("spark://spark-master:7077")
        .config("spark.sql.streaming.metricsEnabled", "false")
        .config("spark.jars.packages",
                ",".join([
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
                    # "org.apache.hadoop:hadoop-aws:3.4.2",
                    # "com.amazonaws:aws-java-sdk-bundle:1.12.367"
                ]))
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark