import psycopg2
import sqlalchemy
from pyspark import SparkContext, join
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

def connection(DBName):
    
    con = psycopg2.connect(
        host = "postgres",
        dbname = DBName,
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
        port = 5432
    )

    return con

def sparkConnection():
    spark = (
        SparkSession.builder
        .appName("ecommerce")
        .master("spark://spark-master:7077")
        .config("spark.executorEnv.PYTHONPATH", "/app")
        .config(
            "spark.jars.packages",
            ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
            "org.postgresql:postgresql:42.7.3"
            ])
        )
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark