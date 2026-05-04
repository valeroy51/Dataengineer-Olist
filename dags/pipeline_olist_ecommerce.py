from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup

import sys
sys.path.append("/opt/airflow")

from scripts.pipeline.load import loadData
from scripts.core.database import createDB, createSchemaTable
from scripts.pipeline.extract import pathData, publishData
from scripts.pipeline.transform import cleanData, goldData
from scripts.utils.newTopic import newtopic
from scripts.utils.connection import sparkConnection

def listData(**context):
    data = pathData()
    context['ti'].xcom_push(key='list_data', value=data)
    return data
    
def createTopic(**context):
    list = context['ti'].xcom_pull(task_ids = 'checkdata', key='list_data')
    for i in list:
        newtopic(i)
        
def publish(**context):
    list = context['ti'].xcom_pull(task_ids = 'checkdata', key='list_data')
    for i in list:
        publishData(i,"kafka-1:29092,kafka-2:29093,kafka-3:29094")
        
def silver(**context):
    spark = sparkConnection()
    list = context['ti'].xcom_pull(task_ids = 'checkdata', key='list_data')
    for i in list: 
        cleanData(i,spark)

def gold():
    spark = sparkConnection()
    goldData(spark)
    
def setupDatabase():
    createDB("olistecommerce")
    
def setupSchema():
    createSchemaTable("ecommerce","olistecommerce")
    
def toDatabase():
    spark = sparkConnection()
    loadData(spark, "olistecommerce", "ecommerce")
    
with DAG(
    dag_id = "pipeline_olist_ecommerce",
    schedule = None,
    catchup = False
) as dag:
    
    check_data = PythonOperator(
        task_id = "checkdata",
        python_callable = listData
    )

    create_topic = PythonOperator(
        task_id = "setup_topic",
        python_callable = createTopic
    )
    
    publish_data = PythonOperator(
        task_id = "publish_data",
        python_callable = publish
    )
    
    silver_data = PythonOperator(
        task_id = "silver_data",
        python_callable = silver
    )
    
    gold_data = PythonOperator(
        task_id = "gold_data",
        python_callable = gold
    )

    with TaskGroup("setup_data_warehouse") as setup_data_warehouse:
        create_database = PythonOperator(
            task_id = "setup_database",
            python_callable = setupDatabase
        )
        
        create_schema = PythonOperator(
            task_id = "setup_schema",
            python_callable = setupSchema
        )
        
        create_database >> create_schema

    load_data = PythonOperator(
        task_id = "load_data",
        python_callable = toDatabase
    )
    
    check_data >> create_topic >> publish_data >> silver_data >> gold_data >> setup_data_warehouse >> load_data