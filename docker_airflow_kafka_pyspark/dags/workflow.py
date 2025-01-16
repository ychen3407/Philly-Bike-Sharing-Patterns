from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

from scripts.stations_info_api import stations_info_api_extract, stations_info_api_transform
from scripts.upload_to_gcp import get_client, fetch_bigquery_schema, upload_to_bigquery, upload_to_bucket_stations_info

import json
from scripts.weather_api import weather_api_extract

key_path=''
project_id=''
dataset='pa_shared_bikes'
bucket='gcp-de-bikes-project'

default_args={
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def stations_info_api_transform_airflow(ti):
    data=ti.xcom_pull(task_ids='stations_info_api_extract1')
    stations=stations_info_api_transform(data)
    return stations

def upload_to_bigquery_airflow(ti, key_path, project_id, dataset, table, write_disposition):
    client=get_client(key_path=key_path, service='bigquery')
    stations=ti.xcom_pull(task_ids='stations_info_api_transform_airflow1')
    stations_df=pd.DataFrame(stations)
    upload_to_bigquery(bq_client=client,
                       df=stations_df,
                       project_id=project_id,
                       dataset=dataset,
                       table=table,
                       write_disposition=write_disposition)

def upload_to_bucket_stations_info_airflow(ti, gcp_bucket_name):
    client=get_client(key_path=key_path, service='storage')
    stations=ti.xcom_pull(task_ids='stations_info_api_transform_airflow1')
    stations_df=pd.DataFrame(stations)
    upload_to_bucket_stations_info(storage_client=client,
                                   df=stations_df,
                                   gcp_bucket_name=gcp_bucket_name)


def upload_to_bigquery_airflow(ti, key_path, project_id, dataset, table, write_disposition):
    dict_data=ti.xcom_pull(task_ids='weather_api_extract_transform1')
    df=pd.DataFrame(dict_data)
    client=get_client(key_path, service='bigquery')
    upload_to_bigquery(bq_client=client,
                       df=df,
                       project_id=project_id,
                       dataset=dataset,
                       table=table,
                       write_disposition=write_disposition)




with DAG(
    dag_id='batch_workflow',
    default_args=default_args,
    start_date=datetime(2025, 1, 1, ),
    schedule='@daily',

) as dag:
    task1=PythonOperator(
        task_id='stations_info_api_extract1',
        python_callable=stations_info_api_extract
    )
    task2=PythonOperator(
        task_id='stations_info_api_transform_airflow1',
        python_callable=stations_info_api_transform_airflow
    )
    task3=PythonOperator(
        task_id='upload_to_bigquery_airflow11',
        python_callable=upload_to_bigquery_airflow,
        op_kwargs={
            'key_path': key_path,
            'project_id': project_id,
            'dataset': dataset,
            'table':'stations_info',
            'write_disposition':"WRITE_TRUNCATE"
        }
    )

    task4=PythonOperator(
        task_id='upload_to_bucket_stations_info_airflow1',
        python_callable=upload_to_bucket_stations_info_airflow,
        op_kwargs={'gcp_bucket_name': bucket}
    )

    task21=PythonOperator(
        task_id='weather_api_extract_transform1',
        python_callable=weather_api_extract,
    )

    task22=PythonOperator(
        task_id='upload_to_bigquery_airflow12',
        python_callable=upload_to_bigquery_airflow,
        op_kwargs={'key_path': key_path,
                   'project_id': project_id,
                   'dataset': dataset,
                   'table': 'weather',
                   'write_disposition': 'WRITE_APPEND'}
    )

task1 >> task2 >> [task3, task4]
task21 >> task22