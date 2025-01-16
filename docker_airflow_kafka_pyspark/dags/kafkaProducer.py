from kafka import KafkaProducer
import json, requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

topic='stations_status'
kafka_host='kafka'
producer=KafkaProducer(bootstrap_servers=f'{kafka_host}:9092',
                    #    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                       )

default_args={
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def stations_status_api_extract():
    "Return byte type"
    url = 'https://gbfs.bcycle.com/bcycle_indego/station_status.json'
    response=requests.get(url)
    if response.status_code == 200:
        return response.content   
    response.raise_for_status()
    return None

def streaming_data(topic, producer):
    response=stations_status_api_extract()
    # data=json.loads(response.decode('utf-8'))['data']['stations']
    producer.send(topic, response)

with DAG (
    dag_id='kafka_producer',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='*/10 * * * *', # every 10 minutes
    catchup=False,
) as dag:
    task1=PythonOperator(
        task_id='streaming_data',
        python_callable=streaming_data,
        op_kwargs={'topic': topic,
                   'producer': producer}
    )

task1