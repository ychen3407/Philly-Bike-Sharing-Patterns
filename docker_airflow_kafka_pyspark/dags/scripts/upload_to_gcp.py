import logging
import json
import datetime, io

from google.oauth2 import service_account
from google.cloud import bigquery, storage

# on Windows: `set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\your\service-account-file.json`
def get_client(key_path, service="bigquery"):
    "Get bigquery or cloud bucket client (service), key_path: json file that store service account crendtial"
    # Authenticate using the service account
    credentials = service_account.Credentials.from_service_account_file(key_path)

    # Create the BigQuery client using the credentials, else storage
    if service == "bigquery":
        client = bigquery.Client(credentials=credentials, 
                                 project=credentials.project_id)
    else:
        client = storage.Client(credentials=credentials, project=credentials.project_id)
    return client


def fetch_bigquery_schema(table):
    """
    Predefined partial table schema and table name for biquery

    Args:
        table: schema to fetch ('stations_info', 'stations_status', 'weather')
    
    Return: a predefined schema and predefind table name, if not exists, return None
    """
    schema, table_name=None, None

    if table == 'stations_info':
        schema=[
            # Specify the type of columns whose type cannot be auto-detected.
            bigquery.SchemaField("station_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("station_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("address", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("active_status", bigquery.enums.SqlTypeNames.BOOLEAN),
        ]
        table_name = 'stg_stations'

    elif table == 'stations_status':
        schema=[
            bigquery.SchemaField("timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("station_id", bigquery.enums.SqlTypeNames.STRING),
        ]
        table_name = 'realtime_stations_status'

    elif table == 'weather':
        schema=[
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.STRING),
        ]
        table_name = 'stg_weather'

    return schema, table_name


def upload_to_bigquery(bq_client, df, project_id, dataset, table, write_disposition="WRITE_TRUNCATE"):
    """
    Upload dataframe to a specified table in bigquery

    Args:
        client: bigquery client with crendential preset
        schema: partial schema to assist in data type definitions (e.g. string type)
        table: name of destination table, should match fetch_bigquery_schema
        write_disposition: big query writing mode as followed
                        "WRITE_TRUNCATE": replaces the table with the loaded data.
                        "WRITE_APPEND": append data if table exist.
                        "WRITE_EMPTY": write to table only if it's empty
    
    Return: None, will add logging information
    """
    schema, table_name = fetch_bigquery_schema(table)

    table_id = f"{project_id}.{dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema, 
        write_disposition=write_disposition, 
    )

    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    ) 

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        # filename='basic.log'
    )

    try:
        job.result()
        logging.info(f"Successfully loaded {job.output_rows} rows into {bq_client.project}")

    except Exception as e:
        logging.error(f"An error occurred while loading data: {e}")

    return None


def upload_to_bucket_stations_info(storage_client, df, gcp_bucket_name):
    """
    Upload stations_info to storage bucket as csv files, won't write out to disk, upload from memory

    Args:
        storage_client: client with service account crendential

    Return: None, log info
    """
    blob_name = f'stations_info_{datetime.datetime.today().date()}.csv'

    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Get the GCP bucket & blob
    bucket = storage_client.get_bucket(gcp_bucket_name)
    b = bucket.blob(blob_name)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if b.exists():
        logging.info(f'Blob already exist: {gcp_bucket_name}/{blob_name}')
    else:
        # upload to newly created blob
        b.upload_from_file(csv_buffer, content_type='text/csv')
        logging.info(f'Successfully uploaded {gcp_bucket_name}/{blob_name}')
    return None