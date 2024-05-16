# pip install --upgrade google-cloud-bigquery
# https://www.rudderstack.com/guides/how-to-access-and-query-your-bigquery-data-using-python-and-r/
# https://www.getcensus.com/blog/how-to-hack-it-extracting-data-from-google-bigquery-with-python-2

from google.cloud import bigquery
from google.cloud import storage
import datetime
import logging

SERVICE_ACCOUNT_JSON = r'C:\Users\mgiri\PycharmProjects\pythonProject\ETL_CF_TO_BQ\jobinterview999-d136dde9c65c.json'

# SERVICE_ACCOUNT_JSON=r'/home/alka_mestri/jobinterview999-d136dde9c65c.json' **** this is for cloud shell terminal

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
bucket_name = 'bq-py-api-extract-data'
blob_name = 'bq_to_csv_query.csv'
destination_uri = f'gs://{bucket_name}/{blob_name}'


def bq_to_csv():
    sql_query = """SELECT * FROM `jobinterview999.BQ_Practice.nested` LIMIT 100;"""
    query_job = client.query(sql_query)
    destination_blob = storage_client.bucket(bucket_name).blob(blob_name)
    destination_blob.content_type = 'text/csv'
    query_job.result().to_dataframe().to_csv(destination_blob.open('w'), index=False)

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    print(f'The query results are exported to {blob.public_url}')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().setLevel(logging.ERROR)
    bq_to_csv()
