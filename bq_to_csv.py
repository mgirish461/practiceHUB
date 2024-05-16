# pip install --upgrade google-cloud-bigquery
# https://www.rudderstack.com/guides/how-to-access-and-query-your-bigquery-data-using-python-and-r/
# https://www.getcensus.com/blog/how-to-hack-it-extracting-data-from-google-bigquery-with-python-2

from google.cloud import bigquery
import datetime
import logging

SERVICE_ACCOUNT_JSON = r'C:\Users\mgiri\PycharmProjects\pythonProject\ETL_CF_TO_BQ\jobinterview999-d136dde9c65c.json'

# SERVICE_ACCOUNT_JSON=r'/home/alka_mestri/jobinterview999-d136dde9c65c.json' **** this is for cloud shell terminal

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
bucket_name = 'bq-py-api-extract-data'
project = "jobinterview999"
dataset_id = "BQ_Practice"
table_id = "nested"


def bq_to_csv():
    sql_query = """SELECT * FROM `jobinterview999.BQ_Practice.nested` LIMIT 100;"""
    data_frame = client.query(sql_query).to_dataframe()
    data_frame.to_csv("bq_to_csv.csv", index=False, header=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().setLevel(logging.ERROR)
    bq_to_csv()


# https://hevodata.com/learn/export-bigquery-table-to-csv/ IMPORTANT
# https://blog.coupler.io/how-to-crud-bigquery-with-python/
