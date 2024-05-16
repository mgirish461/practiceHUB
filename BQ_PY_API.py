# pip install --upgrade google-cloud-bigquery
# https://www.rudderstack.com/guides/how-to-access-and-query-your-bigquery-data-using-python-and-r/
# https://www.getcensus.com/blog/how-to-hack-it-extracting-data-from-google-bigquery-with-python-2
# https://cloud.google.com/bigquery/docs/exporting-data >>> VVI

from google.cloud import bigquery
import datetime
import logging

SERVICE_ACCOUNT_JSON = r'C:\Users\mgiri\PycharmProjects\pythonProject\ETL_CF_TO_BQ\jobinterview999-d136dde9c65c.json'

# SERVICE_ACCOUNT_JSON=r'/home/alka_mestri/jobinterview999-d136dde9c65c.json' **** this is for cloud shell terminal

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
bucket_name = 'bq-py-api-extract-data'
project = "jobinterview999"
dataset_id = "BQ_Practice"
table_id = "check_datatype_size"


def extract_data_to_bq():  # ---------------Default CSV Format------------------ So no JobConfig is required.
    destination_uri = "gs://{}/{}".format(bucket_name, "nested_table_data.csv")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="us-west1"  # Location must match with source table.
    )
    extract_job.result()
    timestamp = datetime.datetime.now()
    print(f"Exported as on >> {timestamp}, project >> {project}, Dataset >> {dataset_id}, Table >> {table_id},"
          f" Fie_Location >>{destination_uri}")


def extract_json_data_to_bq():       # ---------------JSON FORMAT------------------
    destination_uri = "gs://{}/{}".format(bucket_name, "check_datatype_size.json")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="us-west1",
    )
    extract_job.result()
    timestamp = datetime.datetime.now()
    print(f"Exported as on >> {timestamp}, project >> {project}, Dataset >> {dataset_id}, Table >> {table_id},"
          f" Fie_Location >>{destination_uri}")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().setLevel(logging.ERROR)
    extract_data_to_bq()
    extract_json_data_to_bq()
