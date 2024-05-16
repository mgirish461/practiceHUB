from google.cloud import bigquery
client = bigquery.Client()

QUERY = ("""SELECT * FROM `jobinterview999.BQ_Practice.nested` LIMIT 100;""")
query_job = client.query(QUERY)
rows = query_job.result()
for row in rows:
    print(row.name)




# export GOOGLE_APPLICATION_CREDENTIALS="C:\Users\mgiri\PycharmProjects\pythonProject\ETL_CF_TO_BQ\jobinterview999-d136dde9c65c.json"