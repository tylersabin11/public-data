import requests
import pandas as pd 
from google.cloud import bigquery
import os 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-creds.json"
client = bigquery.Client()

url = "https://data.cms.gov/data-api/v1/dataset/c37ebe6d-f54f-4d7d-861f-fefe345554e6/data"
response = requests.get(url)
data = response.json()

df = pd.json_normalize(data)

table_id = "table_id_here"

job = client.load_table_from_dataframe(
    df, 
    table_id, 
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
)
job.result()

print(f"Loaded {job.output_rows} row to {table_id}")
