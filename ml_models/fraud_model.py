import numpy as np 
import pandas as pd 
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
import os 

#Set project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

gcp_creds_path = os.path.join(project_root, 'gcp-creds.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_creds_path
client = bigquery.Client()

#Project information
project_id = 'opportune-chess-463416-e7'
dataset_id = 'intermediate'

#Table information
test_table_name = 'int_test_healthcare_fraud'
train_table_name = 'int_train_healthcare_fraud'

#Set table path to variable
test_table = f"{project_id}.{dataset_id}.{test_table_name}"
train_table = f"{project_id}.{dataset_id}.{train_table_name}"

#Create a query string
test_query = f"SELECT * FROM {test_table}"
train_query = f"SELECT * FROM {train_table}"

# Run query and load to DataFrame
test_df = client.query(test_query).to_dataframe()
train_df = client.query(train_query).to_dataframe()
