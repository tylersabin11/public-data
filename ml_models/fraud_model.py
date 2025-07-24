import numpy as np 
import pandas as pd 
from google.cloud import bigquery
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
train_table_name = 'int_train_healthcare_fraud'
test_table_name = 'int_test_healthcare_fraud'

#Set table path to variable
train_table = f"{project_id}.{dataset_id}.{train_table_name}"
test_table = f"{project_id}.{dataset_id}.{test_table_name}"

#Create a query string
train_query = f"SELECT * FROM {train_table}"
test_query = f"SELECT * FROM {test_table}"

# Run query and load to DataFrame
train_df = client.query(train_query).to_dataframe()
test_df = client.query(test_query).to_dataframe()

#Add potential_fraud to test
test_df.insert(18, "potential_fraud", np.nan)

#Test train split
X_train = train_df.drop(columns=["potential_fraud"])
X_test = test_df.drop(columns=["potential_fraud"])

y_train = train_df["potential_fraud"]
y_test = test_df["potential_fraud"]

#Model training to take place below
