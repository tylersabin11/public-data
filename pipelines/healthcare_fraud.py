import os 
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import bigquery
import pandas as pd 

#Set project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

#Authenticate with Kaggle API
api = KaggleApi()
api.authenticate()

#Set Data path
data_path = os.path.join(project_root, 'data_path')
#Set download link to variable
dataset_link = 'rohitrox/healthcare-provider-fraud-detection-analysis'

#If the path does not exist, make the directory
if not os.path.exists(data_path):
    os.makedirs(data_path)

#Pass in dataset link, path, and unzip the files
api.dataset_download_files(dataset_link, path=data_path, unzip=True)

#Find all files that end in .csv
csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
#Open dataframe to be concat on
dataframe = {}

for file in csv_files:
    #Remove any '-' chars with a '_' for a cleaner table name
    table_name = os.path.splitext(file)[0].replace('-','_')
    file_path = os.path.join(data_path, file)
    df = pd.read_csv(file_path)
    dataframe[table_name] = df
    print(f'Loaded {table_name}: with a shape of {df.shape}')

#Setup BigQuery credentials
gcp_creds_path = os.path.join(project_root, 'gcp-creds.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_creds_path
client = bigquery.Client()

project_id = 'opportune-chess-463416-e7'
dataset_id = 'my_datasets'

for table_name, df in dataframe.items():
    #Set table_id with the project id, dataset name, and the current table_name from df
    table_id = f'{project_id}.{dataset_id}.{table_name.lower()}'
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f'Uploaded {table_name} to {table_id} successfully')
