'''
=================================================
Milestone 3

Nama  : Nakia Melvana
Batch : FTDS-026-RMT

This program is designed to automate the transformation and loading of data from PostgreSQL to Elasticsearch, 
which will then be utilized with Airflow and Kibana. The dataset used pertains to the Netflix user base, 
aimed at analyzing user trends and preferences.
=================================================
'''


# import library
import psycopg2
import re
import pandas as pd 
import datetime as dt 
import warnings
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
warnings.filterwarnings("ignore")

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ================================================= 
# A. FETCH FROM POSTGRESQL
def fecth_data():
    # config database
    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432' 

    # Connect to Database
    connection = psycopg2.connect(
        database = db_name,
        user = db_user,    
        password = db_password,
        host = db_host,
        port = db_port
    )
 
    # Get All Data
    select_query = 'SELECT * FROM table_m3;'
    df = pd.read_sql(select_query,connection)

    # Close the Connection
    connection.close()
   
    # Save into CSV
    df.to_csv('/opt/airflow/dags/P2M3_Nakia_Melvana_data_raw.csv', index = False)
                                   
# =================================================
# B. DATA CLEANING
    
# Define data_cleaning function
def data_cleaning():
    # Read csv raw data to dataframe using pandas
    df = pd.read_csv('/opt/airflow/dags/P2M3_Nakia_Melvana_data_raw.csv')

    # Check if there any duplicated data
    if df.duplicated().any():
        df.drop_duplicates(inplace = True) 
    else:
        print('There is no duplicated data')

    # Modify columns name      
    cols = df.columns.tolist() # Get column names
    new_cols = [] # Initialize list to store modified column names

    for col in cols:
        new_col = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', col) # Split camel case
        new_col = [x.lower() for x in new_col] # Convert to lowercase
        new_col = '_'.join(new_col) # Connect words with underscores
        new_cols.append(new_col) # Append modified column name to list

    df.columns = new_cols

    # Handling Missing Value
    if df.isnull().any().any():
        df.dropna(inplace=True, axis=0).reset_index(drop=True)
    else:
        print('there is no missing value')

    # Delete Month in column 'Plan Duration' and convert the datatype to numeric
    df['plan_duration'] = df['plan_duration'].str.replace('Month', '').apply(pd.to_numeric)
    # convert data type 'Join Date', 'Last Payment Date' to datetime
    df[['join_date', 'last_payment_date']] = df[['join_date', 'last_payment_date']].apply(pd.to_datetime)

    # Save to CSV
    df.to_csv('/opt/airflow/dags/P2M3_Nakia_Melvana_data_clean.csv', index = False)

# =================================================
# B. INSERT INTO ELASTIC SEARCH - WITH AIRFLOW

# define function to insert data to elastic
def insert_into_elastic_manual():
    # Read CSV File into dataframe
    df=pd.read_csv('/opt/airflow/dags/P2M3_Nakia_Melvana_data_clean.csv')

    # Check Connection
    es = Elasticsearch('http://elasticsearch:9200')
    print('Connection Status :', es.ping())
    
    # Insert CSV File to Elastic Search
    for i, r in df.iterrows():
        # Convert each row to JSON format
        doc = r.to_json()
        # Index document to elastic
        res = es.index(index='netflix-new', doc_type='doc', body = doc)

# =================================================
# D. DATA PIPELINE

# Default arguments for the DAG
default_args = {
    'owner' : 'Melva', # Owner of the DAG
    'start_date': datetime(2024, 1, 25, 6, 30, 0) - timedelta(hours=7), # Start date and time of the DAG run
    'retries' : 1, # Number of retries in case of failure
    'retry_delay' : dt.timedelta(minutes=1) # Time interval between retries
}          
     

# Creating the DAG object
with DAG(
    'p2m3', # DAG ID
    default_args= default_args, # Default arguments for the DAG
    schedule_interval= '@daily', # Schedule interval runs daily
    catchup=False) as dag: # Whether or not to backfill past DAG runs

    # Task 1: Print a message that is indicationg the DAG has started using BashOperator
    node_start = BashOperator(
        task_id = 'starting',
        bash_command='echo "I am reading the CSV now..."')

    # Task 2: Fetch the data using PythonOperator
    node_fetch_data = PythonOperator(
        task_id= 'fetch-data',
        python_callable=fecth_data)
    
    # Task 3: Run data_cleaning Function using PythonOperator
    node_data_cleaning = PythonOperator(
        task_id = 'data_cleaning',
        python_callable = data_cleaning)

    # Task 4: Inser data to Elastic using Python Operator
    node_insert_data_to_elastic = PythonOperator(
        task_id = 'insert-data-to-elastic',
        python_callable=insert_into_elastic_manual)

# Define the flow of the task
node_start >> node_fetch_data >> node_data_cleaning >> node_insert_data_to_elastic





