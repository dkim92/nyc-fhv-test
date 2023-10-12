import boto3
import requests
import pandas as pd
from io import BytesIO
import os
from datetime import datetime

#lambda function for import daily increment file runs 

#get and set environment variables 
S3_BUCKET = os.environ['S3_BUCKET'] 
S3_PREFIX = os.environ['S3_PREFIX'] 
ATHENA_OUTPUT_LOCATION = os.environ['ATHENA_OUTPUT_LOCATION']
DATABASE_NAME = os.environ['DATABASE_NAME']
TABLE_NAME = os.environ['TABLE_NAME']
URL = os.environ['URL']
FILENAME = os.environ['FILENAME']

#function for adding partition to s3
def add_partition(date_str):
    athena_client = boto3.client('athena')
    s3_location = f's3://{S3_BUCKET}/{S3_PREFIX}/{date_str}/'
    
    sql = f"""
        ALTER TABLE {TABLE_NAME}
        ADD IF NOT EXISTS PARTITION (partition_0='{date_str}')
        LOCATION '{s3_location}'
    """
    # Start the Athena query execution
    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': DATABASE_NAME
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT_LOCATION
        }
    )
    return response['QueryExecutionId']

def lambda_handler(event, context):
    # Get the latest date of data in S3
    s3 = boto3.client('s3')
    result = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    latest_date = None
    for content in result.get('Contents', []):
        date_str = content['Key'].split('/')[-2]
        if latest_date is None or date_str > latest_date:
            latest_date = date_str
    
    # Extract the latest data using the API
    params = {}
    today_str = datetime.now().strftime('%Y-%m-%d')
    if latest_date:
        params = {'last_date_updated':{today_str}}
    url = URL
    response = requests.get(url, params=params)
    data = response.json()
    
    df = pd.DataFrame(data)
    # Check if the DataFrame is empty 
    if df.empty:
        return {
            'statusCode': 200,
            'body': 'No new data to ingest!'
        }
    #Convert to Parquet and save to S3    
    buffer = BytesIO()
    df.to_parquet(buffer)
    s3_key = f"{S3_PREFIX}/{today_str}/{FILENAME}.parquet"
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    
    add_partition(today_str)
    

    return {
        'statusCode': 200,
        'body': 'Data ingested as delta, saved as Parquet, and partition added to Athena!'
    }
