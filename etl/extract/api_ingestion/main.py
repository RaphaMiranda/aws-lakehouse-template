import json
import logging
import os
import urllib.request
from datetime import datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

BUCKET_NAME = os.environ.get('BRONZE_BUCKET_NAME')
API_URL = "https://jsonplaceholder.typicode.com/posts"

def handler(event, context):
    logger.info("Starting ingestion from API")
    
    try:
        with urllib.request.urlopen(API_URL) as response:
            if response.status != 200:
                raise Exception(f"API request failed with status {response.status}")
            
            data = json.loads(response.read().decode())
            
        logger.info(f"Retrieved {len(data)} records")
        
        # Add timestamp to each record for lineage
        ingestion_time = datetime.utcnow().isoformat()
        for record in data:
            record['_ingestion_time'] = ingestion_time

        # Create line-delimited JSON
        ldjson_data = "\n".join([json.dumps(record) for record in data])
        
        # Partition by date
        now = datetime.utcnow()
        partition_path = f"raw/posts/year={now.year}/month={now.month:02d}/day={now.day:02d}"
        file_name = f"posts_{now.strftime('%H%M%S')}.json"
        
        key = f"{partition_path}/{file_name}"
        
        logger.info(f"Writing data to s3://{BUCKET_NAME}/{key}")
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=ldjson_data,
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Ingestion successful')
        }
        
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise e
