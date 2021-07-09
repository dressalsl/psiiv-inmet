import json
import base64
import boto3

s3 = boto3.client('s3')

def putClientS3(body):
    clientS3Json = s3.put_object(Bucket='inmet-bucket', Key='data_inmet.json', Body=str(json.dumps(body)))

def lambda_handler(event, context):
    listResult = []
    for i in event.get('Records'):
        payload = base64.b64decode(i['kinesis']['data']).decode('utf-8')
        payload = json.loads(payload)
        for item in payload['data']:
            listResult.append(item)
    
    clientS3 = putClientS3(listResult)
    