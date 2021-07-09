import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    listResult = []
    clientS3 = s3.get_object(Bucket='inmet-bucket', Key='data_inmet.json')
    dataClient = clientS3['Body'].read().decode('utf-8')
    listClient =json.loads(dataClient)
   
    print(listClient)
    
    for i in listClient:
        listResult.append(i['UMD_INS'])
        
    for i in range(len(listResult)):
        if listResult[i] == None:
            listResult[i]= "0"
            
    return {'VALOR_OBSERVADO': listResult}