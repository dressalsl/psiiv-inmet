import requests
from datetime import datetime
import boto3
import json

def getDateHour():
    date, hour = str(datetime.now()).split()
    hour = hour[:2] + '00'
    return date, hour

def getApi(date, hour):
    request = requests.get(f"https://apitempo.inmet.gov.br/estacao/dados/{date}/{hour}")
    status = request.status_code
    response = request.json()
    return response , status

def getDataPE(seasons, status):
    if status == 200:
        list = []
        for season in seasons:
            if season['UF'] == 'PE':
                list.append(season)
        return list
    else:
        return False   

def filterDataPE(dataPE):
    filtered = []
    for data in dataPE:
        dict = {
            'DC_NOME': data['DC_NOME'],
            'CHUVA': data['CHUVA'],
            'TEM_MAX': data['TEM_MAX'],
            'TEM_MIN': data['TEM_MIN'],
            'UMD_INS': data['UMD_INS'],
            'RAD_GLO': data['RAD_GLO']

        }
        filtered.append(dict)
    return filtered
    
def sendKinesis(data):
    client = boto3.client('kinesis', region_name='us-east-1')
    client.put_records(
        Records = [{
            'Data': json.dumps({'data': data}),
            'PartitionKey': 'key'
        }],
        StreamName = 'inmet-data-stream',
    )
    return {
        'statusCode': 200,
        'body': json.dumps('the data has been sent to the KDS')
    }

def lambda_handler(event, context):
    date, hour = getDateHour()
    seasons, statusApi = getApi(date, hour)
    dataPE = getDataPE(seasons, statusApi)
    dataFilteredPE = filterDataPE(dataPE)
    sendKinesis(dataFilteredPE)
    return {
        'statusCode': 200,
        'body': json.dumps(dataFilteredPE)
    }