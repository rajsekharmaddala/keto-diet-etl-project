import json
import requests
from datetime import datetime
import os
import boto3

def lambda_handler(event, context):
    XRapidKey = os.environ.get('XRapidKey')
    url = "https://keto-diet.p.rapidapi.com/"

    querystring = {"protein_in_grams__lt":"15","protein_in_grams__gt":"5"}
    
    headers = {
    	"X-RapidAPI-Key": XRapidKey,
    	"X-RapidAPI-Host": "keto-diet.p.rapidapi.com"
    }
    
    response = requests.get(url, headers=headers, params=querystring)
    keto_data = response.json()
    
    client = boto3.client('s3')
    filename = "keto_raw_" + str(datetime.now()) +".json"
    client.put_object(
        Bucket = "keto-data-pipeline-raj",
        Key = "raw_data/to_processed/" + filename,
        Body = json.dumps(keto_data)
        )
    
    
 
        