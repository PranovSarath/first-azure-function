import logging
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from datetime import date
import azure.functions as func
import requests
import json
import os

def get_API_data(country,arg_APIKey):
    url = "https://newsapi.org/v2/top-headlines"
    querystring = {"country":country,"lang":"en","apiKey":arg_APIKey,"pageSize":5}
    response = requests.request("GET", url, params=querystring) 
    return json.loads(response.text)["articles"]


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    countries = ["US","IN","PT"]
    prm_APIKey = os.environ['API_KEY']
    url = os.environ['COSMOS_HOST']
    key = os.environ['COSMOS_HOST_KEY']
    client = CosmosClient(url, credential=key)
    database_name = 'brunodb'
    try:
        database = client.create_database(database_name)
    except exceptions.CosmosResourceExistsError:
        database = client.get_database_client(database_name)

    container_name = 'newsapidata'
    try:
        container = database.create_container(id=container_name, partition_key=PartitionKey(path="/dbloaddate"))
    except exceptions.CosmosResourceExistsError:
        container = database.get_container_client(container_name)
    except exceptions.CosmosHttpResponseError:
        raise
    for country in countries:
        response_obj = get_API_data(country,prm_APIKey)
        for item in response_obj:
            item["country"] = country
            item["dbloaddate"] = str(date.today())
            container.upsert_item(body=item)

    return func.HttpResponse("This HTTP triggered function executed successfully.")

        
