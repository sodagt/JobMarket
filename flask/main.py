from fastapi import FastAPI, Depends, HTTPException, status,Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from passlib.context import CryptContext
from typing import Optional
import jwt
from datetime import datetime, timedelta
import pickle
import pandas as pd

import uvicorn


app = FastAPI()

reco_path = '../outputs/final/indices_reco.pkl'
distance_path = '../outputs/final/distances_reco.pkl'

with open(reco_path, 'rb') as f:
        indices = pickle.load(f)

with open(distance_path, 'rb') as f:
        distances = pickle.load(f)

#jobs = pd.read_pickle('../outputs/final/jobs.pkl')

#jobs_full = jobs[['job_title', 'contract_time', 'company_name', 'salary_min', 'salary_max', 'publication_datetime', 'location', 'industry', 'offer_link', 'source', 'levels', 'category', 'contents', 'publication_time', 'publication_date', 'contract_type', 'country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name', 'job_description', 'type_contract', 'addressCountry']]

#jobs_full_json = jobs_full.to_json(orient='records')


from elasticsearch import Elasticsearch
"""
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    http_auth=('elastic', 'datascientest'), 
    timeout=600,
    verify_certs=True,  # Check SSL certificats
    ca_certs='../elasticsearch/ca/ca.crt'  # Specify CA certificat path
)""" 
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    basic_auth=('elastic', 'datascientest'),
    verify_certs=True,
    ca_certs='../elasticsearch/ca/ca.crt',  # Chemin vers le certificat CA
    request_timeout=600
)

if es.ping():
    print("Connection successful")
else:
    print("Connection failed")


@app.get("/jobs/{jobTitle}")
def jobs_tittle(jobTitle: str):
    """
    Description:
    Cette route renvoie les offres d'emploi avec le job_title indiqué en argument.

    Args:
    le titre du job recherché.

    Returns:
    - json: {\"job_title\":\"Photographer\",\"contract_time\":\"Full-time\",\"company_name\":\"CoCreativ\",\"salary_min\":\"\",\"salary_max\":\"\",\"publication_datetime\":\"2024-11-09\",\"location\":\"New York, NY\",\"industry\":\"Software Development\",\"offer_link\":\"https:\\/\\/www.linkedin.com\\/jobs\\/view\\/photographer-at-cocreativ-4072854058?position=3&pageNum=0&refId=fX7x8eYibgbCmrDQUCU9cA%3D%3D&trackingId=1U6jgNN4heUk12cCYW%2FgNA%3D%3D\",\"source\":\"linkedin\",\"levels\":\"Mid-Senior level\",\"category\":\"Other\",\"contents\":\"not available\",\"publication_time\":null,\"publication_date\":\"2024-11-09\",\"contract_type\":\"not available\",\"country\":\"United States\",\"state\":\"New York\",\"city\":\"City of New York\",\"postcode\":\"10000\",\"latitude\":\"40.7127281\",\"longitude\":\"-74.0060152\",\"currency_name\":\"US Dollar\",\"job_description\":null,\"type_contract\":null,\"addressCountry\":null}.

    Raises:
    Aucune exception n'est levée.
    """
    
    #jobs_title = jobs_full[jobs_full["job_title"] == jobTitle]
    #jobs_title_json = jobs_title.to_json(orient='records')

    result = es.search(
        index='bigdata-jobs',
        query={
        'match': {'job_title': jobTitle}
         }
    )

    final_result = result['hits']['hits']

    #return jobs_title_json
    return final_result

#Exemple requete curl
# curl -X GET -i http://127.0.0.1:2222/jobs/Photographer


@app.get("/jobs/country/{jobCountry}")
def jobs_country(jobCountry: str):
    #jobs_country = jobs_full[jobs_full["country"] == jobCountry]
    #jobs_country_json = jobs_country.to_json(orient='records')

    result = es.search(
        index='bigdata-jobs',
        query={
        'match': {'country': jobCountry}
         }
    )

    final_result = result['hits']['hits']

    #return jobs_country_json
    return final_result

#Exemple requete curl
#curl -X GET -i http://127.0.0.1:2222/jobs/country/France


@app.get("/jobs/industry/{jobIndustry}")
def jobs_country(jobIndustry: str):
    #jobs_country = jobs_full[jobs_full["industry"] == jobIndustry]
    #jobs_country_json = jobs_country.to_json(orient='records')

    result = es.search(
        index='bigdata-jobs',
        query={
        'match': {'industry': jobIndustry}
         }
    )

    final_result = result['hits']['hits']

    #return jobs_country_json
    return final_result


jobs_final = pd.read_pickle('../outputs/final/jobs.pkl')
jobs_final = jobs_final.reset_index(drop=True)

jobs_final['job_title'].astype(str) + '-' + jobs_final['company_name'].astype(str) + '-' + jobs_final['city'].astype(str)+'-'+ jobs_final['levels'].astype(str)+'-'+ jobs_final['job_description'].astype(str)+'-'+ jobs_final['contract_type'].astype(str)


    #jobs_country_json = jobs_country.to_json(orient='records')
import numpy as np

def weighted_recommendations(input_job_title,k=10):
    job_index = jobs_final[jobs_final['job_title'].str.contains(input_job_title, case=False)].index[0]
    neighbor_indices = indices[job_index]
    neighbor_distances = distances[job_index]

    recommended_jobs = jobs_final.iloc[neighbor_indices]    
    indices_for_weighting = [
        'Quality_of_Life_Index', 'Purchasing_Power_Index','Safety_Index','Health_Care_Index', 'Cost_of_Living_Index', 'Rent_Index', 
        'Groceries_Index', 'Crime_Index', 'Climate_Index','Pollution_Index','Traffic_Index', 'Affordability_Index']
    
    indices_values = recommended_jobs[indices_for_weighting].values
    
    # Invert the index for Cost_of_Living_Index , Crime_Index,Pollution_Index,Traffic_Index,Rent_Index,
    indices_values[:,4 ] = 1 / (indices_values[:,4 ] + 0.1)  # Cost_of_Living_Index
    indices_values[:,5 ] = 1 / (indices_values[:,5 ] + 0.1)  #  Rent_Index
    indices_values[:,7 ] = 1 / (indices_values[:,7 ] + 0.1)  #  Crime_Index
    indices_values[:,9 ] = 1 / (indices_values[:,9 ] + 0.1)  #  Pollution_Index
    indices_values[:,10 ] = 1 / (indices_values[:,10 ] + 0.1)  #  Traffic_Index

    weights = np.sum(indices_values, axis=1)
    # normalize weights 
    weights = (weights - np.min(weights)) / (np.max(weights) - np.min(weights))
    weighted_distances = neighbor_distances * weights
    weighted_indices = np.argsort(weighted_distances)[:k]
    
    return recommended_jobs.iloc[weighted_indices]



@app.get("/recommend")
def recommend_jobs(job_title: str):
    try:
        if job_title.lower() not in jobs_final['job_title'].str.lower().values:
            raise HTTPException(status_code=404, detail="Jobs not found")

        recommended_jobs_w = weighted_recommendations(job_title)  
        recommended_jobs_wjson = recommended_jobs_w.to_json(orient='records')
        return recommended_jobs_wjson  # Retourner le résultat JSON
    
    except Exception as e:  # Attraper toutes les exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

    

#curl -i -X GET "http://127.0.0.1:2222/recommend?job_title=Data%20Scientist"



if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=2222)

#uvicorn main:app --reload --port 2222

#http://127.0.0.1:2222/doc
#http://127.0.0.1:2222/redoc


"""
@app.get('/')
def get_index():
    return {
        'method': 'get',
        'endpoint': '/'
        }

@app.get('/other')
def get_other():
    return {
        'method': 'get',
        'endpoint': '/other'
    }

@app.post('/')
def post_index():
    return {
        'method': 'post',
        'endpoint': '/'
        }

@app.delete('/')
def delete_index():
    return {
        'method': 'delete',
        'endpoint': '/'
        }

@app.put('/')
def put_index():
    return {
        'method': 'put',
        'endpoint': '/'
        }

@app.patch('/')
def patch_index():
    return {
        'method': 'patch',
        'endpoint': '/'
        }
"""
