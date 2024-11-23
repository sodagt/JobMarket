from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from passlib.context import CryptContext
from typing import Optional
import jwt
from datetime import datetime, timedelta

import pandas as pd

import uvicorn


app = FastAPI()


jobs = pd.read_pickle('../outputs/final/jobs.pkl')

jobs_full = jobs[['job_title', 'contract_time', 'company_name', 'salary_min', 'salary_max', 'publication_datetime', 'location', 'industry', 'offer_link', 'source', 'levels', 'category', 'contents', 'publication_time', 'publication_date', 'contract_type', 'country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name', 'job_description', 'type_contract', 'addressCountry']]

jobs_full_json = jobs_full.to_json(orient='records')


from elasticsearch import Elasticsearch


es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    http_auth=('elastic', 'datascientest'), 
    timeout=600,
    verify_certs=True,  # Check SSL certificats
    ca_certs='../elasticsearch/ca/ca.crt'  # Specify CA certificat path

)



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

#curl -X GET -i http://127.0.0.1:2222/jobs/Photographer


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
     



#if __name__ == "__main__":
#    uvicorn.run(app, host="127.0.0.1", port=2222)

#uvicorn main:app --reload --port 2222

#http://127.0.0.1:2222/doc
#http://127.0.0.1:2222/redoc