from fastapi import FastAPI, Depends, HTTPException, Body, status,Query,Path
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from passlib.context import CryptContext
from typing import Optional
import jwt
from datetime import datetime, timedelta
import pickle
import pandas as pd

import uvicorn
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse


from auth import JWTBearer,decode_jwt,sign_jwt,token_response
from users import check_user,UserSchema, read_users,write_users


app = FastAPI()

#for logo
app.mount("/static", StaticFiles(directory="static"), name="static")

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

es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    basic_auth=('elastic', 'datascientest'),
    verify_certs=True,
    ca_certs='/ca/ca.crt',  # Chemin vers le certificat CA
    request_timeout=600
)

if es.ping():
    print("Connection successful")
else:
    print("Connection failed")




@app.post("/user/signup")
async def create_user(user: UserSchema = Body(...)):
    """
    Description:
    Cette route permet à un utilisateur de s'inscrire en fournissant les détails de l'utilisateur. Elle ajoute ensuite l'utilisateur et renvoie un jeton JWT.

    Args:
    - user (UserSchema, Body): Les détails de l'utilisateur à créer.

    Returns:
    - str: Un jeton JWT si l'inscription est réussie.

    Raises:
    400 s'il existe un utilisateur avec le même nom
    """
    users = read_users()
    if any(u["username"] == user.username for u in users):
        raise HTTPException(status_code=400, detail="User already exists")

    users.append({"username": user.username, "password": user.password})
    write_users(users)

    # Retourner un jeton JWT
    return sign_jwt(user.username)
#curl -X POST "http://127.0.0.1:2222/user/signup" -H "Content-Type: application/json" -d '{"username": "soda","password": "jeteste1"}'

#{"access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoic29kYSIsImV4cCI6MTczMzA2OTkzM30.u4Da2zeEl1Usm7VC-_83OtxwFyJS7QMxwXEVMuInv38"}%  

@app.get("/fc",  tags=["root"])
async def read_rootff():
    """
    Description:
    Cette route renvoie un message de bienvenue.
    Args:
    Aucun argument requis.
    Returns:
    - JSON: Renvoie  un message de salutation et indiquant comment obtenir un jeton si besoin d'utiliser la recommandation
    Raises:
    Aucune exception n'est levée.
    """
    #return {"message": "Hello,  welcome to Jobmarket, and find the job you deserve!, you can register by going on  'http://127.0.0.1:2222/user/signup'"}
    return {
        "message": "👋 Welcome to JobMarket!",
        "description": "Find the job you deserve! JobMarket is an easy way to search for and apply to jobs.",
        "instructions": {
            "register": "To start, register by visiting the signup page.",
            "signup_url": "http://127.0.0.1:2222/user/signup",
            "login": "Already registered? You can log in with your credentials.",
            "login_url": "http://127.0.0.1:2222/user/login"
        },
        "image": "http://127.0.0.1:2222/static/image.jpeg",
        "note": "Once registered, you will receive a JWT token to authenticate your requests."
    }

@app.get("/",  tags=["root"])
async def read_root():
    """
    Description:
    Cette route renvoie un message de bienvenue.
    Args:
    Aucun argument requis.
    Returns:
    - JSON: Renvoie  un message de salutation et indiquant comment obtenir un jeton si besoin d'utiliser la recommandation
    Raises:
    Aucune exception n'est levée.
    """
    html_content = """
    <html>
        <head>
            <title>Welcome to JobMarket</title>
        </head>
        <body>
            <h1>👋 Welcome to JobMarket!</h1>
            <p>Find the job you deserve! JobMarket is an easy way to search for and apply to jobs.</p>
            <p>To start, register by visiting the signup page: <a href="http://127.0.0.1:2222/user/signup">Sign up</a></p>
            <p>Already registered? You can log in with your credentials: <a href="http://127.0.0.1:2222/user/login">Login</a></p>
            <h3>Logo</h3>
            <img src="http://127.0.0.1:2222/static/image.jpeg"" alt="JobMarket Logo" />
            <p>Once registered, you will receive a JWT token to authenticate your requests.</p>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)
#curl -X GET "http://127.0.0.1:2222/"


@app.post("/user/login", tags=["user"])
async def user_login(user: UserSchema = Body(...)):
    """
    Description:
    Cette route permet à un utilisateur de se connecter en fournissant les détails de connexion. Si les détails sont valides, elle renvoie un jeton JWT. Sinon, elle renvoie une erreur.

    Args:
    - user (UserSchema, Body): Les détails de connexion de l'utilisateur.

    Returns:
    - str: Un jeton JWT si la connexion réussit.

    Raises:
    - HTTPException(401, detail="Unauthorized"): Si les détails de connexion sont incorrects, une exception HTTP 401 Unauthorized est levée.
    """

    if check_user(user):
        return sign_jwt(user.username)
    return {"error": "Wrong login details!"}

#curl -X POST "http://127.0.0.1:2222/user/login" -H "Content-Type: application/json" -d '{"username": "soda","password": "jeteste1"}'

#{"access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoic29kYSIsImV4cCI6MTczMzA3MDMwOH0.rhAQKZW8SaQ-8rZ9XQzEOmf9CE6s1IiEMpyVhN0-t1M"}%  


@app.get("/jobs/{jobTitle}" ,tags=["jobs"])
def jobs_title(jobTitle: str = Path(..., min_length=3, max_length=50)):
    """
    Description:
    Cette route renvoie les offres d'emploi avec le job_title indiqué en argument.

    Args:
    le titre du job recherché.

    Returns:
    - json: {\"job_title\":\"Photographer\",\"contract_time\":\"Full-time\",\"company_name\":\"CoCreativ\",\"salary_min\":\"\",\"salary_max\":\"\",\"publication_datetime\":\"2024-11-09\",\"location\":\"New York, NY\",\"industry\":\"Software Development\",\"offer_link\":\"https:\\/\\/www.linkedin.com\\/jobs\\/view\\/photographer-at-cocreativ-4072854058?position=3&pageNum=0&refId=fX7x8eYibgbCmrDQUCU9cA%3D%3D&trackingId=1U6jgNN4heUk12cCYW%2FgNA%3D%3D\",\"source\":\"linkedin\",\"levels\":\"Mid-Senior level\",\"category\":\"Other\",\"contents\":\"not available\",\"publication_time\":null,\"publication_date\":\"2024-11-09\",\"contract_type\":\"not available\",\"country\":\"United States\",\"state\":\"New York\",\"city\":\"City of New York\",\"postcode\":\"10000\",\"latitude\":\"40.7127281\",\"longitude\":\"-74.0060152\",\"currency_name\":\"US Dollar\",\"job_description\":null,\"type_contract\":null,\"addressCountry\":null}.

    Raises:
    - HTTPException: Si le titre de job est invalide ou si aucune offre n'est trouvée.

    """
    #jobs_title = jobs_full[jobs_full["job_title"] == jobTitle]
    #jobs_title_json = jobs_title.to_json(orient='records')
    if not jobTitle.replace(" ", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid format for the searched job")
    
    result = es.search(
        index='bigdata-jobs',
        query={
        'match': {'job_title': jobTitle}
         }
    )
    final_result = result['hits']['hits']
    if not final_result:
        raise HTTPException(status_code=404, detail="Job not found")

    #return jobs_title_json
    return final_result

#Exemple requete curl
# curl -X GET -i http://127.0.0.1:2222/jobs/Photographer  
#curl -X GET -i http://127.0.0.1:2222/jobs/Software@Engineer

@app.get("/jobs/country/{jobCountry}", tags=["jobs"])
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
 
    result = es.search(
        index='bigdata-jobs',
        query={
        'match': {'industry': jobIndustry}
         } )
    final_result = result['hits']['hits']
    return final_result


jobs_final = pd.read_pickle('../outputs/final/jobs.pkl')
jobs_final = jobs_final.reset_index(drop=True)

jobs_final['job_title'].astype(str) + '-' + jobs_final['company_name'].astype(str) + '-' + jobs_final['city'].astype(str)+'-'+ jobs_final['levels'].astype(str)+'-'+ jobs_final['job_description'].astype(str)+'-'+ jobs_final['contract_type'].astype(str)


    #jobs_country_json = jobs_country.to_json(orient='records')
import numpy as np
def weighted_recommendations_elk(input_job_title, k=10):
    result = es.search(
        index='bigdata-jobs',
        query={ 
            "match": { 
                "job_title": {
                    "query": input_job_title, 
                    "fuzziness": "AUTO"
                }
            }
        },
        size=1 
    )
    
    if not result['hits']['hits']:
        raise HTTPException(status_code=404, detail="No job found for the given title")

    #job_elastic_id = result['hits']['hits'][0]['_id']######## A CHANGER PR LE NOM DU BON INDEX ELASTIC LOCAL
    #job_index = jobs_final[jobs_final['id_df'] == job_elastic_id].index[0]
    job_index=result['hits']['hits'][0]['_id']

    # Récupérer les indices des voisins et leurs distances via Faiss
    neighbor_indices = indices[job_index]
    neighbor_distances = distances[job_index]
    
    # Requête pour récupérer les voisins dans Elasticsearch en utilisant leurs IDs
    #neighbor_results = es.mget(index='bigdata-jobs', ids=[str(idx) for idx in neighbor_indices])
    neighbor_results = es.mget(index='bigdata-jobs',body={ "ids": [str(job_id) for job_id in neighbor_indices] })

    # Créer un DataFrame avec les voisins récupérés depuis Elasticsearch
    recommended_jobs = pd.DataFrame([
        {
            **doc["_source"],  # Contenu des champs du document Elasticsearch
            "elastic_id": doc["_id"],  # Ajouter l'ID Elastic à chaque voisin
            "distance": neighbor_distances[i],  # Ajouter la distance associée à chaque voisin
        }
        for i, doc in enumerate(neighbor_results['docs']) if doc['found']  # Vérifie si le document existe
    ])

    # Indices utilisés pour le calcul pondéré
    indices_for_weighting = [
        'Quality_of_Life_Index', 'Purchasing_Power_Index', 'Safety_Index', 'Health_Care_Index', 
        'Cost_of_Living_Index', 'Rent_Index', 'Groceries_Index', 'Crime_Index', 'Climate_Index',
        'Pollution_Index', 'Traffic_Index', 'Affordability_Index'
    ]

    indices_values = recommended_jobs[indices_for_weighting].values

    # Inverser certains indices pour le calcul des poids
    indices_values[:, 4] = 1 / (indices_values[:, 4] + 0.1)  # Cost_of_Living_Index
    indices_values[:, 5] = 1 / (indices_values[:, 5] + 0.1)  # Rent_Index
    indices_values[:, 7] = 1 / (indices_values[:, 7] + 0.1)  # Crime_Index
    indices_values[:, 9] = 1 / (indices_values[:, 9] + 0.1)  # Pollution_Index
    indices_values[:, 10] = 1 / (indices_values[:, 10] + 0.1)  # Traffic_Index

    # Calcul des poids et distances pondérées
    weights = np.sum(indices_values, axis=1)
    weights = (weights - np.min(weights)) / (np.max(weights) - np.min(weights))  # Normalisation
    weighted_distances = np.array(neighbor_distances) * weights
    weighted_indices = np.argsort(weighted_distances)[:k]  # Top k résultats pondérés

    # Retourner les recommandations pondérées
    return recommended_jobs.iloc[weighted_indices]


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


@app.get("/recommend",dependencies=[Depends(JWTBearer())])
def recommend_jobs(job_title: str):
    try:
        if job_title.lower() not in jobs_final['job_title'].str.lower().values:
            raise HTTPException(status_code=404, detail="Jobs not found")

        recommended_jobs_w = weighted_recommendations(job_title)  
        recommended_jobs_wjson = recommended_jobs_w.to_json(orient='records')
        return recommended_jobs_wjson  # Retourner le résultat JSON
    
    except Exception as e:  # Attraper toutes les exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

#http://127.0.0.1:2222/recommended?


@app.get("/recommend_elastic",dependencies=[Depends(JWTBearer())])
def recommend_elastic(job_title: str):
    try:
        recommended_jobs_w = weighted_recommendations_elk(job_title)
        recommended_jobs_wjson = recommended_jobs_w.to_json(orient='records')
        return recommended_jobs_wjson
    except Exception as e:  # Gérer les exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
#curl -i -X GET "http://127.0.0.1:2222/recommend?job_title=Data%20Scientist"

#curl -X GET -i "http://127.0.0.1:2222/recommend?job_title=Data%20Scientist" -H 'Authorization: Bearer 9eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoic29kYSIsImV4cCI6MTczMzA3MDMwOH0.rhAQKZW8SaQ-8rZ9XQzEOmf9CE6s1IiEMpyVhN0-t1M'
#http://127.0.0.1:2222/recommend?job_title=Data%20Scientist&token=9eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoic29kYSIsImV4cCI6MTczMzA3MDMwOH0.rhAQKZW8SaQ-8rZ9XQzEOmf9CE6s1IiEMpyVhN0-t1M'


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


