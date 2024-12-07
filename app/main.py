from fastapi import FastAPI, Depends, HTTPException, Body, status,Query,Path,Request, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from datetime import timedelta 
import time
import pickle
import pandas as pd
import numpy as np
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from elasticsearch import AsyncElasticsearch
import logging

from auth import ACCESS_TOKEN_EXPIRATION, create_access_token,verify_password , Token ,get_current_user, get_current_user_unlimited#,     JWTBearer,decode_jwt,sign_jwt,token_response
from users import load_users_from_file, save_users_to_file ,UserSchema # read_users,write_users  ,   ,check_user



app= FastAPI(
    title="Jobmarket",
    description="Jobmarket API powered by FastAPI. Find the job you deserve",
    version="1.0.1")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

#for logo
app.mount("/static", StaticFiles(directory="static"), name="static")


#docker path
reco_path = '/outputs/indices_reco.pkl'
distance_path = '/outputs/distances_reco.pkl'

#load indice and distances for the recomandation modek
with open(reco_path, 'rb') as f:
        indices = pickle.load(f)

with open(distance_path, 'rb') as f:
        distances = pickle.load(f)

#Load user
users_db = load_users_from_file("users.json")
#print(users_db)


#  logger configuration
logging.basicConfig(
    filename='api_logs.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@app.middleware("http")
async def log_request_response(request: Request, call_next):
    """
    Middleware to capt the data from requests and response 
    """
    start_time = time.time()
    user = request.headers.get("Authorization", "Anonymous")
    method = request.method
    endpoint = request.url.path
    ip_address = request.client.host
    user_agent = request.headers.get("User-Agent")

    # Initial requests log  
    logging.info(f"Incoming request: User={user}, Method={method}, Endpoint={endpoint}, IP={ip_address}, UserAgent={user_agent}")

    response: Response = await call_next(request)

    duration_ms = int((time.time() - start_time) * 1000)
    status_code = response.status_code

    # response log  
    logging.info(f"Request processed: User={user}, Method={method}, Endpoint={endpoint}, IP={ip_address}, "
                 f"Status={status_code}, Duration={duration_ms}ms")

    return response



#from elasticsearch import Elasticsearch

es = AsyncElasticsearch(
    hosts=["https://es01:9200"],
    ca_certs="/ca/ca.crt",  # Path to the docker CA certificate
    basic_auth=('elastic', 'datascientest'),  # credentials
    verify_certs=True,
    request_timeout=600
)

#check de la connexion
if es.ping():
    print("Connection successful")
else:
    print("Connection failed")




#Bienvenue Docker
@app.get("/",tags=["welcome"])
async def read_root():
    """
    Description:
    Cette route renvoie un message de bienvenue.
    Args:
    Aucun argument requis.
    Returns:
    - HTML: Renvoie  un message de salutation et indiquant comment obtenir un jeton si besoin d'utiliser la recommandation
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
            <p>To start, register by visiting the signup page: <a href="/register">Sign up</a></p>
            <p>Already registered? You can log in with your credentials: <a href="/user/login">Login</a></p>
            <h3>Logo</h3>
            <img src="/static/image.jpeg"" alt="JobMarket Logo" />
            <p>Once registered, you will receive a JWT token to authenticate your requests.</p>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)
#ccurl -X 'GET' 'http://localhost/' 'accept: application/json'

#inscription de l'utilisateur
@app.post("/register",tags=["user"])
async def register_user( user: UserSchema, 
    access: str = Query(..., pattern="^(basic|unlimited)$")  # Le paramètre acces est un paramètre de requête avec validation
):
    """
    Description:
    Cette route permet à un utilisateur de s'authentifier en fournissant un nom d'utilisateur et un mot de passe. Si l'authentification est réussie, elle renvoie un jeton d'accès JWT.
    On fournit le type d'accès souhaté en tant que paramètre de requête (basic ou unlimited).

    Args:
    - form_data (OAuth2PasswordRequestForm, dépendance): Les données de formulaire contenant le nom d'utilisateur et le mot de passe.
    - type d'accès: basic ou unlimited

    Returns:
    - Message: "User registered successfully"

    Raises:
    - HTTPException(400, detail="Incorrect username or password"): Si l'authentification échoue en raison d'un nom d'utilisateur ou d'un mot de passe incorrect, une exception HTTP 400 Bad Request est levée.
    """
    if user.username in users_db or any(existing_user['email'] == user.email for existing_user in users_db.values()):
        raise HTTPException(status_code=400, detail="User already exists")

    hashed_password = pwd_context.hash(user.password)
    new_user = {
        "username": user.username,
        "name": user.name.capitalize(),
        "email": user.email,
        "hashed_password": hashed_password,
        "access": access
    }
    users_db[user.username] = new_user
    save_users_to_file("users.json", users_db)
    return {"message": "User registered successfully"}

"""curl -X 'POST' \
  'http://localhost/register?access=basic' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "username": "test_docker",
  "name": "test",
  "password": "test",
  "email": "test@test"
}'
"""
#curl -X POST "localhost/register" -H "Content-Type: application/json" -d '{"username": "sodathiam", "name": "soda thiam", "password": "soda_password", "email": "sodagayethiam@hotmail.com"}'

@app.post("/token", response_model=Token ,tags=["user"])
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Description:
    Cette route permet à un utilisateur de s'authentifier en fournissant un nom d'utilisateur et un mot de passe. Si l'authentification est réussie, elle renvoie un jeton d'accès JWT.

    Args:
    - form_data (OAuth2PasswordRequestForm, dépendance): Les données de formulaire contenant le nom d'utilisateur et le mot de passe.

    Returns:
    - Token: Un modèle de jeton d'accès JWT.

    Raises:
    - HTTPException(400, detail="Incorrect username or password"): Si l'authentification échoue en raison d'un nom d'utilisateur ou d'un mot de passe incorrect, une exception HTTP 400 Bad Request est levée.
    """

    user = users_db.get(form_data.username)
    hashed_password = user.get("hashed_password")
    if not user or not verify_password(form_data.password, hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRATION)
    access_token = create_access_token(data={"sub": form_data.username}, expires_delta=access_token_expires)

    return {"access_token": access_token, "token_type": "bearer"}

"""
curl -X 'POST' \
  'http://localhost/token' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=password&username=test_docker&password=test&scope=&client_id=string&client_secret=string'
""" 



@app.get("/jobs/{jobTitle}" ,tags=["jobs"])
async def jobs_title(
    jobTitle: str = Path(..., min_length=3, max_length=50), current_user: str = Depends(get_current_user)):
    """
    Description:
    Cette route renvoie les offres d'emploi avec le job_title indiqué en argument.

    Args:
    - le titre du job recherché.
    - current_user (str, dépendance): Le nom de l'utilisateur actuellement authentifié.


    Returns:
    - json: {\"job_title\":\"Photographer\",\"contract_time\":\"Full-time\",\"company_name\":\"CoCreativ\",\"salary_min\":\"\",\"salary_max\":\"\",\"publication_datetime\":\"2024-11-09\",\"location\":\"New York, NY\",\"industry\":\"Software Development\",\"offer_link\":\"https:\\/\\/www.linkedin.com\\/jobs\\/view\\/photographer-at-cocreativ-4072854058?position=3&pageNum=0&refId=fX7x8eYibgbCmrDQUCU9cA%3D%3D&trackingId=1U6jgNN4heUk12cCYW%2FgNA%3D%3D\",\"source\":\"linkedin\",\"levels\":\"Mid-Senior level\",\"category\":\"Other\",\"contents\":\"not available\",\"publication_time\":null,\"publication_date\":\"2024-11-09\",\"contract_type\":\"not available\",\"country\":\"United States\",\"state\":\"New York\",\"city\":\"City of New York\",\"postcode\":\"10000\",\"latitude\":\"40.7127281\",\"longitude\":\"-74.0060152\",\"currency_name\":\"US Dollar\",\"job_description\":null,\"type_contract\":null,\"addressCountry\":null}.

    Raises:
    - HTTPException: Si le titre de job est invalide ou si aucune offre n'est trouvée ou l'utilisateur n'est pas authentifié

    """
  
    if not jobTitle.replace(" ", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid format for the searched job")
    
    result = await es.search(
        index='bigdata-jobs',
        body={ 
         "query" : {
             'match': {'job_title': jobTitle}
         }
       }
    )
    final_result = result['hits']['hits']
    if not final_result:
        raise HTTPException(status_code=404, detail="Job not found")

    return final_result

#Exemple requete curl
# curl -X GET -i localhost/jobs/Photographer  
"""curl -X 'GET' \
  'http://localhost/jobs/dat' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X2RvY2tlciIsImV4cCI6MTczMzQyMjM5OX0.ZhFED0hrnC3BQXWiZU9khWvBDYlegcDcZZV7owzaTjU'
"""

@app.get("/jobs/country/{jobCountry}", tags=["jobs"])
async def jobs_country(jobCountry: str = Path(..., min_length=3, max_length=50), current_user: str = Depends(get_current_user)):
    """
    Description:
    Cette route renvoie les offres d'emploi avec le pays indiqué en argument.

    Args:
    - le pays où l'on souhaite chercher un job
    - current_user (str, dépendance): Le nom d'utilisateur actuellement authentifié.


    Returns:
    - json: {\"job_title\":\"Photographer\",\"contract_time\":\"Full-time\",\"company_name\":\"CoCreativ\",\"salary_min\":\"\",\"salary_max\":\"\",\"publication_datetime\":\"2024-11-09\",\"location\":\"New York, NY\",\"industry\":\"Software Development\",\"offer_link\":\"https:\\/\\/www.linkedin.com\\/jobs\\/view\\/photographer-at-cocreativ-4072854058?position=3&pageNum=0&refId=fX7x8eYibgbCmrDQUCU9cA%3D%3D&trackingId=1U6jgNN4heUk12cCYW%2FgNA%3D%3D\",\"source\":\"linkedin\",\"levels\":\"Mid-Senior level\",\"category\":\"Other\",\"contents\":\"not available\",\"publication_time\":null,\"publication_date\":\"2024-11-09\",\"contract_type\":\"not available\",\"country\":\"United States\",\"state\":\"New York\",\"city\":\"City of New York\",\"postcode\":\"10000\",\"latitude\":\"40.7127281\",\"longitude\":\"-74.0060152\",\"currency_name\":\"US Dollar\",\"job_description\":null,\"type_contract\":null,\"addressCountry\":null}.

    Raises:
    - HTTPException: Si aucune offre n'est trouvée ou si l'utilisateur n'est pas authentifié

    """
    result = await es.search(
        index='bigdata-jobs',
        body={ 
         "query" : {
             'match': {'country': jobCountry}
         }
       }
    )
    final_result = result['hits']['hits']
    if not final_result:
       raise HTTPException(status_code=404, detail="Country not found")

    return final_result

"""curl -X 'GET' \
  'http://localhost/jobs/country/France' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X2RvY2tlciIsImV4cCI6MTczMzQyMjM5OX0.ZhFED0hrnC3BQXWiZU9khWvBDYlegcDcZZV7owzaTjU'
"""

@app.get("/jobs/industry/{jobIndustry}", tags=["jobs"])
async def jobs_industry(jobIndustry: str = Path(..., min_length=3, max_length=50), current_user: str = Depends(get_current_user)):
    """
    Description:
    Cette route renvoie les offres d'emploi avec le type d'industrie dans lequel on souhaite travailler indiqué en argument.

    Args:
    - l'industrie qui nous intéresse 
    - current_user (str, dépendance): Le nom d'utilisateur actuellement authentifié.


    Returns:
    - json: une liste de jobs liés à ce pays
    Raises:
    - HTTPException: Si aucune offre n'est trouvée ou si l'utilisateur n'est pas authentifié

    """
    result = await es.search(
        index='bigdata-jobs',
        body={ 
        "query" : {
            'match': {'industry': jobIndustry}
        }
      }
    )
    final_result = result['hits']['hits']
    if not final_result:
        raise HTTPException(status_code=404, detail="Industry not found")

    return final_result
"""curl -X 'GET' \
  'http://localhost/jobs/industry/data' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X2RvY2tlciIsImV4cCI6MTczMzQyMjM5OX0.ZhFED0hrnC3BQXWiZU9khWvBDYlegcDcZZV7owzaTjU'
"""


@app.get("/company/{name_company}", tags=["companies"])
async def name_company(name_company: str = Path(..., min_length=2, max_length=50), current_user: str = Depends(get_current_user)):
    """
    Description:
    Cette route renvoie les informtions dont on dispose sur une compagnie.

    Args:
    - la compagnie qui nous intéresse 
    - current_user (str, dépendance): Le nom d'utilisateur actuellement authentifié.


    Returns:
    - json: 

    Raises:
    - HTTPException: Si aucune compagnie n'est trouvée ou si l'utilisateur n'est pas authentifié

    """
    result = await es.search(
        index='bigdata-companies',
        body={ 
        "query" : {
            'match': {'name_company': name_company}
        }
      }
    )
    final_result = result['hits']['hits']
    if not final_result:
        raise HTTPException(status_code=404, detail="company not found")

    return final_result
"""curl -X 'GET' \
  'http://localhost/company/meta' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X2RvY2tlciIsImV4cCI6MTczMzQyMjM5OX0.ZhFED0hrnC3BQXWiZU9khWvBDYlegcDcZZV7owzaTjU'
""" 


@app.get("/company/country{country}", tags=["companies"])
async def name_company(country: str = Path(..., min_length=2, max_length=50), current_user: str = Depends(get_current_user)):
    """
    Description:
    Cette route renvoie les informtions dont on dispose sur les compagnies d'un pays.

    Args:
    - le pays qui nous intéresse 
    - current_user (str, dépendance): Le nom d'utilisateur actuellement authentifié.


    Returns:
    - json: 

    Raises:
    - HTTPException: Si aucun pays n'est trouvé ou si l'utilisateur n'est pas authentifié

    """
    result = await es.search(
        index='bigdata-companies',
        body={ 
        "query" : {
            'match': {'country': country}
        }
      }
    )
    final_result = result['hits']['hits']
    if not final_result:
        raise HTTPException(status_code=404, detail="country not found")

    return final_result



indices = pd.read_pickle('/outputs/indices_reco.pkl')
distances = pd.read_pickle('/outputs/distances_reco.pkl')



async def weighted_recommendations_elk(input_job_title, k=10):
    # Recherche du job dans Elasticsearch
    result = await es.search(
        index='bigdata-jobs',
        body={
            "query": { "match": {"job_title": {"query": input_job_title, "fuzziness": "AUTO"}}},"size": 1})

    if not result['hits']['hits']:
        raise HTTPException(status_code=404, detail="No job found for the given title")

    # Récupérer l'ID Elastic du job correspondant
    job_index = result['hits']['hits'][0]['_source']['index_jobs']

    # Récupérer les indices des voisins et leurs distances via Faiss
    neighbor_indices = indices[int(job_index)]  # Assurez-vous que 'indices' est défini dans le scope
    neighbor_distances = distances[int(job_index)]  # Assurez-vous que 'distances' est défini dans le scope
    print ('neighbor_indices')
    print (neighbor_indices)
    print ('neighbor_distances')
    print (neighbor_distances)
    # Requête pour récupérer les voisins dans Elasticsearch
    neighbor_results = await es.search(
        index='bigdata-jobs',
        body={"query": {"terms": { "index_jobs": [int(job_id) for job_id in neighbor_indices]  }}} )

    # Créer un DataFrame avec les voisins récupérés depuis Elasticsearch
    recommended_jobs = pd.DataFrame([
        {
            **doc["_source"],  # Contenu des champs du document Elasticsearch
            "elastic_id": doc["_id"],  # Ajouter l'ID Elastic à chaque voisin
            "index_jobs": doc["_source"].get("index_jobs", None),  # Ajouter le champ index_jobs
            "distance": neighbor_distances[i],  # Ajouter la distance associée à chaque voisin
        }
        for i, doc in enumerate(neighbor_results['hits']['hits'])  # Parcours des documents retournés
    ])

    # Vérifier que des voisins ont été trouvés
    if recommended_jobs.empty:
        raise HTTPException(status_code=404, detail="No neighbors found for the job")

    # Indices utilisés pour la pondération 
    indices_for_weighting = [
        'Quality_of_Life_Index', 'Purchasing_Power_Index', 'Safety_Index', 'Health_Care_Index', 
        'Cost_of_Living_Index', 'Rent_Index', 'Groceries_Index', 'Crime_Index', 'Climate_Index',
        'Pollution_Index', 'Traffic_Index', 'Affordability_Index'
    ]

    for index in indices_for_weighting:
        if index not in recommended_jobs.columns:
            raise ValueError(f"Missing index in recommended jobs: {index}")

    indices_values = recommended_jobs[indices_for_weighting].values

    # Inverser les indices pour Cost_of_Living, Rent, Crime, Pollution, Traffic pour le calcul des poids
    for idx in [4, 5, 7, 9, 10]:  
        indices_values[:, idx] = 1 / (indices_values[:, idx] + 0.1)

    # Calcul des poids et distances pondérées
    weights = np.sum(indices_values, axis=1)
    weights = (weights - np.min(weights)) / (np.max(weights) - np.min(weights))  # Normalisation
    weighted_distances = np.array(neighbor_distances) * weights
    weighted_indices = np.argsort(weighted_distances)[:k]  # Top k résultats pondérés

    return recommended_jobs.iloc[weighted_indices]



@app.get("/recommender_engine/{input_job_title}", tags=["recommender engine"])
async def recommend_elastic( input_job_title:str = Path(..., min_length=3, max_length=50), current_user: str = Depends(get_current_user_unlimited)):
    """
    Description:
    Cette route renvoie les offres d'emploi que le modèle de recommandation propose en tenant en compte l'emploi souhaité et les indices de qualité de vie des villes. 

    Args:
    - le job que l'on souhaite 
    - current_user (str, dépendance): Le nom d'utilisateur actuellement authentifié. Il faut qu'il possède un accès à 'unlimited' pour avoir des résultats


    Returns:
    - json: une liste de k=10 emplois qui correspondent le mieux

    Raises:
    - HTTPException: Si aucune offre n'est trouvée ou si l'utilisateur n'est pas authentifié
    """
    try:
        recommended_jobs_w = await weighted_recommendations_elk(input_job_title)
        recommended_jobs_wjson = recommended_jobs_w.to_json(orient='records')
        return recommended_jobs_wjson
    except Exception as e:  
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


#curl -X 'GET' 'http://localhost/recommender_engine/{input_job_title}'  -H 'accept: application/json' \
 # -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X2RvY2tlciIsImV4cCI6MTczMzQyMjM5OX0.ZhFED0hrnC3BQXWiZU9khWvBDYlegcDcZZV7owzaTjU'


#if __name__ == "__main__":
#    uvicorn.run(app, host="127.0.0.1", port=2222)

#uvicorn main:app --reload --port 2222