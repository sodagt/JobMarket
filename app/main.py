from fastapi import FastAPI, Depends, HTTPException, Body, status,Query,Path
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from passlib.context import CryptContext
from typing import Optional
#import jwt
from datetime import datetime, timedelta
import pickle
import pandas as pd
import uvicorn
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi import status


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

#local path
#reco_path = '../outputs/final/indices_reco.pkl'
#distance_path = '../outputs/final/distances_reco.pkl'

#docker path
reco_path = '/outputs/indices_reco.pkl'
distance_path = '/outputs/distances_reco.pkl'

with open(reco_path, 'rb') as f:
        indices = pickle.load(f)

with open(distance_path, 'rb') as f:
        distances = pickle.load(f)

users_db = load_users_from_file("users.json")


print(users_db)


from elasticsearch import Elasticsearch

es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    basic_auth=('elastic', 'datascientest'),
    verify_certs=True,
    ca_certs='../ca/ca.crt',  # Chemin vers le certificat CA
    request_timeout=600
)

#check de la connexion
if es.ping():
    print("Connection successful")
else:
    print("Connection failed")

username='aminata4'
user = users_db.get(username)
print(user)



#Bienvenue
#@app.get("/",tags=["welcome"])
#async def read_root():
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
   # html_content = 
"""
    <html>
        <head>
            <title>Welcome to JobMarket</title>
        </head>
        <body>
            <h1>👋 Welcome to JobMarket!</h1>
            <p>Find the job you deserve! JobMarket is an easy way to search for and apply to jobs.</p>
            <p>To start, register by visiting the signup page: <a href="http://127.0.0.1:2222/register">Sign up</a></p>
            <p>Already registered? You can log in with your credentials: <a href="http://127.0.0.1:2222/user/login">Login</a></p>
            <h3>Logo</h3>
            <img src="http://127.0.0.1:2222/static/image.jpeg"" alt="JobMarket Logo" />
            <p>Once registered, you will receive a JWT token to authenticate your requests.</p>
        </body>
    </html>
"""
    #return HTMLResponse(content=html_content)
#curl -X GET "http://127.0.0.1:2222/"


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
#curl -X GET "localhost


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


#curl -X POST "http://127.0.0.1:2222/register" -H "Content-Type: application/json" -d '{"username": "sodathiam", "name": "soda thiam", "password": "soda_password", "email": "sodagayethiam@hotmail.com", "resource": "Module jobs"}'

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

#curl -X POST http://127.0.0.1:2222/user/signup

#curl -X POST http://127.0.0.1:2222/user/signup

#curl -X POST "http://127.0.0.1:2222/user/signup" -H "Content-Type: application/json" -d '{"username": "soda","password": "jeteste1"}'






#curl -X POST "http://127.0.0.1:2222/user/login" -H "Content-Type: application/json" -d '{"username": "soda","password": "jeteste1"}'

#{"access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoic29kYSIsImV4cCI6MTczMzA3MDMwOH0.rhAQKZW8SaQ-8rZ9XQzEOmf9CE6s1IiEMpyVhN0-t1M"}%  

"""
    Description:
    Cette route renvoie un message "Hello World, but secured!" uniquement si l'utilisateur est authentifié.

    Args:
    - current_user (str, dépendance): Le nom d'utilisateur de l'utilisateur actuellement authentifié.

    Returns:
    - JSON: Renvoie un JSON contenant un message de salutation sécurisé si l'utilisateur est authentifié, sinon une réponse non autorisée.

    Raises:
    - HTTPException(401, detail="Unauthorized"): Si l'utilisateur n'est pas authentifié, une exception HTTP 401 Unauthorized est levée.
    """


@app.get("/jobs/{jobTitle}" ,tags=["jobs"])
def jobs_title(
    jobTitle: str = Path(..., min_length=3, max_length=50), current_user: str = Depends(get_current_user)):
    """
    Description:
    Cette route renvoie les offres d'emploi avec le job_title indiqué en argument.

    Args:
    - le titre du job recherché.
    - current_user (str, dépendance): Le nom d'utilisateur de l'utilisateur actuellement authentifié.


    Returns:
    - json: {\"job_title\":\"Photographer\",\"contract_time\":\"Full-time\",\"company_name\":\"CoCreativ\",\"salary_min\":\"\",\"salary_max\":\"\",\"publication_datetime\":\"2024-11-09\",\"location\":\"New York, NY\",\"industry\":\"Software Development\",\"offer_link\":\"https:\\/\\/www.linkedin.com\\/jobs\\/view\\/photographer-at-cocreativ-4072854058?position=3&pageNum=0&refId=fX7x8eYibgbCmrDQUCU9cA%3D%3D&trackingId=1U6jgNN4heUk12cCYW%2FgNA%3D%3D\",\"source\":\"linkedin\",\"levels\":\"Mid-Senior level\",\"category\":\"Other\",\"contents\":\"not available\",\"publication_time\":null,\"publication_date\":\"2024-11-09\",\"contract_type\":\"not available\",\"country\":\"United States\",\"state\":\"New York\",\"city\":\"City of New York\",\"postcode\":\"10000\",\"latitude\":\"40.7127281\",\"longitude\":\"-74.0060152\",\"currency_name\":\"US Dollar\",\"job_description\":null,\"type_contract\":null,\"addressCountry\":null}.

    Raises:
    - HTTPException: Si le titre de job est invalide ou si aucune offre n'est trouvée ou l'utilisateur n'est pas authentifié

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
def jobs_country(jobCountry: str = Path(..., min_length=3, max_length=50), current_user: str = Depends(get_current_user_unlimited)):
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

#local path
#jobs_final = pd.read_pickle('../outputs/final/jobs.pkl')

#docker path#
jobs_final = pd.read_pickle('/outputs/jobs.pkl')

jobs_final = jobs_final.reset_index(drop=True)

jobs_final['job_title'].astype(str) + '-' + jobs_final['company_name'].astype(str) + '-' + jobs_final['city'].astype(str)+'-'+ jobs_final['levels'].astype(str)+'-'+ jobs_final['job_description'].astype(str)+'-'+ jobs_final['contract_type'].astype(str)


#if __name__ == "__main__":
#    uvicorn.run(app, host="127.0.0.1", port=2222)

#uvicorn main:app --reload --port 2222