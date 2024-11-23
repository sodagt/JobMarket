from flask import Flask, jsonify
from flask import render_template
from flask import request
from flask import make_response 
from flask_pydantic import validate
from pydantic import BaseModel

import pandas as pd

import pprint


api = Flask(__name__)


jobs = pd.read_pickle('../outputs/final/jobs.pkl')

jobs_full = jobs[['job_title', 'contract_time', 'company_name', 'salary_min', 'salary_max', 'publication_datetime', 'location', 'industry', 'offer_link', 'source', 'levels', 'category', 'contents', 'publication_time', 'publication_date', 'contract_type', 'country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name', 'job_description', 'type_contract', 'addressCountry']]

jobs_full_json = jobs_full.to_json(orient='records')

#pprint.pprint(jobs_json, compact=True)

def handler_error404(err):
  return "You have encountered an error of 404",404

api.register_error_handler(404,handler_error404)



from elasticsearch import Elasticsearch


es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    http_auth=('elastic', 'datascientest'), 
    timeout=600,
    verify_certs=True,  # Check SSL certificats
    ca_certs='../elasticsearch/ca/ca.crt'  # Specify CA certificat path

)



class Country(BaseModel):
    country:str
    #company:str


class Jobs(BaseModel):
    title:str
    #company:str

@api.route('/status', methods=['GET'])
def status():
    try:
        return jsonify({"status": 1})
    except ValueError:
        return "L'API ne fonctionne pas",400



@api.route("/welcome/<name>")
def welcome(name):
    return "Hello {} \n. Welcome to our API.".format(name)


"""
@app.route("/jobs",methods=["POST","GET"])
def jobs():
    if request.method=="POST" :
        data = request.get_json()
        return "Hello {} \n".format(data["name"])
    return jobs_full_json


@app.route("/jobs")
@validate()
def country(query:Country):
    country_name = query.country
    jobs_country = jobs_full[jobs_full["country"] == country_name]
    jobs_country_json = jobs_country.to_json(orient='records')

    return jobs_country_json

    

@api.route("/jobs",methods=["POST","GET"])
def jobs():
    if request.method=="POST" :
        data = request.get_json()
        return "Hello {} \n".format(data["name"])
    return jobs_full_json

    """

@api.route("/jobs",methods=["POST","GET"])
def jobs():
    result = es.search(
        size=1000,
        index='bigdata-jobs',
        query={
        'match_all': {}
         }
    )

    final_result = result['hits']['hits']
    return final_result

#http://127.0.0.1:2222/jobs


@api.route("/jobs/country")
@validate()
def country(query:Country):
    country_name = query.country
    #jobs_country = jobs_full[jobs_full["country"] == country_name]
    #jobs_country_json = jobs_country.to_json(orient='records')

    result = es.search(
        index='bigdata-jobs',
        query={
        'match': {'country': country_name}
         }
    )

    final_result = result['hits']['hits']

    #return jobs_country_json
    return final_result

#http://127.0.0.1:2222/jobs/country?country=India


@api.route("/jobs/title")
@validate()
def title(query:Jobs):
    job_title_name = query.title
    #jobs_title = jobs_full[jobs_full["job_title"] == job_title_name]
    #jobs_title_json = jobs_title.to_json(orient='records')

    result = es.search(
        index='bigdata-jobs',
        query={
        'match': {'job_title': job_title_name}
         }
    )

    final_result = result['hits']['hits']


    #return jobs_title_json
    return final_result

#http://127.0.0.1:2222/jobs/title?title=photographer



if __name__ == "__main__":
    api.run(host="0.0.0.0",port="2222",debug=True)



##curl -XGET 'localhost:9200/bigdata-jobs/doc/_validate?q=field:value’