import sys
sys.path.append('../')

import pandas as pd

from datetime import datetime

import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


#load jobs data
jobs_full = pd.read_pickle('outputs/final/jobs.pkl')

jobs_full_json = jobs_full.to_json(orient='records')
jobs_json_records = json.loads(jobs_full_json)



#load companies data
companies_full = pd.read_pickle('outputs/final/companies.pkl')
companies_full['avg_age_employees'] = companies_full['avg_age_employees'].str[:3].replace('', '0').replace('not','0').replace(' ans','').astype(float)
companies_full['nb_employee'] = companies_full['nb_employee'].replace('not available', '0').replace('', '0').astype(float)
companies_full['ingest_date'] = pd.Timestamp.now().isoformat()

companies_full_json = companies_full.to_json(orient='records')
companies_json_records = json.loads(companies_full_json)




#initialize elastic
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    basic_auth=('elastic', 'datascientest'), 
    request_timeout=600,
    verify_certs=True,  # Check SSL certificats
    ca_certs='../ca/ca.crt'  # Specify CA certificat path

)


#write data on elastif if and only if elastic is available
if es.ping():
    print("Connection successful")

    index_name_jobs = 'bigdata-jobs'
    doctype = 'census_record'

    #write jobs on elastic
    print("START writting Jobs ")

    action_list_jobs = []
    for row in jobs_json_records:
        record ={
            '_op_type': 'index',
            '_index': index_name_jobs,
            '_source': row,
            '_id': row['offer_link']

        }
        action_list_jobs.append(record)
    bulk(es, action_list_jobs)

    es.indices.refresh(index='bigdata-jobs')
    
    print("END writting Jobs ")



    #write companies on elastic
    print("START writting comapnies ")

    index_name_companies = 'bigdata-companies'
    doctype = 'census_record'


    action_list_companies = []

    #        '_id': row[key_column],
    for row in companies_json_records:
        record ={
            '_op_type': 'index',
            '_index': index_name_companies,
            '_source': row
        }
        action_list_companies.append(record)
    bulk(es,action_list_companies)

    print("END writting comapnies ")



else:
    print("Connection failed")