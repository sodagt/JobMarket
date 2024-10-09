import sys
sys.path.append('../')

import pickle
import pandas as pd
from configs.conf import es
from common.utils import insert_data_elk

jobs = pd.read_pickle('../outputs/final/jobs.pkl')
companies = pd.read_pickle('../outputs/final/companies.pkl')


companies = companies.fillna('')

#Write df in ElasticSearch

#Create index in needed
if not es.indices.exists(index="all_companies"):
   es.indices.create(index="all_companies")

companies['ingest_date'] = pd.Timestamp.now()
print(companies.head())

#companies['publication_date'] = companies['publication_date'].where(companies['publication_date']=='not available', None)
companies['publication_date'] = companies['publication_date'].replace('not available', None)
companies['publication_date'] = companies['publication_date'].replace('', None)



insert_data_elk(companies)


res = es.search(index="all_companies", body={"query": {"match_all": {}}})
print(res)


#insert_data_elk2(companies,'all_companies' )

#print (companies.publication_date)
