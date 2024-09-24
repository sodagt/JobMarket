import sys
sys.path.append('../')

import pickle
import pandas as pd
from configs.conf import es
from common.utils import insert_data_elk

jobs = pd.read_pickle('outputs/final/jobs.pkl')
companies = pd.read_pickle('/outputs/final/companies.pkl')

''' 
#Write df in ElasticSearch

#Create index in needed
if not es.indices.exists(index="all_companies"):
   es.indices.create(index="all_companies")

companies['ingest_date'] = pd.Timestamp.now()

insert_data_elk(companies)

companies['ingest_date'] = pd.Timestamp.now()
''' 