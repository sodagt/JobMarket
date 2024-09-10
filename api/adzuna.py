"""
Cette classe nous permet de webscrapper les données du site Adzuna.

"""
import sys
sys.path.append('../')

#from sys import argv


import requests
from bs4 import BeautifulSoup as bs_adzuna
import pandas as pd
import json


#from elasticsearch import Elasticsearch
#from elasticsearch.helpers import bulk

from configs.conf import es
from common.utils import insert_data_elk



#url_adzuna = "https://api.adzuna.com/v1/api/jobs/us/search/1?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&content-type=text/html"
url_adzuna = "https://api.adzuna.com/v1/api/jobs/us/search/1?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&results_per_page=100"
page_adzuna = requests.get(url_adzuna)
soup_adzuna = bs_adzuna(page_adzuna.content,"lxml")

#html body dl dd ol li dt

#print(soup_adzuna)
#print(soup_adzuna.prettify())


# START JSON PART
#
data = page_adzuna.json()
output = json.dumps(data, indent=4)
print(output)


recs = data['results']
df = pd.json_normalize(recs)
#df_test = pd.DataFrame(df)
print(df.shape)
#print(df_test.shape)
#print(df.info)
#print(df['salary_max'])


#get data and create data frame
for i in range(1,2):
    #new_url = "https://api.adzuna.com/v1/api/jobs/us/search/"+str(i)+"?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff"
    page_adzuna_new = requests.get("https://api.adzuna.com/v1/api/jobs/us/search/"+str(i)+"?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&results_per_page=100")
    new_data = page_adzuna_new.json()
    recs_new = new_data['results']
    df_new = pd.json_normalize(recs_new)
    df = pd.concat([df, df_new]).reset_index(drop=True)
    df["source"] = "adzuna"

    
print("/////////////////////////////////////")
print(df.head(5))
#print(df.columns)
#print(df['company.display_name'].head(5))



#Write df in ElasticSearch

#Create index in needed
if not es.indices.exists(index="bigdata-adzuna"):
   es.indices.create(index="bigdata-adzuna")



insert_data_elk(df)

df['ingest_date'] = pd.Timestamp.now()

# Search document
res = es.search(index="bigdata-adzuna", body={"query": {"match_all": {}}})
print(res)