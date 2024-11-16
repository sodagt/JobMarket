"""
Cette classe nous permet de webscrapper les données du site Adzuna.

"""
import sys
import time

#print("Before Modified sys.path:", sys.path)
#sys.path.append('../..')
# Print the modified sys.path
#print("Modified sys.path:", sys.path)

#from sys import argv


import requests
from bs4 import BeautifulSoup as bs_adzuna
import pandas as pd
import json


#from elasticsearch import Elasticsearch
#from elasticsearch.helpers import bulk

#from configs.conf import es
#from common.utils import insert_data_elk



#url_adzuna = "https://api.adzuna.com/v1/api/jobs/us/search/1?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&content-type=text/html"
url_adzuna = "https://api.adzuna.com/v1/api/jobs/us/search/1?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&results_per_page=100"
url_adzuna_gb = "https://api.adzuna.com/v1/api/jobs/gb/search/1?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&results_per_page=1000"



#countries = ['au','be','br','ca','ch','de','es','fr','gb','in','it','mx','nl','nz','pl','sg','us','za']
countries = ['fr','gb','in','it','mx','nl','nz','pl','sg','us','za']

#countries = ['au','be']
#us as united states, gb as great britain, at as autralia
#avaailables countries are au, be, br, ca, ch, de, es, fr, gb, in, it, mx, nl, nz, pl, sg, us, za

"""
page_adzuna = requests.get(url_adzuna)
soup_adzuna = bs_adzuna(page_adzuna.content,"lxml")
#print(soup_adzuna)
#print(soup_adzuna.prettify())

#html body dl dd ol li dt

# START JSON PART
data = page_adzuna.json()
output = json.dumps(data, indent=4)
#print(output)


recs = data['results']
df = pd.json_normalize(recs)
#print(df.shape)


#df_test = pd.DataFrame(df)

#print(df_test.shape)
#print(df.info)
#print(df['salary_max'])
"""

#get data and create data frame
#for country in countries:
#    page_adzuna_new = requests.get("https://api.adzuna.com/v1/api/jobs/"+country+"/search/"+str(i)+"?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&results_per_page=100")

for country in countries:

    df = pd.DataFrame()


    print(country)

    error_page = 0

    for page in range(1,1200):
        print(page)
        time.sleep(2)

        #new_url = "https://api.adzuna.com/v1/api/jobs/us/search/"+str(i)+"?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff"
        #first key id
        url = "https://api.adzuna.com/v1/api/jobs/"+str(country)+"/search/"+str(page)+"?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&results_per_page=100"
                
        #second key id
        #url = "https://api.adzuna.com/v1/api/jobs/"+str(country)+"/search/"+str(page)+"?app_id=203b447b&app_key=d5b8b476bf953c4fefe2d21abf95bd92&results_per_page=100"

        #print(url)
        page_adzuna_new = requests.get(url)
        if page_adzuna_new.status_code == 200:
            try:
                new_data = page_adzuna_new.json()
                recs_new = new_data['results']
                df_new = pd.json_normalize(recs_new)
                df = pd.concat([df, df_new]).reset_index(drop=True)
                df["source"] = "adzuna"
                df['country']=  country
            except ValueError:
                print("Response is not valid JSON:", page_adzuna_new.text)
        else:
            print(f"Error: {page_adzuna_new.status_code}, Response: {page_adzuna_new.text}")
            error_page +=1

            if (error_page == 30): 
                print("Switching Country")
                countries_iter = iter(countries)         # Convert list to iterator
                country = next(countries_iter, None)      # Get the next item or None if at the end
                print(country)
                error_page = 0
                df = pd.DataFrame()
                break


    df.to_pickle('outputs/raw/jobs_adzuna_'+country+'_nov.pkl')



    
print("/////////////////////////////////////")
print(df.head(5))
#print(df.columns)
#print(df['company.display_name'].head(5))

#save raw data
#df.to_pickle('outputs/raw/jobs_adzuna_us_nov.pkl')

''' 
#Write df in ElasticSearch

#Create index in needed
if not es.indices.exists(index="bigdata-adzuna"):
   es.indices.create(index="bigdata-adzuna")




insert_data_elk(df)

df['ingest_date'] = pd.Timestamp.now()

# Search document
res = es.search(index="bigdata-adzuna", body={"query": {"match_all": {}}})
print(res)

'''