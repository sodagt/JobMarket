#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import re
# Affiche le répertoire courant
#current_directory = os.getcwd()
#print("Répertoire courant :", current_directory)
import os
os.chdir('/Users/sodagayethiam/Documents/Formations/Parcours_data_engineer/projet_jobmarket/JobMarket/processing')


# In[1]:


import sys
sys.path.append('../')
import glob 

import pandas as pd
from tqdm import tqdm
tqdm.pandas()  # Initialiser tqdm pour pandas
from common.utils import translate_to_english
from common.utils import clean_text
from common.utils import extract_salary_info
from common.utils import get_infos_location, load_location_cache, save_location_cache

import pickle
from datetime import datetime
#pd.set_option('display.max_columns', None)  # Affiche toutes les colonnes
#pd.set_option('display.max_colwidth', None)  # Affiche tout le contenu des cellules


# In[52]:


import glob 
raw_path = '../outputs/raw/'
cache_path = '../outputs/intermediate/location_cache.pkl'


jobs_adzuna_pkl = glob.glob(f'{raw_path}*jobs_adzuna*.pkl')
jobs_adzuna = pd.concat([pd.read_pickle(file) for file in jobs_adzuna_pkl], axis=0)

jobs_themuse_pkl = glob.glob(f'{raw_path}*jobs_muse*.pkl')
jobs_themuse = pd.concat([pd.read_pickle(file) for file in jobs_themuse_pkl], axis=0)

jobs_linkedin_pkl = glob.glob(f'{raw_path}*jobs_linkedin*.pkl')
jobs_linkedin = pd.concat([pd.read_pickle(file) for file in jobs_linkedin_pkl], axis=0)

jobs_wttj_pkl  = glob.glob(f'{raw_path}*jobs_wttj*.pkl')
jobs_wttj = pd.concat([pd.read_pickle(file) for file in jobs_wttj_pkl], axis=0)

print('taille jobs_adzuna:',jobs_adzuna.shape)
print('taille jobs_linkedin:',jobs_linkedin.shape)
print('taille jobs_themuse:',jobs_themuse.shape)
print('taille jobs_wttj:',jobs_wttj.shape)


# In[4]:


jobs_adzuna_selected = jobs_adzuna[['title','description','contract_type', 'contract_time',
'company.display_name', 'created','location.display_name','category.label', 'salary_max' ,'salary_min', #'location.area',
'redirect_url','source', 'country']].rename(columns={'title': 'job_title', 'description': 'job_description',
'company.display_name': 'company_name','created': 'publication_datetime','location.display_name': 'location','category.label': 'category',
'redirect_url': 'offer_link', 'country':'country_adzuna'})
#'location.area': 'locations_company',

jobs_adzuna_selected['publication_date'] = pd.to_datetime(jobs_adzuna_selected['publication_datetime']).dt.strftime("%Y-%m-%d")

jobs_adzuna_selected['publication_time'] = jobs_adzuna_selected['publication_datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").time())
#jobs_adzuna_selected['country']='US'
jobs_adzuna_selected['industry']='not available'
#jobs_adzuna_selected['currency']='dollars'
jobs_adzuna_selected['levels']='not available'


#jobs_adzuna_selected['locations_company'] = jobs_adzuna_selected['locations_company'].astype(str)
#jobs_adzuna_selected['locations_company'] = jobs_adzuna_selected['locations_company'].str.replace(r'[\[\]]', '', regex=True)
jobs_adzuna_selected.head()

print( 'taille jobadzuna avant dedoubloonnage', jobs_adzuna_selected.shape)
#companies['contents'] = companies['description'].apply(clean_text)
jobs_adzuna_selected=jobs_adzuna_selected.drop_duplicates()
print( 'taille jobadzuna apres dedoubloonnage', jobs_adzuna_selected.shape)


# In[5]:


#locat='Fort Douglas, Salt Lake County'
#get_infos_location(locat)

jobs_adzuna_selected.location.nunique() #16329
jobs_adzuna_selected.location.count() 


# In[6]:





# In[ ]:



load_location_cache(cache_path)
jobs_adzuna_selected [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= jobs_adzuna_selected['location'].progress_apply(get_infos_location)





# In[58]:


jobs_theMuse_selected = jobs_themuse[['name','contents','type', 'company.name',
'publication_date', 'locations','categories','refs.landing_page','levels.name']].rename(columns={'name': 'job_title', 'contents': 'job_description',
'type': 'contract_type','company.name': 'company_name','publication_date': 'publication_datetime','locations': 'location',
'categories': 'category','refs.landing_page': 'offer_link','levels.name':'levels'})

jobs_theMuse_selected['publication_date'] = pd.to_datetime(jobs_theMuse_selected['publication_datetime']).dt.strftime("%Y-%m-%d")
jobs_theMuse_selected['publication_time'] = jobs_theMuse_selected['publication_datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").time())

#jobs_theMuse_selected['country']='a travailler'
jobs_theMuse_selected['industry']='not available'
#jobs_theMuse_selected['currency']='not available'
jobs_theMuse_selected['contract_time']='not available'
jobs_theMuse_selected['salary_max']=0
jobs_theMuse_selected['salary_min']=0

jobs_theMuse_selected['location'] = jobs_theMuse_selected['location'].astype(str)
jobs_theMuse_selected['location'] = jobs_theMuse_selected['location'].str.replace(r'[\[\]]', '', regex=True)
#jobs_theMuse_selected['locations_company']=jobs_theMuse_selected['location']

jobs_theMuse_selected['job_description'] = jobs_theMuse_selected['job_description'].apply(clean_text)

jobs_theMuse_selected["source"] = "TheMuse"
jobs_theMuse_selected=jobs_theMuse_selected.drop_duplicates()
print( 'taille jobs_theMuse apres dedoubloonnage', jobs_theMuse_selected.shape)

jobs_theMuse_selected.head()


# In[59]:



load_location_cache(cache_path)
jobs_theMuse_selected [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= jobs_theMuse_selected['location'].progress_apply(get_infos_location)




jobs_wttj_selected = jobs_wttj[['title', 'company', 'type_contract', 'contents', 'date_public',
       'employmentType','addressCountry', 'addressLocality',  'industry', 'link_job', 'salary', 'postalCode', 'streetAddress']].rename(
columns={'title': 'job_title', 'contents': 'job_description',
'employmentType': 'contract_type','company': 'company_name','date_public': 'publication_datetime','addressLocality': 'location','type_contract':'contract_type',
' addressCountry': 'country_wttj','link_job': 'offer_link','streetAddress': 'streetAdress_wttj','postalCode': 'postalCode_wttj'})



jobs_wttj_selected['publication_date'] = pd.to_datetime(jobs_wttj_selected['publication_datetime']).dt.strftime("%Y-%m-%d")
jobs_wttj_selected['publication_time'] = jobs_wttj_selected['publication_datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").time())
jobs_wttj_selected['industry']='not available'
jobs_wttj_selected['contract_time']='not available'
jobs_wttj_selected['category']='not available'

jobs_wttj_selected[['currency_wttj', 'salary_min', 'salary_max']] = jobs_wttj_selected['salary'].apply(extract_salary_info)
jobs_wttj_selected = jobs_wttj_selected.drop('salary', axis=1)

jobs_wttj_selected['location'] = jobs_wttj_selected['location'].astype(str)
jobs_wttj_selected['location'] = jobs_wttj_selected['location'].str.replace(r'[\[\]]', '', regex=True)
#jobs_wttj_selected['locations_company']=jobs_wttj_selected['location']

jobs_wttj_selected['job_description'] = jobs_wttj_selected['job_description'].apply(clean_text)
jobs_wttj_selected["levels"] = "not available"

jobs_wttj_selected["source"] = "Wttj"
jobs_wttj_selected=jobs_wttj_selected.drop_duplicates()


jobs_wttj_selected.head()


# In[76]:



load_location_cache(cache_path)
jobs_wttj_selected [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= jobs_wttj_selected['streetAdress_wttj'].progress_apply(get_infos_location)

print( 'taille jobs_theMuse_selected', jobs_wttj_selected.shape)




jobs_linkedin_selected = jobs_linkedin[['title','employment_type', 'company','salary_min','salary_max',
'posted_time','location','job_industry','link','source','seniority_level','job_function']].rename(columns={'title': 'job_title', 'employment_type': 'contract_time',
'company': 'company_name','posted_time': 'publication_datetime','job_industry': 'industry','link': 'offer_link','seniority_level':'levels','job_function':'category'})

#jobs_linkedin_selected['publication_date'] = pd.to_datetime(jobs_adzuna_selected['publication_datetime']).dt.strftime("%Y-%m-%d")
jobs_linkedin_selected['job_description']='not available'
#jobs_linkedin_selected['currency']='dollars'
jobs_linkedin_selected['publication_time'] =None

jobs_linkedin_selected['publication_date'] = pd.to_datetime(jobs_linkedin_selected['publication_datetime']).dt.strftime("%Y-%m-%d")

jobs_linkedin_selected['contract_type']='not available'

#jobs_linkedin_selected['locations_company']='not available'

#jobs_linkedin_selected['job_description'] = jobs_linkedin_selected['job_description'].apply(clean_text)
jobs_linkedin_selected.head()


load_location_cache(cache_path)

jobs_linkedin_selected [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= jobs_linkedin_selected['location'].progress_apply(get_infos_location)


# In[ ]:





# In[83]:


jobs1 = pd.concat([jobs_linkedin_selected, jobs_theMuse_selected], axis=0)
jobs2 = pd.concat([jobs1, jobs_adzuna_selected], axis=0)
jobs_final = pd.concat([jobs2, jobs_wttj_selected], axis=0)
jobs_final=jobs_final.drop_duplicates()
#jobs = pd.concat([jobs2, jobs_wttj_selected], axis=0)

shape=jobs_final.shape
print(f"Taille du dataframe fusionné : {shape}")
#Taille du dataframe fusionné : (16269, 21)
jobs_final.to_pickle('../outputs/final/jobs.pkl')


# In[84]:


jobs_final.columns


# In[85]:


jobs_final=jobs_final.drop("country_adzuna", axis=1)




# In[99]:


pd.set_option('display.max_columns', None)  # Affiche toutes les colonnes

jobs_final[jobs_final.country=='Not found']


# In[16]:


jobs_final.head()
jobs_final['job_description'] = jobs_final['job_description'].progress_apply(translate_to_english)




