#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#import re
# Affiche le répertoire courant
#current_directory = os.getcwd()
#print("Répertoire courant :", current_directory)
#import os
#os.chdir('/Users/sodagayethiam/Documents/Formations/Parcours_data_engineer/projet_jobmarket/JobMarket/processing')


# In[6]:


#import packages
import sys
sys.path.append('../')
import glob 

import pandas as pd
from tqdm import tqdm
tqdm.pandas()  # Init tqdm for seeing progression of apply in pandas
from common.utils import translate_to_english
from common.utils import clean_text
from common.utils import extract_salary_info
from common.utils import get_infos_location, load_location_cache, save_location_cache

import pickle
from datetime import datetime
from bs4 import BeautifulSoup
pd.set_option('display.max_columns', None)  # show all columns of the dataframe 
from langdetect import detect

#pd.set_option('display.max_colwidth', None)  # show all lines of the dataframe 


# In[2]:


#define data and cache paths
raw_path = '../outputs/raw/'
cache_path = '../outputs/intermediate/location_cache.pkl'

#read the data
jobs_adzuna_pkl = glob.glob(f'{raw_path}*jobs_adzuna*.pkl')
jobs_adzuna = pd.concat([pd.read_pickle(file) for file in jobs_adzuna_pkl], axis=0)

jobs_themuse_pkl = glob.glob(f'{raw_path}*jobs_muse*.pkl')
jobs_themuse = pd.concat([pd.read_pickle(file) for file in jobs_themuse_pkl], axis=0)

jobs_linkedin_pkl = glob.glob(f'{raw_path}*jobs_linkedin*.pkl')
jobs_linkedin = pd.concat([pd.read_pickle(file) for file in jobs_linkedin_pkl], axis=0)

jobs_wttj_pkl  = glob.glob(f'{raw_path}*jobs_wttj*.pkl')
jobs_wttj = pd.concat([pd.read_pickle(file) for file in jobs_wttj_pkl], axis=0)

print('shape jobs_adzuna:',jobs_adzuna.shape)
print('shape jobs_linkedin:',jobs_linkedin.shape)
print('shape jobs_themuse:',jobs_themuse.shape)
print('shape jobs_wttj:',jobs_wttj.shape)


# In[3]:


print('Treatment of jobs adzuna')
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


# In[4]:


#jobs_adzuna_selected.location.nunique() #16329
#jobs_adzuna_selected.location.count() 


# In[5]:



load_location_cache(cache_path)
jobs_adzuna_selected [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= jobs_adzuna_selected['location'].progress_apply(get_infos_location)


# In[ ]:


jobs_adzuna_selected.columns


# In[ ]:


#location_cache['Phoenix, eThekwini']
#correction_location('Phoenix, eThekwini')


# In[51]:


jobs_adzuna_selected.head()


# In[33]:


#save jobs_adzuna_selected
#jobs_adzuna_selected.to_pickle('../outputs/intermediate/jobs_adzuna_selected.pkl')


# In[55]:


#save jobs_adzuna_selected
#jobs_adzuna_selected.to_pickle('../outputs/intermediate/jobs_adzuna_selected.pkl')
jobs_adzuna_selected=pd.read_pickle('../outputs/intermediate/jobs_adzuna_selected.pkl')


# In[ ]:


jobs_themuse.head() 


# In[6]:


print('Treatment of jobs The Muse')

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


# In[7]:


load_location_cache(cache_path)
jobs_theMuse_selected [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= jobs_theMuse_selected['location'].progress_apply(get_infos_location)


# In[28]:



#df[['currency', 'min_salary', 'max_salary']] = df['salary'].apply(extract_salary_info)
jobs_wttj.head()


# In[29]:


print('Treatment of jobs Welcome to te jungle')

jobs_wttj_selected = jobs_wttj[['title', 'company', 'type_contract', 'contents', 'date_public',
       'employmentType','addressCountry', 'addressLocality',  'industry', 'link_job', 'salary', 'postalCode', 'streetAddress']].rename(
columns={'title': 'job_title', 'contents': 'job_description',
'employmentType': 'contract_time','company': 'company_name','date_public': 'publication_datetime','addressLocality': 'location','type_contract':'contract_type',
'addressCountry': 'country_wttj','link_job': 'offer_link','streetAddress': 'streetAdress_wttj','postalCode': 'postalCode_wttj'})



jobs_wttj_selected['publication_date'] = pd.to_datetime(jobs_wttj_selected['publication_datetime']).dt.strftime("%Y-%m-%d")
jobs_wttj_selected['publication_time'] = jobs_wttj_selected['publication_datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").time())
jobs_wttj_selected['industry']='not available'
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


# In[30]:


#cache_path = '../outputs/intermediate/location_cache.pkl'

load_location_cache(cache_path)
jobs_wttj_selected [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= jobs_wttj_selected['streetAdress_wttj'].progress_apply(get_infos_location)

print( 'taille jobs_theMuse_selected', jobs_wttj_selected.shape)
#save_location_cache(cache_path)


# In[ ]:


#jobs_linkedin.link.unique()
jobs_linkedin.head()


# In[12]:


print('Treatment of jobs Linkedin')

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


# In[13]:



load_location_cache(cache_path)

jobs_linkedin_selected [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= jobs_linkedin_selected['location'].progress_apply(get_infos_location)


# In[31]:


print('Fusion of the 4 sources')

jobs1 = pd.concat([jobs_linkedin_selected, jobs_theMuse_selected], axis=0, ignore_index=True)
#jobs1 = jobs1.reset_index(drop=True)
jobs2 = pd.concat([jobs1, jobs_adzuna_selected], axis=0, ignore_index=True)
#jobs2 = jobs2.reset_index(drop=True)
#jobs_wttj_selected = jobs_wttj_selected.reset_index(drop=True)

jobs_final = pd.concat([jobs2, jobs_wttj_selected], axis=0, ignore_index=True)
jobs_final=jobs_final.drop_duplicates()
#jobs = pd.concat([jobs2, jobs_wttj_selected], axis=0)

shape=jobs_final.shape
print(f"shape of fusionned dataframe : {shape}")
#Taille du dataframe fusionné : (16269, 21)


# In[80]:


#clean the final dataframe
#jobs_final=jobs_final.drop("country_adzuna", axis=1)
jobs_final.country.value_counts()
#pd.crosstab(jobs_final.currency_name, jobs_final.country_code)
#jobs_final.source.value_counts()
#pd.crosstab(jobs_final.country, jobs_final.country_adzuna)
#pd.crosstab(jobs_final.country, jobs_final.country_wttj)


# In[ ]:


#Correct country whch are not found by geopy from the location via the country in adzuna

country_dict = { 'au': 'Australia', 'be': 'België / Belgique / Belgien', 'br': 'Brasil', 'ca': 'Canada','de': 'Deutschland','es': 'España', 
                  'fr': 'France', 'gb': 'United Kingdom', 'in': 'India','mx': 'México', 'nl': 'Nederland', 'nz': 'New Zealand / Aotearoa',
                  'pl': 'Polska', 'sg': 'Singapore', 'us': 'United States','za': 'South Africa'}


jobs_final = jobs_final.reset_index(drop=True)
jobs_final.loc[jobs_final['country'] == 'Not found', 'country'] = jobs_final.apply( lambda row: country_dict.get(row['country_adzuna'], 'Unknown'), axis=1)
jobs_final=jobs_final.drop("country_adzuna", axis=1)

#jobs_final.country.value_counts()
#jobs_final['contents'] = jobs_final['contents'].progress_apply(translate_to_english)


#jobs_final.currency_name.value_counts()
#jobs_final[jobs_final.country=='Not found']


# In[95]:


jobs_final = jobs_final[jobs_final['job_title'].notnull()] #make sure we have'nt jobs without title

#standardise contract_time
jobs_final['contract_time'] = jobs_final['contract_time'].fillna('not available')
jobs_final['contract_time'] = jobs_final['contract_time'].str.lower().str.replace('-', '_').str.strip()
jobs_final.loc[jobs_final['contract_time'] == "internship", 'contract_time'] = 'intern'
jobs_final.loc[jobs_final['contract_time'] == "contract", 'contract_time'] = 'contractor'

#standardise company_name
jobs_final['company_name'] = jobs_final['company_name'].fillna('not available')
jobs_final['company_name'] = jobs_final['company_name'].str.upper().str.strip()

#replace the value unknow in industry
jobs_final.loc[jobs_final['industry'] == "Unknown", 'industry'] = 'not available'

#clean salary and replace the unknown values by 0
jobs_final['salary_min'] = pd.to_numeric(jobs_final['salary_min'], errors='coerce').fillna(0)
jobs_final['salary_max'] = pd.to_numeric(jobs_final['salary_max'], errors='coerce').fillna(0)

#format to datetime
jobs_final['publication_date'] = pd.to_datetime(jobs_final['publication_date'], errors='coerce')
jobs_final['publication_time'] = pd.to_datetime(jobs_final['publication_time'],  format='%H:%M:%S',errors='coerce')
jobs_final['publication_time'] = jobs_final['publication_time'].apply(lambda x: x.time() if pd.notna(x) else pd.NaT)
jobs_final['publication_time'] = jobs_final['publication_time'].fillna(pd.to_datetime('00:00:00').time())

#replace the industry by category where we haven't values 
jobs_final = jobs_final.reset_index(drop=True)
jobs_final.loc[jobs_final['industry'] == "not available", 'industry'] = jobs_final['category']

jobs_final['levels'] = jobs_final['levels'].str.lower().str.strip()

jobs_final['job_description'] = jobs_final['job_description'].fillna('not available')

jobs_final['contract_type'] = jobs_final['contract_type'].fillna('not available')
jobs_final['contract_type'] = jobs_final['contract_type'].str.lower().str.strip()
jobs_final.loc[jobs_final['contract_type'] == "permanent contract", 'contract_type'] = 'permanent'

#replace not found coordonate
jobs_final['latitude'] = jobs_final['latitude'].replace('Not found', '0').astype(float)
jobs_final['longitude'] = jobs_final['longitude'].replace('Not found', '0').astype(float)

#replace Unknown values by not found to standardize
jobs_final.loc[jobs_final['state'] == "Unknown", 'state'] = 'Not found'
jobs_final.loc[jobs_final['city'] == "Unknown", 'city'] = 'Not found'
jobs_final.loc[jobs_final['postcode'] == "Unknown", 'postcode'] = 'Not found'

#replace specific values of countries not found by the script
jobs_final.loc[jobs_final['country'] == "Australia", 'country_code'] = 'au'
jobs_final.loc[jobs_final['country'] == "United Kingdom", 'country_code'] = 'gb'
jobs_final.loc[jobs_final['country'] == "België / Belgique / Belgien", 'country_code'] = 'be'

#replace unknowns currencies 
currency_dict = {  'be': 'Euro', 'fr': 'Euro','br': 'BRL',  'au': 'Australian Dollar',  'es': 'Euro', 
    'pl': 'Polish Zloty',  'it': 'Euro', 'de': 'Euro', 'nl': 'Euro',  'gb': 'Pound Sterling', 'cf': 'CFA Franc',  'at': 'Euro',  'ci': 'CFA Franc', 'pt': 'Euro',  # Portugal
    'tr': 'Turkish Lira', 'sk': 'EUR',  'rs': 'Serbian Dinar',  'ie': 'Euro',  # Ireland
    'ro': 'Romanian Leu',  'bg': 'Bulgarian Lev',  'tw': 'New Taiwan Dollar', 'ee': 'EUR',   'lt': 'EUR',  'gr': 'EUR', 'mc': 'EUR' }
jobs_final = jobs_final.reset_index(drop=True)
jobs_final.loc[jobs_final['currency_name'].isin(['Unknown', 'Not found']), 'currency_name'] = jobs_final.apply( lambda row: currency_dict.get(row['country_code'], 'Unknown'), axis=1)

#Clean the html balises
jobs_final['job_description'] =jobs_final['job_description'].apply(lambda x: BeautifulSoup(x, "html.parser").get_text())
jobs_final['job_description'] = jobs_final['job_description'].apply(clean_text)

#translate text from french or german to english
jobs_final['job_title'] = jobs_final['job_title'].progress_apply(translate_to_english)
jobs_final['job_description'] = jobs_final['job_description'].progress_apply(translate_to_english)

jobs_final['lang'] = jobs_final['job_description'].progress_apply(detect)


# In[ ]:



"""from functools import lru_cache
@lru_cache(maxsize=None)
def cached_translate(text):
    return translate_to_english(text)

jobs_final['job_description'] = jobs_final['job_description'].progress_apply(cached_translate)"""


# In[20]:


jobs_final['lang'].value_counts()


# In[ ]:


town_data=pd.read_pickle('../outputs/intermediate/town_data.pkl')
town_data.country_code.value_counts(dropna=False)
#calculate the indicator of the country by taking the average of the cities to fill the NA values 
indicator_avg_by_country = town_data.groupby('country_code')[[
    'Quality_of_Life_Index', 'Purchasing_Power_Index', 'Safety_Index', 'Health_Care_Index',
    'Cost_of_Living_Index', 'Rent_Index', 'Groceries_Index', 'Crime_Index', 
    'Climate_Index', 'Pollution_Index', 'Traffic_Index', 'Affordability_Index'
]].mean()

city_country = pd.merge(town_data, indicator_avg_by_country, on='country_code', how='left', suffixes=('', '_country_avg'))
#mzrge jobs with informations about city
jobs = pd.merge(jobs_final,  town_data[['Quality_of_Life_Index', 'Purchasing_Power_Index',
       'Safety_Index', 'Health_Care_Index', 'Cost_of_Living_Index',
       'Rent_Index', 'Groceries_Index', 'Crime_Index', 'Climate_Index',
       'Pollution_Index', 'Traffic_Index', 'Affordability_Index',
       'city']], on='city', how='left')
#merge jobs with informations about countries
jobs_city_country = pd.merge(jobs, indicator_avg_by_country, on='country_code', how='left', suffixes=('', '_country_avg'))
#replace the nan values for cities with values of the countries  

index_life_col = ['Quality_of_Life_Index', 'Purchasing_Power_Index', 'Safety_Index', 'Health_Care_Index', 'Cost_of_Living_Index',
       'Rent_Index', 'Groceries_Index', 'Crime_Index', 'Climate_Index', 'Pollution_Index', 'Traffic_Index', 'Affordability_Index']

for column in index_life_col:
    jobs_city_country[column] = jobs_city_country[column].fillna(jobs_city_country[f'{column}_country_avg'])

# delete avg indicators by country in final dataframe
jobs_city_country.drop(columns=[f'{column}_country_avg' for column in index_life_col], inplace=True)
#replace the last NA values by the mean
jobs_city_country[index_life_col] = jobs_city_country[index_life_col].fillna(jobs_city_country[index_life_col].mean())

#pd.crosstab(jobs_final.currency_name, jobs_final.country)


# In[171]:



#jupyter nbconvert --to script process_jobs.ipynb


# In[28]:


jobs_final.loc[jobs_final['country_code'].isin( ['Not found']), 'country'].value_counts()
jobs_final.currency_name.value_counts()


# In[232]:


#Avoid  double entries in the dataframe

jobs_final.loc[:, 'clef_doublons'] = jobs_final['job_title'].astype(str) + '-' + jobs_final['company_name'].astype(str) + '-' + jobs_final['city'].astype(str)+'-'+ jobs_final['levels'].astype(str)+'-'+ jobs_final['job_description'].astype(str)+'-'+ jobs_final['contract_type'].astype(str)
doublons_freq = jobs_final['clef_doublons'].value_counts()
print(doublons_freq[doublons_freq > 1])  # Afficher uniquement les doublons

aggregation_rules = {col: 'first' for col in jobs_final.columns}
aggregation_rules.update({
    'salary_min': 'max',  
    'salary_max': 'max',  
    'publication_datetime': 'max',  
    'publication_date': 'max',  
    'publication_time': 'max',  
    'publication_date': 'min',    
})
jobs_final = jobs_final.groupby('clef_doublons').agg(aggregation_rules).reset_index(drop=True)
jobs_final = jobs_final.drop('clef_doublons', axis=1)

jobs_final.shape


# In[23]:


columns_to_keep = [ 'job_title', 'contract_time', 'company_name', 'salary_min',
    'salary_max', 'publication_datetime', 'location', 'industry',
    'offer_link', 'source', 'levels', 'category', 'job_description',
    'publication_time', 'publication_date', 'contract_type', 'country',
    'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name',
    'country_code', 'codeIso_lvl4', 'lang', 'Quality_of_Life_Index',
    'Purchasing_Power_Index', 'Safety_Index', 'Health_Care_Index',
    'Cost_of_Living_Index', 'Rent_Index', 'Groceries_Index', 'Crime_Index',
    'Climate_Index', 'Pollution_Index', 'Traffic_Index',
    'Affordability_Index']

jobs_final = jobs_final[columns_to_keep]
jobs_final=jobs_final[jobs_final['lang'] == 'en']


# In[25]:


jobs_final.to_pickle('../outputs/final/jobs.pkl')
#jobs_final=pd.read_pickle('../outputs/final/jobs.pkl')
#'''FINNNNNNNNNNNNNNNNNNNNN'''

