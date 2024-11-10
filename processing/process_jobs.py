#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import re
# Affiche le répertoire courant
current_directory = os.getcwd()
print("Répertoire courant :", current_directory)


# In[2]:


import sys
sys.path.append('../')

import pandas as pd
from tqdm import tqdm
tqdm.pandas()  # Initialiser tqdm pour pandas
from common.utils import translate_to_english
from common.utils import clean_text
from common.utils import extract_salary_info

import pickle
from datetime import datetime


# In[3]:


#jobs_adzuna= pd.read_pickle('../outputs/raw/jobs_adzuna_sept.pkl')
jobs_adzuna= pd.read_pickle('../outputs/raw/jobs_adzuna_nov.pkl')
jobs_themuse = pd.read_pickle('../outputs/raw/jobs_muse_sept.pkl')
jobs_wttj = pd.read_pickle('../outputs/raw/jobs_wttj_aout.pkl')
jobs_linkedin = pd.read_pickle('../outputs/raw/jobs_linkedin_nov.pkl')


# In[4]:


jobs_adzuna2= pd.read_pickle('../outputs/raw/jobs_adzuna_us_nov.pkl')
jobs_adzuna3= pd.read_pickle('../outputs/raw/jobs_adzuna_sept.pkl')
jobs_adzuna= pd.read_pickle('../outputs/raw/jobs_adzuna_nov.pkl')

print(jobs_adzuna.shape)
print(jobs_adzuna2.shape)
print(jobs_adzuna3.shape)

jobs_adzuna = pd.concat([jobs_adzuna, jobs_adzuna2], axis=0)
jobs_adzuna = pd.concat([jobs_adzuna, jobs_adzuna3], axis=0)
jobs_adzuna.shape

#jobs_linkedin2 = pd.read_pickle('../outputs/raw/jobs_linkedin_sept.pkl')


# In[ ]:





# In[5]:


for column in jobs_adzuna.columns:
    print(column, type(jobs_adzuna[column].iloc[0]))
jobs_adzuna.head()


# In[6]:


jobs_adzuna_selected = jobs_adzuna[['title','description','contract_type', 'contract_time',
'company.display_name', 'created','location.display_name','category.label', 'salary_max' ,'salary_min', #'location.area',
'redirect_url','source']].rename(columns={'title': 'job_title', 'description': 'job_description',
'company.display_name': 'company_name','created': 'publication_datetime','location.display_name': 'location','category.label': 'category',
'redirect_url': 'offer_link'})
#'location.area': 'locations_company',

jobs_adzuna_selected['publication_date'] = pd.to_datetime(jobs_adzuna_selected['publication_datetime']).dt.strftime("%Y-%m-%d")

jobs_adzuna_selected['publication_time'] = jobs_adzuna_selected['publication_datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").time())
jobs_adzuna_selected['country']='US'
jobs_adzuna_selected['industry']='not available'
jobs_adzuna_selected['currency']='dollars'
jobs_adzuna_selected['levels']='not available'

#jobs_adzuna_selected['locations_company'] = jobs_adzuna_selected['locations_company'].astype(str)
#jobs_adzuna_selected['locations_company'] = jobs_adzuna_selected['locations_company'].str.replace(r'[\[\]]', '', regex=True)
jobs_adzuna_selected.head()

print( 'taille jobadzuna avant dedoubloonnage', jobs_adzuna_selected.shape)
#companies['contents'] = companies['description'].apply(clean_text)

jobs_adzuna_selected=jobs_adzuna_selected.drop_duplicates()
print( 'taille jobadzuna apres dedoubloonnage', jobs_adzuna_selected.shape)


# In[7]:


jobs_adzuna_selected.head()


# In[8]:


##jobs_adzuna_selected[jobs_adzuna_selected['job_title']=='Clinical Lab Technologist - Blood Bank']
#duplicates = jobs_adzuna_selected[jobs_adzuna_selected.duplicated()]
#duplicates[duplicates['job_title']=='Clinical Lab Technologist - Blood Bank']


# In[9]:


jobs_themuse.head()


# In[10]:


jobs_theMuse_selected = jobs_themuse[['name','contents','type', 'company.name',
'publication_date', 'locations','categories','refs.landing_page','levels.name']].rename(columns={'name': 'job_title', 'contents': 'job_description',
'type': 'contract_type','company.name': 'company_name','publication_date': 'publication_datetime','locations': 'location',
'categories': 'category','refs.landing_page': 'offer_link','levels.name':'levels'})

jobs_theMuse_selected['publication_date'] = pd.to_datetime(jobs_theMuse_selected['publication_datetime']).dt.strftime("%Y-%m-%d")
jobs_theMuse_selected['publication_time'] = jobs_theMuse_selected['publication_datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").time())

jobs_theMuse_selected['country']='a travailler'
jobs_theMuse_selected['industry']='not available'
jobs_theMuse_selected['currency']='not available'
jobs_theMuse_selected['contract_time']='not available'
jobs_theMuse_selected['salary_max']=0
jobs_theMuse_selected['salary_min']=0

jobs_theMuse_selected['location'] = jobs_theMuse_selected['location'].astype(str)
jobs_theMuse_selected['location'] = jobs_theMuse_selected['location'].str.replace(r'[\[\]]', '', regex=True)
#jobs_theMuse_selected['locations_company']=jobs_theMuse_selected['location']

jobs_theMuse_selected['job_description'] = jobs_theMuse_selected['job_description'].apply(clean_text)

jobs_theMuse_selected["source"] = "TheMuse"

jobs_theMuse_selected.head()


# In[11]:



#df[['currency', 'min_salary', 'max_salary']] = df['salary'].apply(extract_salary_info)
jobs_wttj.head()


# In[12]:


jobs_wttj_selected = jobs_wttj[['title', 'company', 'type_contract', 'contents', 'date_public',
       'employmentType','addressCountry', 'addressLocality',  'industry', 'link_job', 'salary']].rename(
columns={'title': 'job_title', 'contents': 'job_description',
'employmentType': 'contract_type','company': 'company_name','date_public': 'publication_datetime','addressLocality': 'location',
' addressCountry': 'country','link_job': 'offer_link'})



jobs_wttj_selected['publication_date'] = pd.to_datetime(jobs_wttj_selected['publication_datetime']).dt.strftime("%Y-%m-%d")
jobs_wttj_selected['publication_time'] = jobs_wttj_selected['publication_datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").time())
jobs_wttj_selected['industry']='not available'
jobs_wttj_selected['contract_time']='not available'
jobs_wttj_selected['category']='not available'

jobs_wttj_selected[['currency', 'salary_min', 'salary_max']] = jobs_wttj_selected['salary'].apply(extract_salary_info)
jobs_wttj_selected = jobs_wttj_selected.drop('salary', axis=1)

jobs_wttj_selected['location'] = jobs_wttj_selected['location'].astype(str)
jobs_wttj_selected['location'] = jobs_wttj_selected['location'].str.replace(r'[\[\]]', '', regex=True)
#jobs_wttj_selected['locations_company']=jobs_wttj_selected['location']

jobs_wttj_selected['job_description'] = jobs_wttj_selected['job_description'].apply(clean_text)
jobs_wttj_selected["levels"] = "not available"

jobs_wttj_selected["source"] = "Wttj"
jobs_wttj_selected=jobs_wttj_selected.drop_duplicates()


jobs_wttj_selected.head()


# In[13]:


#jobs_linkedin.link.unique()
jobs_linkedin.head()


# In[14]:


jobs_linkedin_selected = jobs_linkedin[['title','employment_type', 'company','salary_min','salary_max',
'posted_time','location','job_industry','link','source','seniority_level','job_function']].rename(columns={'title': 'job_title', 'employment_type': 'contract_time',
'company': 'company_name','posted_time': 'publication_datetime','job_industry': 'industry','link': 'offer_link','seniority_level':'levels','job_function':'category'})

#jobs_linkedin_selected['publication_date'] = pd.to_datetime(jobs_adzuna_selected['publication_datetime']).dt.strftime("%Y-%m-%d")
jobs_linkedin_selected['contents']='not available'
jobs_linkedin_selected['currency']='a travailler'
jobs_linkedin_selected['publication_time'] =None

jobs_linkedin_selected['publication_date'] = pd.to_datetime(jobs_linkedin_selected['publication_datetime']).dt.strftime("%Y-%m-%d")

jobs_linkedin_selected['contract_type']='not available'

#jobs_linkedin_selected['locations_company']='not available'

#jobs_linkedin_selected['job_description'] = jobs_linkedin_selected['job_description'].apply(clean_text)
jobs_linkedin_selected.head()


# In[15]:


jobs1 = pd.concat([jobs_linkedin_selected, jobs_theMuse_selected], axis=0)
jobs2 = pd.concat([jobs1, jobs_adzuna_selected], axis=0)
jobs_final = pd.concat([jobs2, jobs_wttj_selected], axis=0)
jobs_final=jobs_final.drop_duplicates()
#jobs = pd.concat([jobs2, jobs_wttj_selected], axis=0)

shape=jobs_final.shape
print(f"Taille du dataframe fusionné : {shape}")
#Taille du dataframe fusionné : (16269, 21)


# In[16]:


jobs_final.head()
jobs_final['contents'] = jobs_final['contents'].progress_apply(translate_to_english)


# In[17]:


jobs_final.to_pickle('../outputs/final/jobs.pkl')


# In[19]:





# In[20]:


companies_linkedin.head()


# In[21]:





# In[ ]:



#jupyter nbconvert --to script process_jobs.ipynb

