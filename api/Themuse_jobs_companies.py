#!/usr/bin/env python
# coding: utf-8

''' Extraction des infos via l'API TheMuse sur les jobs et les companies disponibles
Version Mai 2024'''

import requests
import pandas as pd


# fonction pour les transformations de colonne
def extract_names(column_to_extract):
    values_to_extract = [item["name"] for item in column_to_extract]
    return values_to_extract

def extract_shortnames(column_to_extract):
    values_to_extract = [item["short_name"] for item in column_to_extract]
    return values_to_extract

def transform_list_to_string(column_to_transform):
    value_to_transform=''.join(column_to_transform)
    return value_to_transform





#tRécupération des infos sur les jobs

url_api_themuse_jobs='https://www.themuse.com/api/public/jobs?'
job_response=requests.get(url_api_themuse_jobs, params={'page': 1})
#récupération de la derniere page disponible
max_page_jobs=job_response.json()["page_count"]
max_page_jobs




#requests.get(url_api_themuse_jobs, params={'page': max_page_jobs})

#initialisation 
df_jobs_themuse = pd.DataFrame()

#Interrogation de toutes les pages en vérifiant qu'i y a une réponse
for num_page in range (1,max_page_jobs+1):
    param = {'page': num_page}
    job_response=requests.get(url_api_themuse_jobs, params=param)

    if job_response.status_code == 200:
        themuse_job_dict=job_response.json()
        results=themuse_job_dict["results"]
        df_new = pd.json_normalize(results)
        df_jobs_themuse = pd.concat([df_jobs_themuse, df_new]).reset_index(drop=True)
    else:
        print (f"status code" ,job_response.status_code, "at page", num_page )
        break
        

df_jobs_themuse.shape
#(1980, 16)



#Transformation des colonnes 
df_jobs_themuse['locations'] = df_jobs_themuse['locations'].apply(extract_names)
df_jobs_themuse['categories'] = df_jobs_themuse['categories'].apply(extract_names)
df_jobs_themuse['levels.name'] = df_jobs_themuse['levels'].apply(extract_names)
df_jobs_themuse['levels.short_name'] = df_jobs_themuse['levels'].apply(extract_shortnames)
df_jobs_themuse = df_jobs_themuse.drop('levels', axis=1)
df_jobs_themuse['categories'] = df_jobs_themuse['categories'].apply(transform_list_to_string)
df_jobs_themuse['levels.short_name'] = df_jobs_themuse['levels.short_name'].apply(transform_list_to_string)
df_jobs_themuse['levels.name'] = df_jobs_themuse['levels.name'].apply(transform_list_to_string)

df_jobs_themuse







# Extraction des infos sur les compagnies
url_api_themuse_companies='https://www.themuse.com/api/public/companies?'
comp_response=requests.get(url_api_themuse_companies, params={'page': 1})
max_page_companies=comp_response.json()["page_count"]

#initialisation 
df_companies = pd.DataFrame()

#extraction des infos des compagnies sur toutes les pages dispo
for num_page in range (1,max_page_companies+1):
    param = {'page': num_page}
    companies_response=requests.get(url_api_themuse_companies, params=param)

    if companies_response.status_code == 200:
        themuse_comp_dict=companies_response.json()
        results=themuse_comp_dict["results"]
        df_new = pd.json_normalize(results)
        df_companies = pd.concat([df_companies, df_new]).reset_index(drop=True)



# Appliquer la fonction sur la colonne
df_companies['locations'] = df_companies['locations'].apply(extract_names)
df_companies['industries'] = df_companies['industries'].apply(extract_names)

df_companies.to_pickle('data/raw/companies_muse_sept.pkl')
df_jobs_themuse.to_pickle('data/raw/jobs_muse_sept.pkl')







