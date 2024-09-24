
#export PYTHONPATH=$(pwd)

#import packages
import pickle
import pandas as pd
from tqdm import tqdm
tqdm.pandas()  # Initialiser tqdm pour pandas
from common.utils import translate_french_to_english
from common.utils import clean_text
#import re
#from langdetect import detect
#from deep_translator import GoogleTranslator


#affichage des champs 
pd.set_option('display.max_colwidth', None)

#chargement du df des compagnies welcome to the jungle
companies_wttj = pd.read_pickle('../data/raw/companies_wttj_aout.pkl')
companies_themuse = pd.read_pickle('../data/raw/companies_muse_sept.pkl')



#traitement des noms de compagnies manquantes da
companies_wttj['name_company2']=companies_wttj['url_company'].str.extract(r'companies/([a-zA-Z0-9-]+)')
companies_wttj.loc[companies_wttj['name_company'].isnull(), 'name_company'] =companies_wttj['name_company2']
companies_wttj.drop('name_company2', axis=1, inplace=True)

#ajout de la source
companies_wttj['source']='wttj'
companies_themuse['source']='TheMuse'

#Nettoyer les champs des descriptions , locations et industriers
companies_wttj['contents'] = companies_wttj['contents'].astype(str)
# Supprimer les crochets '[ ]'
companies_wttj['contents'] = companies_wttj['contents'].str.replace(r'[\[\]]', '', regex=True)

companies_themuse['locations'] = companies_themuse['locations'].apply(' '.join)
companies_themuse['industries'] = companies_themuse['industries'].apply(' '.join)


#CSelectionned columns
themuse_selected = companies_themuse[['name', 'twitter','description','refs.landing_page','publication_date',
'size.name', 'refs.logo_image','locations','industries', 'refs.jobs_page' ,'source']].rename(columns={'name': 'name_company', 'twitter': 'twitter_link',
'refs.landing_page': 'company_website','size.name': 'size','refs.logo_image': 'logo','refs.jobs_page': 'url_jobs'})

themuse_selected['linkedin_link']='not available'
themuse_selected['facebook_link']='not available'
themuse_selected['instagram_link']='not available'
themuse_selected['creation_date']='not available'
themuse_selected['nb_employee']='not available'
themuse_selected['parity_women']='not available'
themuse_selected['parity_men']='not available'
themuse_selected['avg_age_employees']='not available'
themuse_selected['ca']='not available'


wttj_selected = companies_wttj[['name_company', 'linkedin_link', 'twitter_link_element','facebook_link','instagram_link','contents',
'company_website','creation_date', 'nb_employee','location' ,'domain', 'url_company','parity_women',
'parity_men','avg_age','ca','source']].rename(columns={'twitter_link_element': 'twitter_link', 'contents': 'description',
'location': 'locations','domain': 'industries','url_company': 'url_jobs','avg_age': 'avg_age_employees'})

wttj_selected['publication_date'] = None

#wttj_selected['publication_date']='not available'
wttj_selected['size']='not available'
wttj_selected['logo']='not available'

companies = pd.concat([themuse_selected, wttj_selected], axis=0)
shape=companies.shape
print(f"Taille du dataframe fusionné : {shape}")



#traduction des textes en Francais 
companies['contents'] = companies['description'].apply(clean_text)
companies['contents'] = companies['contents'].progress_apply(translate_french_to_english)

#save the result

companies.to_pickle('./data/final/companies.pkl')

#companies_wttj['creation_date'] = companies_wttj['creation_date'].astype(int)
#companies_wttj['nb_employee'] = companies_wttj['nb_employee'].astype(int)

