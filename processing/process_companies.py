#export PYTHONPATH=$(pwd)
print ('Step 1/8: import packages')
#import packages
import os
os.chdir('/Users/sodagayethiam/Documents/Formations/Parcours_data_engineer/projet_jobmarket/JobMarket/processing')
import sys
sys.path.append('../')
import pickle
import pandas as pd
from tqdm import tqdm
tqdm.pandas()  # Initialiser tqdm pour pandas
import glob #pour la gestion des motifs dans les fichiers pickle
from common.utils import translate_to_english
from common.utils import clean_text
from common.utils import get_infos_location, load_location_cache, save_location_cache
from common.utils import max_length_string

#affichage des champs 
pd.set_option('display.max_colwidth', None)
raw_path = '../outputs/raw/'
cache_path = '../outputs/intermediate/location_cache.pkl'


print ('Step 2/8: read datas')


#companies Dataframe from welcome to the jungle
#companies_wttj = pd.read_pickle('../outputs/raw/companies_wttj_aout.pkl')
files_wttj = glob.glob(f'{raw_path}*companies_wttj*.pkl')
companies_wttj = pd.concat([pd.read_pickle(file) for file in files_wttj], axis=0)

#companies Dataframe from the muse
files_themuse = glob.glob(f'{raw_path}*companies_muse*.pkl')
companies_themuse = pd.concat([pd.read_pickle(file) for file in files_themuse], axis=0)
#companies_themuse = pd.read_pickle('../outputs/raw/companies_muse_sept.pkl')

#companies Dataframe from Linkedin
files_linkedin = glob.glob(f'{raw_path}*companies_linkedin*.pkl')
companies_linkedin = pd.concat([pd.read_pickle(file) for file in files_linkedin], axis=0)


print ('Step 3/8: Data cleaning')
#Data cleaning
#tCorrect_name from companies not avalaible during the webscrapping
companies_wttj['name_company2']=companies_wttj['url_company'].str.extract(r'companies/([a-zA-Z0-9-]+)')
companies_wttj.loc[companies_wttj['name_company'].isnull(), 'name_company'] =companies_wttj['name_company2']
companies_wttj.drop('name_company2', axis=1, inplace=True)

#Specify the source
companies_wttj['source']='wttj'
companies_themuse['source']='TheMuse'

companies_wttj['contents'] = companies_wttj['contents'].astype(str)
companies_wttj['contents'] = companies_wttj['contents'].str.replace(r'[\[\]]', '', regex=True)

companies_themuse['locations'] = companies_themuse['locations'].apply(' '.join)
companies_themuse['industries'] = companies_themuse['industries'].apply(' '.join)


print ('Step 4/8: Select specific columns')

#Select specific columns 
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


linkedin_selected = companies_linkedin[['company_name','company_description','company_website','company_industries','company_links',
'company_size', 'company_location','company_creation_date' ,'source']].rename(columns={'company_name': 'name_company', 'company_description': 'description',
'company_size': 'size','company_industries': 'industries','company_creation_date': 'creation_date','company_location':'locations','company_links':'linkedin_link'})

linkedin_selected['twitter_link']='not available'
linkedin_selected['facebook_link']='not available'
linkedin_selected['instagram_link']='not available'
linkedin_selected['nb_employee']=None
linkedin_selected['parity_women']='not available'
linkedin_selected['parity_men']='not available'
linkedin_selected['avg_age_employees']='not available'
linkedin_selected['ca']='not available'


print ('Step 5/8: merge companies from different sources ')

#Concatenate the companies from different sources 
companies = pd.concat([themuse_selected, wttj_selected], axis=0)
companies = pd.concat([companies, linkedin_selected], axis=0)
companies=companies.drop_duplicates()

shape=companies.shape
print(f"Shape  of merged companies : {shape}")

companies['twitter_link'] = companies['twitter_link'].fillna('not available').astype(str)
companies['company_website'] = companies['company_website'].replace('Visit website', 'not available').astype(str)
companies['publication_date'] = pd.to_datetime(companies['publication_date'], errors='coerce')
#companies['publication_date'] = companies['publication_date'].fillna(pd.Timestamp('1990-01-01'))
companies['logo'] = companies['logo'].fillna('not available').astype(str)
companies['creation_date'] = companies['creation_date'].fillna('not available').astype(str)
companies['url_jobs'] = companies['url_jobs'].fillna('not available').astype(str)
companies['nb_employee'] = companies['nb_employee'].fillna('not available').astype(str)


print ('Step 6/8: Translate')

#Translate from french 
companies['contents'] = companies['description'].apply(clean_text)
companies['contents'] = companies['contents'].progress_apply(translate_to_english)


print ('Step 7/8: Get informatins from locations' )

#Get location informations
load_location_cache(cache_path)
companies [['country', 'state', 'city', 'postcode', 'latitude', 'longitude', 'currency_name','country_code','codeIso_lvl4' ]]= companies['locations'].progress_apply(get_infos_location)


print ('Step 8/8: Manage duplicated companies')


companies = companies.drop('description', axis=1)
companies=companies.drop_duplicates()

print (companies.shape)

currency_dict = { 'fr': 'Euro', 'it': 'Euro', 'es': 'Euro', 'de': 'Euro','fi': 'Euro','br': 'BRL',  # Réal brésilien
                  'ie': 'Euro', 'pt': 'Euro', 'nl': 'Euro'}
companies = companies.reset_index(drop=True)
companies.loc[companies['currency_name'] == 'Unknown', 'currency_name'] = companies.apply( lambda row: currency_dict.get(row['country_code'], 'Unknown'), axis=1)

companies.loc[:, 'clef_doublons'] = companies['name_company'].astype(str) + ' ' + companies['industries'].astype(str) + ' ' + companies['locations'].astype(str)


def max_length_string(series):
    series = series.dropna()
    if series.empty:
        return 'Not Available' 
    return series.loc[series.str.len().idxmax()] # Find which element has the nmax numbers of character


aggregation_rules = {col: 'first' for col in companies.columns}
aggregation_rules.update({
    'twitter_link': max_length_string,  
    'company_website': max_length_string,  
    'logo': max_length_string,  
    'linkedin_link': max_length_string,  
    'facebook_link': max_length_string,  
    'instagram_link': max_length_string,  
    #'locations': lambda x: ', '.join(x.dropna().unique()),  # Concatène les valeurs uniques
    'publication_date': 'min',    
    'creation_date': 'min',                               # Prend la date la plus ancienne
    #'nb_employee': 'max',                                 # Prend le nombre d'employés maximum
})



companies = companies.groupby('clef_doublons').agg(aggregation_rules).reset_index(drop=True)
companies = companies.drop('clef_doublons', axis=1)

companies.shape

print ('Save the companies on /outputs/final/companies.pk')
#save the result
companies.to_pickle('../outputs/final/companies.pkl')

#companies_wttj['creation_date'] = companies_wttj['creation_date'].astype(int)
#companies_wttj['nb_employee'] = companies_wttj['nb_employee'].astype(int)

