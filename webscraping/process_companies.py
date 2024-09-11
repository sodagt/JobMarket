#import packages
import pickle
import pandas as pd

#affichage des champs 
pd.set_option('display.max_colwidth', None)

#chargement du df des compagnies
companies_wttj = pd.read_pickle('data/raw/companies_wttj_aout.pkl')
companies_wttj.describe()

companies_wttj['name_company2']=companies_wttj['url_company'].str.extract(r'companies/([a-zA-Z0-9-]+)')
companies_wttj.loc[companies_wttj['name_company'].isnull(), 'name_company'] =companies_wttj['name_company2']
companies_wttj.drop('name_company2', axis=1, inplace=True)

#ajout de la source
companies_wttj['source']='wttj'

#transformer la liste en string
companies_wttj['contents'] = companies_wttj['contents'].apply(' '.join)


#companies_wttj['creation_date'] = companies_wttj['creation_date'].astype(int)
#companies_wttj['nb_employee'] = companies_wttj['nb_employee'].astype(int)

print(companies_wttj.head(2))
print(companies_wttj.dtypes)