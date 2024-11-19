"""
Toutes les fonctions utilisées dans le projet sont définies dans ce fichier (read, write, get,..)
"""
import pandas as pd
import time
import sys
sys.path.append('../')
import time
#from pyspark.sql import DataFrame
import pickle

from configs.conf import es
from elasticsearch.helpers import bulk

import re
from langdetect import detect
from deep_translator import GoogleTranslator

from geopy.geocoders import Nominatim
import pycountry

#Write DF in Elasticsearch
def generate_data_to_elk(df: pd.DataFrame):
    for index, row in df.iterrows():
        yield {
            "_index": "bigdata-adzuna", 
            #"_id": row["id"],  #specify unique value as key
            "_source": row.to_dict(),
        }


def insert_data_elk(df: pd.DataFrame):
    #Insert DataFrame in Elasticsearch
    try:
        # Convert DataFrame and insert in Elasticsearch
        success, failed = bulk(es, generate_data_to_elk(df))
        print(f"Successfully indexed {success} documents")

    except Exception as e:
        print(f"Error: {e}")

#Fonction permettanr d'etraire une information à partir d'un champ contenant une liste
def extract_values(column_to_extract, value):
    values_to_extract = [item[value] for item in column_to_extract]
    return values_to_extract

#Fonction qui traduit un texte du francaias à l'angalis 
def translate_to_english(text):
    if pd.notna(text) and isinstance(text, str) and len(text.strip()) > 1:  # Vérifiez si le texte est significatif
        try:
            lang = detect(text)  
            if lang == 'fr':
                translator = GoogleTranslator(source='fr', target='en')
                translation = translator.translate(text)
                return translation
            elif lang == 'de':  # Allemand
                translator = GoogleTranslator(source='de', target='en')
                translation = translator.translate(text)
                return translation.text
            else:
                return text  
        except Exception as e:
            print(f"Erreur de traduction ou de détection : {e}")
            return text  
    return text


#Fonction pour nettoyer un texte
    
def clean_text(text):
    if pd.notna(text) and isinstance(text, str):
        # Remplacer les caractères spéciaux Unicode par des espaces 
        text = text.replace('\u202f', ' ')
        #retours à la ligne
        text = text.replace('\n', ' ')
        # Supprimer les espaces multiples
        text = re.sub(r'\s+', ' ', text)
        return text
    return ""


def extract_salary_info(salary_text):
    # Recherche de la devise
    currency_match = re.search(r'Salary:([^\d\s:]+)', salary_text)
    currency = currency_match.group(1) if currency_match else None
    salary_range = re.findall(r'[\d.,]+K?', salary_text)
    
    # Convertir les valeurs en nombres en remplacant les K et s'il y a month multiplier par 12
    min_salary = max_salary = 0
    if len(salary_range) > 0:
        min_salary = float(salary_range[0].replace('K', '').replace(',', '')) 
        if 'K' in salary_range[0]:
            min_salary=min_salary*1000 
        if 'month' in salary_range[0]:
            min_salary=min_salary*12 
    if len(salary_range) > 1:
        max_salary = float(salary_range[1].replace('K', '').replace(',', ''))
        if 'K' in salary_range[1]:
            max_salary=max_salary*1000 
        if 'month' in salary_range[1]:
            max_salary=max_salary*12 
    return pd.Series([currency, min_salary, max_salary])


#fonction qui récupère le pays, la monnaie, la ville, le code postal et les coordonnées géographiques  
#location_cache = {} #initialisation cache location
# Charger le cache
def load_location_cache(cache_path):
    global location_cache
    try:
        with open(cache_path, 'rb') as f:
            location_cache = pickle.load(f)
            print("Cache chargé.")
    except FileNotFoundError:
        #location_cache = {}
        print("Aucun cache trouvé. Création d'un nouveau.")


#cache_path = '../outputs/intermediate/location_cache.pkl'

# Sauvegarder le cache
def save_location_cache(cache_path):
    global location_cache
    with open(cache_path, 'wb') as f:
        pickle.dump(location_cache, f)
        print ('cache sauvegardé')


def get_infos_location(location_name):
    # Vérifier si l'élément est déjà dans le cache
    if location_name in location_cache:
        return pd.Series(location_cache[location_name])
    
    try:
        geolocator = Nominatim(user_agent="my_geolocation_app_v1.0", timeout=5)  # Initialisation
        time.sleep(2)  # Délai entre les requêtes pour éviter de surcharger l'API
        
        # Géocodage de la localisation
        location_obj = geolocator.geocode(location_name)
        if not location_obj:
            return pd.Series(['Not found'] * 7)
        
        # Extraction des coordonnées
        latitude = str(location_obj.latitude)
        longitude = str(location_obj.longitude)
        
        # Reverse géocodage pour obtenir les détails de l'adresse
        location_reverse = geolocator.reverse(f"{latitude},{longitude}")
        address = location_reverse.raw.get('address', {})
        
        # Extraction des informations d'adresse
        country = address.get('country', 'Unknown')
        city = address.get('city', address.get('town', 'Unknown'))
        state = address.get('state', 'Unknown')
        postcode = address.get('postcode', 'Unknown')
        
        # Obtention des informations sur la devise
        country_info = pycountry.countries.get(name=country)
        if country_info:
            currency = pycountry.currencies.get(numeric=country_info.numeric)
            currency_name = currency.name if currency else 'Unknown'
        else:
            currency_name = 'Unknown'
        
        # Création du résultat à renvoyer sous forme de liste
        result = [country, state, city, postcode, latitude, longitude, currency_name]
        
        # Mise à jour du cache avec les informations
        location_cache[location_name] = result
        
        return pd.Series(result)
    
    except Exception as e:
        # Gestion des erreurs générales
        print(f"Error for location '{location_name}': {e}")
        location_cache[location_name] = ['Not found'] * 7  # Mise à jour du cache avec un résultat d'erreur
        return pd.Series(['Not found'] * 7)