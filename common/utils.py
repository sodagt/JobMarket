"""
Toutes les fonctions utilisées dans le projet sont définies dans ce fichier (read, write, get,..)
"""
import pandas as pd

import sys
sys.path.append('../')

#from pyspark.sql import DataFrame


from configs.conf import es
from elasticsearch.helpers import bulk

import re
from langdetect import detect
from deep_translator import GoogleTranslator


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
def translate_french_to_english(text):
    if pd.notna(text) and isinstance(text, str) and len(text.strip()) > 1:  # Vérifiez si le texte est significatif
        try:
            lang = detect(text)  
            if lang == 'fr':
                translator = GoogleTranslator(source='fr', target='en')
                translation = translator.translate(text)
                return translation
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
