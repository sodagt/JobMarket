"""
Toutes les fonctions utilisées dans le projet sont définies dans ce fichier (read, write, get,..)
"""

import sys
sys.path.append('../')

from pyspark.sql import DataFrame


from configs.conf import es
from elasticsearch.helpers import bulk




#Write DF in Elasticsearch
def generate_data_to_elk(df: DataFrame):
    for index, row in df.iterrows():
        yield {
            "_index": "bigdata-adzuna", 
            #"_id": row["id"],  #specify unique value as key
            "_source": row.to_dict(),
        }


def insert_data_elk(df: DataFrame):
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
