#!/usr/bin/env python
# coding: utf-8
#Process data from https://www.numbeo.com/ to hve informations about cities


from geopy.geocoders import Nominatim
import time 
import pandas as pd
import pickle
import os
os.chdir('/Users/sodagayethiam/Documents/Formations/Parcours_data_engineer/projet_jobmarket/JobMarket/processing')
from tqdm import tqdm
tqdm.pandas() 

#Read informations about life 
cost_living = pd.read_csv('../input/cost_living_index.csv', sep=';')
crime = pd.read_csv('../input/crime_index.csv', sep=';')
healthcare = pd.read_csv('../input/healthcare_index.csv', sep=';')
pollution = pd.read_csv('../input/pollution_index.csv', sep=';')
property = pd.read_csv('../input/property_index.csv', sep=';')
quality_life = pd.read_csv('../input/town_data.csv', sep=';')
traffic = pd.read_csv('../input/traffic_index.csv', sep=';')


''' 
cost_living = cost_living.add_prefix("cost_")
crime = crime.add_prefix("crime_")
healthcare = healthcare.add_prefix("health_")
pollution = pollution.add_prefix("pollution_")
property = property.add_prefix("property_")
quality_life = quality_life.add_prefix("quality_")
traffic = traffic.add_prefix("traffic_")
''' 

town_data  = cost_living.merge(crime, on="City", suffixes=("_cost", "_crime"), how='outer') \
                               .merge(healthcare, on="City", suffixes=("", "_health"), how='outer') \
                               .merge(pollution, on="City", suffixes=("", "_pollution"), how='outer') \
                               .merge(property, on="City", suffixes=("", "_property"), how='outer') \
                               .merge(quality_life, on="City", suffixes=("", "_quality"), how='outer') \
                               .merge(traffic, on="City", suffixes=("", "_traffic"), how='outer') 


print(town_data.head())
town_data.columns


#select final columns
town_data=town_data[['City', 'Quality of Life Index','Purchasing Power Index','Safety Index','Health Care Index','Cost of Living Index','Rent Index','Groceries Index','Crime Index',
'Climate Index', 'Pollution Index', 'Traffic Index','Affordability Index']]



def city_geopy(city_town):
    ''' Get city, country_code and country from geopy'''
    try:
        geolocator = Nominatim(user_agent="my_geolocation_app_v1.0", timeout=5)
        time.sleep(2) 
        
        location_obj = geolocator.geocode(city_town, language='en')
        if not location_obj:
            return pd.Series(['Not found'] * 3)
        latitude = str(location_obj.latitude)
        longitude = str(location_obj.longitude)
        # Reverse géocodage pour obtenir les détails de l'adresse
        location_reverse = geolocator.reverse(f"{latitude},{longitude}")
        address = location_reverse.raw.get('address', {})
        country = address.get('country', 'Unknown')
        city = address.get('city', address.get('town', 'Unknown'))
        country_code=address.get('country_code', '')
        result = [country, city,country_code ]
        return pd.Series(result)
    except Exception as e:
            print(f"Error for location '{city_town}': {e}")
            return pd.Series(['Not found'] * 3)

#create new columns to save informations from geopy
town_data [['country', 'city','country_code' ]]= town_data['City'].progress_apply(city_geopy)

town_data.columns = town_data.columns.str.replace(' ', '_', regex=False)

#save final data of towns 
town_data.to_pickle('../outputs/intermediate/town_data.pkl')
