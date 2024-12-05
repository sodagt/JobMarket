#!/usr/bin/env python
# coding: utf-8


#pip install scikit-learn
#pip install faiss-cpu

import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.feature_extraction.text import TfidfVectorizer
import faiss
#https://github.com/facebookresearch/faiss/blob/main/README.md
import numpy as np
import pickle

distance_path = '../outputs/final/distances_reco.pkl'
reco_path = '../outputs/final/indices_reco.pkl'

jobs_final=pd.read_pickle('/Users/sodagayethiam/Documents/Formations/Parcours_data_engineer/projet_jobmarket/JobMarket/outputs/final/jobs.pkl')




#encode categorical columns
ohe = OneHotEncoder( drop="first", sparse_output=False)
cat = [ 'industry', 'category', 'levels']

jobs_encoded = ohe.fit_transform(jobs_final[cat])

encoded_df = pd.DataFrame(jobs_encoded, columns=ohe.get_feature_names_out(cat))

num_col=['salary_min', 'salary_max']#, 'Quality_of_Life_Index', 'Purchasing_Power_Index', 'Safety_Index', 'Health_Care_Index',
#       'Cost_of_Living_Index', 'Rent_Index', 'Groceries_Index', 'Crime_Index','Climate_Index', 'Pollution_Index', 'Traffic_Index', 'Affordability_Index']

scaler = MinMaxScaler()
jobs_num = scaler.fit_transform(jobs_final[num_col])
jobs_num_df = pd.DataFrame(jobs_num, columns=num_col)


vectorizer = TfidfVectorizer(max_features=100)
job_titles_tfidf = vectorizer.fit_transform(jobs_final['job_title'])
job_desc_tfidf = vectorizer.fit_transform(jobs_final['job_description'])

job_titles_tfidf = pd.DataFrame(job_titles_tfidf.toarray(), 
                                 columns=vectorizer.get_feature_names_out(), 
                                 index=jobs_final.index)
job_desc_tfidf = pd.DataFrame(job_desc_tfidf.toarray(), 
                                 columns=vectorizer.get_feature_names_out(), 
                                 index=jobs_final.index)
jobs_reco = pd.concat([encoded_df, jobs_num_df], axis=1)
jobs_reco = pd.concat([jobs_reco, job_titles_tfidf], axis=1)
jobs_reco = pd.concat([jobs_reco, job_desc_tfidf], axis=1)
jobs_reco.shape #(125214, 1051)

# convert  float32 for Faiss input and with continguous format for the cosinus distance
jobs_reco_np = np.ascontiguousarray(jobs_reco.to_numpy().astype('float32'))

#normalise for cosinus distance 
faiss.normalize_L2(jobs_reco_np)
index = faiss.IndexFlatIP(jobs_reco_np.shape[1])  # IP = Inner Product, équivalent à cosinus avec données normalisées
index.add(jobs_reco_np)  # Ajout des données

# Searchng for k nearest neighborhood for each job  
k=10
distances, indices = index.search(jobs_reco_np, k)


#Apply a weight for life index

def weighted_recommendations(input_job_title,k=10):
    job_index = jobs_final[jobs_final['job_title'].str.contains(input_job_title, case=False)].index[0]
    neighbor_indices = indices[job_index]
    neighbor_distances = distances[job_index]

    recommended_jobs = jobs_final.iloc[neighbor_indices]    
    indices_for_weighting = [
        'Quality_of_Life_Index', 'Purchasing_Power_Index','Safety_Index','Health_Care_Index', 'Cost_of_Living_Index', 'Rent_Index', 
        'Groceries_Index', 'Crime_Index', 'Climate_Index','Pollution_Index','Traffic_Index', 'Affordability_Index']
    
    indices_values = recommended_jobs[indices_for_weighting].values
    
    # Invert the index for Cost_of_Living_Index , Crime_Index,Pollution_Index,Traffic_Index,Rent_Index,
    indices_values[:,4 ] = 1 / (indices_values[:,4 ] + 0.1)  # Cost_of_Living_Index
    indices_values[:,5 ] = 1 / (indices_values[:,5 ] + 0.1)  #  Rent_Index
    indices_values[:,7 ] = 1 / (indices_values[:,7 ] + 0.1)  #  Crime_Index
    indices_values[:,9 ] = 1 / (indices_values[:,9 ] + 0.1)  #  Pollution_Index
    indices_values[:,10 ] = 1 / (indices_values[:,10 ] + 0.1)  #  Traffic_Index

    weights = np.sum(indices_values, axis=1)
    # normalize weights 
    weights = (weights - np.min(weights)) / (np.max(weights) - np.min(weights))
    weighted_distances = neighbor_distances * weights
    weighted_indices = np.argsort(weighted_distances)[:k]
    
    return recommended_jobs.iloc[weighted_indices]






with open(distance_path, 'rb') as f:
    distances = pickle.load(f)

with open(reco_path, 'rb') as f:
    indices = pickle.load(f)






print ('Fin reco')


