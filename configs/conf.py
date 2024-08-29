
from elasticsearch import Elasticsearch


#Create es var

#es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    http_auth=('elastic', 'datascientest'), 
    timeout=60,
    verify_certs=True,  # Check SSL certificats
    ca_certs='./ca/ca.crt'  # Specify CA certificat path

)

#print(es)




"""
from pymongo import MongoClient

client = MongoClient(
    host = "127.0.0.1",
    port = 27017,
    username = "datascientest",
    password = "dst123"
)

print(client.list_database_names())

"""


