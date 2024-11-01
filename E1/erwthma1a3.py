#Script for indexing ratings

import pandas as pd
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

#The address and the port of the Elasticsearch server
server_address = "http://localhost:9200" #"http://192.168.2.20:9200" #change this address according to your settings (http://localhost:9200)

#Create an Elasticsearch object
es = Elasticsearch(server_address) #change this address according to your settings (http://localhost:9200)

#Create index in Elasticsearch
#Ignore value 400 which is caused by IndexAlreadyExistsException when creating an index
es.indices.create(index="ratings", ignore=400) 

#Load ratings csv file and create a dataframe
df_ratings =  pd.read_csv("../BX-Book-Ratings.csv")

#converts ratings dataframe to a list of json objects in records orientation (orient="records")
#this means that each element of the list will be a json object that represents a dataframe record
#after this line of code we will have only a string (not an actual list of json objects)
json_ratings_str = df_ratings.to_json(orient="records")

#converts ratings json string actual to actual list of json objects so it can be manipulated
json_ratings = json.loads(json_ratings_str)


#We will use this actions list to index the list of rating documents en masse using BULK API helper
#This way we are going to save time rather than indexing each element individualy using a for loop
actions =[
    {
        "_index" : "ratings",
        "_id" : index,
        "_source": ratings_list_item
    }

    for index , ratings_list_item  in enumerate(json_ratings)
]

#Load data to Elasticsearch using BULK API helper
helpers.bulk(es, actions)