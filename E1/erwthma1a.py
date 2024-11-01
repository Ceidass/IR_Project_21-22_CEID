#Script for indexing books

import pandas as pd
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import spacy

#The address and the port of the Elasticsearch server
server_address = "http://localhost:9200" #"http://192.168.2.20:9200" #change this address according to your settings (http://localhost:9200)

#Create an Elasticsearch object
es = Elasticsearch(server_address) #change this address according to your settings (http://localhost:9200)

#Create index in Elasticsearch
#Ignore value 400 which is caused by IndexAlreadyExistsException when creating an index
es.indices.create(index="books", ignore=400) 

#Load books csv file and create a dataframe
df_books =  pd.read_csv("../BX-Books.csv")

#converts books dataframe to a list of json objects in records orientation (orient="records")
#this means that each element of the list will be a json object that represents a dataframe record
#after this line of code we will have only a string (not an actual list of json objects)
json_books_str = df_books.to_json(orient="records")

#converts books json string actual to actual list of json objects so it can be manipulated
json_books = json.loads(json_books_str)

# This is optional but it will help in questions 3 and 4 #########################################################

# Load the spacy model that you have installed
# This will be used for word embeddings
nlp = spacy.load('en_core_web_lg')

i=0

for book in json_books:
    summary = book["summary"]
    emb = nlp(summary) # Creates word embedings for the summary
    book["embedding"] = emb.vector
    book["id"] = i # This will help to iterate through multiple elements (more than 10000)
    i += 1
####################################################################################################################

#We will use this actions list to index the list of book documents en masse using BULK API helper
#This way we are going to save time rather than indexing each element individualy using a for loop
actions =[
    {
        "_index" : "books",
        "_id" : index,
        "_source": books_list_item
    }

    for index , books_list_item  in enumerate(json_books)
]

#Load data to Elasticsearch using BULK API helper
helpers.bulk(es, actions)