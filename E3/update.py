import pandas as pd
import json
from elasticsearch import Elasticsearch
import spacy

#Load books csv file and create a dataframe
df_books =  pd.read_csv("../BX-Books.csv")

#converts books dataframe to a list of json objects in records orientation (orient="records")
#this means that each element of the list will be a json object that represents a dataframe record
#after this line of code we will have only a string (not an actual list of json objects)
json_books_str = df_books.to_json(orient="records")

#converts books json string actual to actual list of json objects so it can be manipulated
json_books = json.loads(json_books_str)

# Load the spacy model that you have installed
# This will be used for word embeddings
nlp = spacy.load('en_core_web_lg')

for book in json_books:
    summary = book["summary"]
    emb = nlp(summary) # Creates word embedings for the summary
    book["embedding"] = emb.vector
    print(book)