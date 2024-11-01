"""The user has to give the user ID
   they want to train a neural network"""
from ast import Return
from re import X
from tabnanny import verbose
import pandas as pd
import numpy as np
import sys # command line arguments and exit function
from elasticsearch import Elasticsearch
import spacy

from keras.models import Sequential
from keras.layers import Dense
from keras.callbacks import EarlyStopping

pd.set_option('display.max_colwidth', 150) # To control columns truncation when printing
pd.set_option('display.max_rows', None) # To eliminate rows truncation when printing
#pd.set_option('display.expand_frame_repr', True)

# Load the spacy model that you have installed
# This will be used for word embeddings
nlp = spacy.load('en_core_web_lg')

# The address and the port of the Elasticsearch server
server_address = "http://192.168.2.20:9200" # Change this address according to your settings (http://localhost:9200)

# Create an Elasticsearch object
es = Elasticsearch(server_address) # Change this address according to your settings (http://localhost:9200)

ret = es.search(index="ratings", query={"match": {"uid":sys.argv[1]}}, size=10000) # The query is given by the user as argument during the script execution

# Creates an empty Data Frame
ret_df = pd.DataFrame()

# Variable for counting how many books the user has rated or not
ratedCount = 0
nonRatedCount = 0

# Appends one by one the retrieved results to a Pandas DataFrame
for i in ret["hits"]["hits"]:

    # Search for this book in books index
    bk = es.search(index="books", query={"match": {"isbn":i["_source"]["isbn"]}})

    if bk["hits"]["total"]["value"] == 1: # If book exists in the books index (because some of the books does not exist)
        summary = bk["hits"]["hits"][0]["_source"]["summary"]
        # print(bk)#debug
        # print(summary) #debug
        # print(" ") #debug
        emb = nlp(summary) # Creates word embedings for the summary
        i["_source"]["embedding"] = emb.vector/np.linalg.norm(emb.vector) # Normalize and add it to the dict
        #print(i["_source"]["embedding"]) #debug

        if i["_source"]["rating"] != 0: # If the book has been rated from the user
            ratedCount += 1
        else:
            nonRatedCount += 1
        ret_df=ret_df.append(i["_source"], ignore_index=True) # Appends record to the DataFrame

# Numpy arrays for storing the training datasets
X = np.empty((ratedCount, 300))
Y = np.zeros((ratedCount, 10))
# Numpy array for storing the datasets we want to predict
predict = np.empty((nonRatedCount, 300))

# Counter for helping with non zero ratings
nonZeroCount = 0
zeroCount = 0
for i in range(len(ret_df)):
    if ret_df.iloc[i,2] != 0: # If there is a rating by the user
        X[nonZeroCount,:] = ret_df.iloc[i,3]
        # Puts a 1 to the vectors place that coresponds to users rating (if rating 5 then vector [0,0,0,0,1,0,0,0,0,0])
        Y[nonZeroCount, int(ret_df.iloc[i,2]) - 1 ] = 1. 
        nonZeroCount += 1
    else: # If there is not a rating by the user
        predict[zeroCount,:] = ret_df.iloc[i,3]
        zeroCount += 1

# Neural network train
stop = EarlyStopping(monitor="loss", mode="min", verbose=1, patience=10)

model = Sequential()
model.add(Dense(300, input_dim=300, activation='relu'))
model.add(Dense(1000, activation='relu'))
model.add(Dense(10, activation='softmax'))

model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])

model.fit(X, Y, epochs=1000, batch_size=1, callbacks=[stop])

# Predicting ratings for non rated books
softmaxPredict = model.predict(predict)

#  Array for storing new rating predictions (integers)
predictions = np.zeros(zeroCount)

# Initialize helper variable to zero
predictCount = 0

# Now is the actual one hot encoded predictions
predictions = np.argmax(softmaxPredict, 1) + 1 

for i in range(len(ret_df)):
    if ret_df.iloc[i,2] == 0:
        ret_df.iloc[i,2] = predictions[predictCount]
        predictCount += 1


#print(softmaxPredict) #debug
#print(predictions) #debug
print(ret_df.iloc[:,0:3])

# serialize model to JSON
model_json = model.to_json()
with open("model.json", "w") as json_file:
    json_file.write(model_json)
# serialize weights to HDF5
model.save_weights("model.h5")
print("Saved model to disk")