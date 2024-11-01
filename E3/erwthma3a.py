"""The user has to give the query phrase,
   the maximum number of results they
   want to be printed and a user ID
   as arguments when running the script
   i.e. python3 erwthma3a.py 'hello world' 10 8"""

from cmath import nan
import pandas as pd
import numpy as np
import sys # command line arguments and exit function
from elasticsearch import Elasticsearch


from keras.models import Sequential
from keras.layers import Dense
from keras.callbacks import EarlyStopping

def myScore(esScore, maxscore, isbn, user, embedding): 

    # The final score that the function will return
    # The score is initialized as the ElasticSearch's score
    # as this will be returned if the book has no ratings
    finalScore = esScore

    # Variables indicating if our user or other users has rated that specific book
    userRated = False
    URate = 0
    publicRated = False
    PSumRate = 0
    countRated = 0

    # Checking inside the books ratings index if there is a rating
    # for the book with this specific isbn
    check = es.search(index="ratings", query={"match": {"isbn": isbn}}, size=10000)

    # iterate through ratings
    for rt in check["hits"]["hits"]:
        if rt["_source"]["rating"] != 0: # if the book has been read and there is a rating
            if rt["_source"]["uid"] == int(user): # if there is a rating from our user
                userRated = True
                URate = rt["_source"]["rating"]
            else: #if the rating is from another user
                publicRated = True
                countRated += 1
                PSumRate += rt["_source"]["rating"]
        else: # if the book has been read and there is NOT a rating
            if rt["_source"]["uid"] == int(user): # if is OUR user
                userRated,URate = makePredict(rt["_source"]["uid"], embedding)
            else: #if is ANOTHER user
                temp = makePredict(rt["_source"]["uid"], embedding)
                publicRated = temp[0]
                if temp[0] == True:
                    countRated += 1
                PSumRate += temp[1]

    if not userRated and not publicRated:
        finalScore = esScore
    elif not userRated and publicRated:
        finalScore = 0.5 * esScore + 0.5 * (((PSumRate / countRated) * maxscore) / 10)
    elif userRated and not publicRated:
        finalScore = 0.5 * esScore + 0.5 * (URate * maxscore) / 10
    elif userRated and publicRated:
        finalScore= 0.5 * esScore + 0.25 * (((URate + (PSumRate / countRated)) * maxscore) / 10)
    
    # Bonus system so that the ratings does not always pull down the score
    # Adds if the score is grater or equal 5 and subtracts otherwise
    if URate != 0:
        if URate < 5:
            finalScore -= ((5 % URate) * maxscore) / 10
        else:
            finalScore += ((URate % 5) * maxscore) / 10
    
    if PSumRate != 0:
        if (PSumRate / countRated) < 5:
            finalScore -= ((5 % (PSumRate / countRated)) * maxscore) / 10
        else:
            finalScore += (((PSumRate / countRated) % 5) * maxscore) / 10
    #################################################################################
    
    return finalScore


def makePredict(user, embedding):
    # Searches for all the books that user has read
    rat = es.search(index="ratings", query={"match": {"uid":user}}, size=10000)
    # Creates an empty Data Frame
    rat_df = pd.DataFrame()

    # Initialize prediction result
    prediction = (False, 0)

    # Appends one by one the retrieved results to a Pandas DataFrame
    for i in rat["hits"]["hits"]:
        if i["_source"]["rating"] != 0: # If the book has been rated from the user

            # Search for this book in books index
            bk = es.search(index="books", query={"match": {"isbn":i["_source"]["isbn"]}})
            if bk["hits"]["total"]["value"] == 1: # If book exists in the books index (because some of the books does not exist)
                # We add the book embedding vector to the Dataframe after normalizing it
                emb = bk["hits"]["hits"][0]["_source"]["embedding"] # Stores word embedings for the summary
                i["_source"]["embedding"] = emb/np.linalg.norm(emb) # Normalizes it and add it to the dict
                # Appends record to the DataFrame
                rat_df = rat_df.append(i["_source"], ignore_index=True)
    

    if rat_df.shape[0] != 0: # If user has rated at least one book from those they have read
        # Numpy arrays for storing the training datasets
        X = np.empty((rat_df.shape[0], 300))
        Y = np.zeros((rat_df.shape[0], 10))
        # Numpy array for storing the datasets we want to predict
        predict = np.expand_dims(embedding, axis=0) # Reshapes it to have 2 dimensions

        for i in range(len(rat_df)):
            X[i,:] = rat_df.iloc[i,3]
            # Puts a 1 to the vectors place that coresponds to users rating (if rating 5 then vector [0,0,0,0,1,0,0,0,0,0])
            Y[i, int(rat_df.iloc[i,2]) - 1 ] = 1. 

        # Neural network train
        stop = EarlyStopping(monitor="loss", mode="min", verbose=1, patience=10)

        model = Sequential()
        model.add(Dense(300, input_dim=300, activation='relu'))
        model.add(Dense(1000, activation='relu'))
        model.add(Dense(10, activation='softmax'))

        model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])

        model.fit(X, Y, epochs=1000, batch_size=1, callbacks=[stop])

        # Predicting ratings for the book (softmax vector)
        softmaxPredict = model.predict(predict)

        # Now is the actual rating (integer)
        prediction = (True, np.argmax(softmaxPredict, 1)[0] + 1 )

    return prediction

pd.set_option('display.max_colwidth', 150) # To control columns truncation when printing
pd.set_option('display.max_rows', None) # To eliminate rows truncation when printing
#pd.set_option('display.expand_frame_repr', True)

# The address and the port of the Elasticsearch server
server_address = "http://192.168.2.20:9200" # Change this address according to your settings (http://localhost:9200)

# Creates an empty Data Frame
ret_df = pd.DataFrame() 

# Variable for storing the last returned ID
last = 0

# Create an Elasticsearch object
es = Elasticsearch(server_address) # Change this address according to your settings (http://localhost:9200)

while True:
    ret = es.search(index="books", q=sys.argv[1], search_after=[last], sort=[{"_score": "asc"}] ,size=10000) # The query is given by the user as argument during the script execution

    last = ret["hits"]["hits"][-1]["_score"] # Store the last returned ID

    # Appends one by one the retrieved results to an Pandas DataFrame
    for i in ret["hits"]["hits"]:
        i["_source"]["score"] = myScore(i["_score"], ret["hits"]["hits"][-1]["_score"], i["_source"]["isbn"], sys.argv[3], i["_source"]["embedding"])
        ret_df = ret_df.append(i["_source"], ignore_index=True)

    #Check if there is no reason to search for more pages
    if len(ret["hits"]["hits"]) < 10000:
        break

# Setting the number of rows we want to print if there are documents retrieved
# This way we can prevent geting out of dataframe's rows' bounds

if int(sys.argv[2]) <= 0: #if user give a non positive number
    print("\nGIVE A POSITIVE NUMBER OF MAX NUM OF BOOKS YOU WANT TO PRINT")
    sys.exit()
else :
    if  len(ret_df) == 0:# If there are no retrieved results
        print("\nNO RESULTS\n")
        sys.exit()
    else :
        if len(ret_df) < int(sys.argv[2]): # If user wants more than the retrieved results
            row = len(ret_df)
        else: # If user wants less than or equal to the number of the retrieved results
            row = int(sys.argv[2])


# Sorts DataFrame values by score in Descending order and copy it to new DataFrame
final_df = ret_df.sort_values(by=["score"], ascending=False).copy()

# Resets the Dataframe indexes because are mixed up after sorting
final_df.reset_index(drop=True, inplace=True)

# Informs user about the retrieved documents and the displayed ones
print("\nViewing " + str(row) + " out of " + str(ret["hits"]["total"]["value"]) + " retrieved results\n") 

# Prints some specified columns from the created DataFrame
print(final_df.loc[0:row-1,["score","book_title","isbn"]])
