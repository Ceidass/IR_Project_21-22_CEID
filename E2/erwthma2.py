"""The user has to give the query phrase,
   the maximum number of results they
   want to be printed and a user ID
   as arguments when running the script
   i.e. python3 erwthma2.py 'hello world' 10 152"""

import pandas as pd
import sys # command line arguments and exit function
from elasticsearch import Elasticsearch

def myScore(esScore, maxscore, isbn, user): 

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
        if rt["_source"]["rating"] != 0:
            if rt["_source"]["uid"] == int(user): # if there is a rating from our user
                userRated = True
                URate = rt["_source"]["rating"]
            else: #if the rating is from another user
                publicRated = True
                countRated += 1
                PSumRate += rt["_source"]["rating"]
    
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
        

pd.set_option('display.max_colwidth', 150) # To control columns truncation when printing
pd.set_option('display.max_rows', None) # To eliminate rows truncation when printing
#pd.set_option('display.expand_frame_repr', True)

# The address and the port of the Elasticsearch server
server_address = "http://192.168.2.20:9200" # Change this address according to your settings (http://localhost:9200)

# Create an Elasticsearch object
es = Elasticsearch(server_address) # Change this address according to your settings (http://localhost:9200)

# Creates an empty Data Frame
ret_df = pd.DataFrame()

# Variable for storing the last returned ID
last = 0


while True:
    ret = es.search(index="books", q=sys.argv[1], _source_excludes=["embedding"], search_after=[last], sort=[{"_score": "asc"}], size=10000) #fix# The query is given by the user as argument during the script execution
    
    last = ret["hits"]["hits"][-1]["_score"] # Store the last returned ID

    # Appends one by one the retrieved results to an Pandas DataFrame
    for i in ret["hits"]["hits"]:
        i["_source"]["score"] = myScore(i["_score"], ret["hits"]["hits"][-1]["_score"], i["_source"]["isbn"], sys.argv[3])
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

# Informs user about the retrieved documents and the displayed ones
print("\nViewing " + str(row) + " out of " + str(len(ret_df)) + " retrieved results\n")   

# Sorts DataFrame values by score in Descending order and copy it to new DataFrame
final_df = ret_df.sort_values(by=["score"], ascending=False).copy()

# Resets the Dataframe indexes because are mixed up after sorting
final_df.reset_index(drop=True, inplace=True)

# Prints some specified columns from the created DataFrame
print(final_df.loc[0:row-1,["score","book_title","isbn"]])
