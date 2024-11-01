"""The user has to give the query phrase
   and the maximum number of results they
   want to be printed as arguments when
   running the script
   i.e. python3 erwthma1b.py 'hello world' 10 """

import pandas as pd
import sys # command line arguments and exit function
from elasticsearch import Elasticsearch

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
    ret = es.search(index="books", q=sys.argv[1], _source_excludes=["embedding"], search_after=[last], sort=[{"_score": "asc"}], size=10000) # The query is given by the user as argument during the script execution
    
    last = ret["hits"]["hits"][-1]["_score"] # Store the last returned ID

    # Appends one by one the retrieved results to an Pandas DataFrame
    for i in ret["hits"]["hits"]:
        i["_source"]["score"] = i["_score"]
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
    if  len(ret_df) == 0:#ret["hits"]["total"]["value"] == 0: # If there are no retrieved results
        print("\nNO RESULTS\n")
        sys.exit()
    else :
        if len(ret_df) < int(sys.argv[2]):#ret["hits"]["total"]["value"] < int(sys.argv[2]): # If user wants more than the retrieved results
            row = len(ret_df)#ret["hits"]["total"]["value"] 
        else: # If user wants less than or equal to the number of the retrieved results
            row = int(sys.argv[2])

# Informs user about the retrieved documents and the displayed ones
print("\nViewing " + str(row) + " out of " + str(len(ret_df)) + " retrieved results\n")   

# Sorts DataFrame values by score in Descending order and copy it to new DataFrame
final_df = ret_df.sort_values(by=["score"], ascending=False).copy()

# Resets the Dataframe indexes because they may have mixed up after sorting
final_df.reset_index(drop=True, inplace=True)

# Prints some specified columns from the created DataFrame
print(final_df.loc[0:row-1,["score","book_title","isbn"]])
