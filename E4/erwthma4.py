from tabnanny import check
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
#importing kMeans
from sklearn.cluster import MiniBatchKMeans
from matplotlib import pyplot as plt
import time #debug


pd.set_option('display.max_colwidth', 150) # To control columns truncation when printing
pd.set_option('display.max_rows', None) # To eliminate rows truncation when printing
#pd.set_option('display.expand_frame_repr', True)

start = time.time() # debug
# The address and the port of the Elasticsearch server
server_address = "http://192.168.2.20:9200" # Change this address according to your settings (http://localhost:9200)

# Create an Elasticsearch object
es = Elasticsearch(server_address) # Change this address according to your settings (http://localhost:9200)

# Creates an empty Data Frame
ret_df = pd.DataFrame() 

# Create empty array to store the training data
X = np.empty((0,300))



# Variable for storing the last returned ID
last = 0

# While Elasticsearch returns 10000 documents
while True:
    # Search again starting from the last returned and sorting by ascending ID
    ret = es.search(index="books", query={"match_all": {}}, search_after=[last], sort=[{"id":"asc"}], size=10000)

    last = ret["hits"]["hits"][-1]["_source"]["id"] # Store the last returned ID
    for i in ret["hits"]["hits"]:# Iterate through all returned documents
        ret_df=ret_df.append(i["_source"], ignore_index=True) # Appends record to the DataFrame
        X = np.append(X, np.expand_dims(i["_source"]["embedding"], axis=0), axis=0)# Appends embedding vector to the training dataset
    if len(ret["hits"]["hits"]) < 10000:
        break

j = 1
# Empty lists for loss function coords
xloss=[]
yloss=[]

while True:
    kmeans = MiniBatchKMeans(n_clusters=j, init='k-means++', max_iter=1, batch_size=10000, n_init=1)
    kmeans.fit(X)
    xloss = np.append(xloss, j)
    yloss = np.append(yloss, kmeans.inertia_)
    print(j)
    print(kmeans.inertia_) 
    j += 100
    if j>10001:
        break

end = time.time()#debug

print("Time passed " + str(end-start))#debug
#print(end-start)#debug

plt.plot(xloss,yloss, color="r", marker="o", label="Check")
plt.title('Knee')
plt.xlabel('Clusters Number')
plt.ylabel('LOSS')

plt.legend()

plt.show()
