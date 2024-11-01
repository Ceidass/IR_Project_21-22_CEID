from elasticsearch import Elasticsearch

#The address and the port of the Elasticsearch server
server_address = "http://192.168.2.16:9200" #change this address according to your settings (http://localhost:9200)

#Create an Elasticsearch object
es = Elasticsearch(server_address) #change this address according to your settings (http://localhost:9200)

ret = es.search("science")

print(ret)