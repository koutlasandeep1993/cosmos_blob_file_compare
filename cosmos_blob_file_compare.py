
import pydocumentdb.document_client as document_client
import json
import csv
from azure.storage.blob import BlockBlobService

#Configuring meta data#
with open ( "C:\luna\json\meta_data.json") as data_file:
  data=json.load(data_file)
data_file.close()

####cosmos DB config####
cosmos_key = data["cosmos"][0]["key"]
cosmos_url = data["cosmos"][0]["url"]
database_name=data["cosmos"][0]["database_name"]
container_name=data["cosmos"][0]["container_name"]
database_link = 'dbs/' + database_name
collection_link = database_link + '/colls/{0}'.format(container_name)
doc_link = collection_link + '/docs/' +'9513'
query_cosmos= data["query"][0]["query"]
###Blob Storage config###

blob_key=data["blob"][0]["key"]
account=data["blob"][0]["account"]
container_name=data["blob"][0]["container_name"]


####connecting to cosmos db#

client_doc=document_client.DocumentClient(cosmos_url, {'masterKey': cosmos_key})

query = { "query": query_cosmos}    

options = {} 
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 2
print("############Connected to Cosmos DB############")
#### Connecting to Blob Storage####

block_blob_service = BlockBlobService(account_name=account, account_key=blob_key)
generator = list(block_blob_service.list_blobs(container_name))
for blob in generator:
    blob_name=blob.name
    file_path='C:\\luna\\files\\'+blob_name
    block_blob_service.get_blob_to_path(container_name, blob_name, file_path, open_mode='wb', snapshot=None, start_range=None, end_range=None, validate_content=False, progress_callback=None, max_connections=2, lease_id=None, if_modified_since=None, if_unmodified_since=None, if_match=None, if_none_match=None, timeout=None)

print("############Connected to BLOB Storage############")

###################  Configuration completed ###############

print("Started fetching data from Cosmos DB ")

result_iterable = client_doc.QueryDocuments(collection_link, query, options)
lists_of_cosmos=[]
for line in list(result_iterable):
    del line['_ts'],line['EXPIRY_DATE'],line['FULL_UDI'],line['_attachments'],line['_etag'],line['_self'],line['_rid'],line['PACKAGE_LOT_NUMBER']
    lists_of_cosmos.append(line)


print("Writing Cosmos output to json file in local")

cosmos_file= open("C:\\luna\\cosmos_tgt.json","w")
j_cosmos1=json.dumps(lists_of_cosmos)
json.dump(lists_of_cosmos, cosmos_file)
j_cosmos2=json.loads(j_cosmos1)
cosmos_file.close()


print("Started fetching data from CSV file ")  
list_of_file=[]
input_file = csv.DictReader(open("C:\\luna\\blob_src.csv"))
for row in input_file:
    list_of_file.append(row)
    


print("campareing csv file with cosmos db")

with open("C:\\luna\\FILE_SRC_NOT_IN_TGT_COSMOS.json", 'w') as outFile1:
    for line_one in list_of_file:
        if line_one not in j_cosmos2:
            result = json.dumps(line_one,indent = 2)
            outFile1.write(result)

j_csv1= json.dumps(list_of_file)
j_csv2= json.loads(j_csv1)

print("campareing COSMOS DB with CSV File")
with open("C:\\luna\\COSMOS_TGT_NOT_IN_SRC_FILE.json", 'w') as outFile2:
    for line1 in lists_of_cosmos:
        if line1 not in j_csv2:
            result1 = json.dumps(line1,indent = 2)
            outFile2.write(result1)


print("NO of records in CSV stored in BLOB "+collection_link+ " : " +str(len(list_of_file)))
print("NO of documents in "+collection_link+ " : " + str(len(lists_of_cosmos)))
