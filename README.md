# cosmos_blob_file_compare
 This Repository will compare data from cosmos db (which is in json format) by querying to cosmos db with  the file available in blob to validate data.

# Usage Details of Script:

1. Python will connect to AZure cosmos db and Azure blob storage.
2. Once connection is established python will send query to cosmos db and output of the query is available as iterable.
3. we can loop through and store as a list.
4. python will Download file from blob storage and store in local.
5. it will read the file as dictonary and store in list.
6. Python will also keep a copy of data from cosmos db using json.dump
7. Than records in cosmos db are compared with csv and bad records are stored in json file and vise verse

