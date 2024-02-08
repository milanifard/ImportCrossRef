import pymongo
import json
import gzip
import shutil
from os import listdir, rename, remove, path
from os.path import isfile, join
import glob
from pymongo import MongoClient, InsertOne
db_connection_string = 'mongodb://localhost:27017'
cross_files_path = "f:\\Crossref\\April 2023 Public Data File from Crossref"
client = pymongo.MongoClient(db_connection_string)
db = client["crossref"]
collection = db["crossref"]
i = 0
listing = glob.glob(cross_files_path+'/*.gz')
i = 0
for filename in listing:
    print(filename)
    with gzip.open(filename, 'rb') as f_in:
        with open(filename+".json", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        with open(filename+".json", mode="r", encoding='utf-8') as f:
            json_content = ''.join(f.readlines())
            print("*")
            data = json.loads(json_content)
            collection.insert_many(data["items"])
        remove(filename+".json")
    rename(filename,"f:\\Crossref\\DoneFiles\\"+path.basename(filename))
