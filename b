[200~# -*- coding: utf-8 -*-
"""
Created on Wed Jul 27 10:51:17 2022

@author: Abhijeet Singh
"""

import logging
import pymongo
import datetime as dt
import pandas as pd

try:
    client = pymongo.MongoClient('mongodb://122.160.40.168:27018')
    print("Server connected")
except pymongo.errors.ServerSelectionTimeoutError as err:
    print(err)
db = client["cscspv_v4"]
df = pd.DataFrame(db.quarterly_dsp_dim_cscid_merchantid_productid_serviceid.find({},{"_id": 0 ,"cscid": 1 ,"merchantid": 1 ,"recorddate": 1 , "productid": 1 , "serviceid": 1 , "amt": 1, "txn": 1 }))
cols = ["cscid","merchantid","productid","recorddate","serviceid"]
df['count'] = df.groupby(cols)['cscid'].transform("size")
df = df[df["count"] > 1 ]
df1=df.groupby(["cscid","merchantid","productid","recorddate","serviceid"]).agg({"amt":"sum","txn":"sum"})
#df1.reset_index()
#print(df1)
mydb = client["Abhijeet"]
mycol = mydb["newcscid"]

myquery = { "address": { "$regex": "^S" } }
newvalues = { "$set": { "name": "Minnie" } }

x = mycol.update_many(myquery, newvalues)

print(x.modified_count, "documents updated.")
