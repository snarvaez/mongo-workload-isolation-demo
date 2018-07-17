from __future__ import print_function
import pymongo
import random

print('=== CONNECTING TO MONGODB  ===')
connstr = "mongodb://snarvaez:snarvaez**@ec2-54-202-54-66.us-west-2.compute.amazonaws.com:27017/CustomerSingleView?&replicaSet=rs&authSource=admin"
client = pymongo.MongoClient(connstr, readPreference='secondary', readPreferenceTags='use:BI')
db = client.CustomerSingleView
coll = db.Customers

for i in range(10000):
    # Two shards. sh0: region 0-999. sh1: region 1000-1999
    region = random.randint(0,1999)
    try:
        result = coll.find({'region':region}).count()
        print("region: " + str(region) + ". count: " + str(result))
    except Exception as err:
        print(err)
        raise
