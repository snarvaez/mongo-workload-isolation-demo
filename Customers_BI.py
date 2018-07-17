from __future__ import print_function
import pymongo
import random
import datetime

from random import randrange
from datetime import timedelta

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

date_range_min = datetime.datetime.strptime('1/1/2018', '%m/%d/%Y')
date_range_max = datetime.datetime.strptime('12/31/2018', '%m/%d/%Y')

print('=== CONNECTING TO MONGODB  ===')
#connstr = "mongodb://snarvaez:snarvaez**@ec2-54-244-49-101.us-west-2.compute.amazonaws.com:27017,ec2-34-211-126-57.us-west-2.compute.amazonaws.com:27017,ec2-34-216-86-139.us-west-2.compute.amazonaws.com:27017,ec2-34-219-248-214.us-west-2.compute.amazonaws.com:27017,ec2-34-220-88-73.us-west-2.compute.amazonaws.com:27017/?replicaSet=rs&authSource=admin"
connstr = "mongodb://snarvaez:snarvaez**@ec2-54-244-49-101.us-west-2.compute.amazonaws.com:27017/?replicaSet=rs&authSource=admin"

client = pymongo.MongoClient(connstr,
    readPreference='secondary',
    readPreferenceTags='use:BI')

db = client.CustomerSingleView
coll = db.Customers

for i in range(100):
    startDt = random_date(date_range_min, date_range_max)
    endDt = random_date(date_range_min, date_range_max)
    try:
        result = coll.find({'policies.nextRenewalDt': { '$gte': startDt, '$lte': startDt }}).count()
        print("Found: " + str(result))
    except Exception as err:
        print(err)
        raise
