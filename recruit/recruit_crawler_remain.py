from pymongo import MongoClient
import pymongo
from pymongo.errors import DuplicateKeyError
import datetime
import re
from bson import ObjectId
import logging

# 转移剩余的全量招聘岗位数据

mongo_url = 'mongodb://xxx'
client = MongoClient(mongo_url)
recruit_source = client["xxx"]["xxx"]
recruit_target = client["xxx"]["xxx"]



# 招聘数据同步
print("同步企业招聘数据")
recruits = recruit_source.find({}).sort([("crawl_time",1)])
print(recruits.count())
insert = 0
dupl = 0
for r in recruits:
    print(r["_id"])
    try:
        r["transfer_time"] = datetime.datetime.today()
        recruit_target.insert_one(r)
        insert += 1
    except DuplicateKeyError:
        print("重复数据，数据id=[{}]已存在".format(str(r["_id"])))
        dupl += 1

print("新增数据[{}]条，重复数据[{}]条".format(insert, dupl))



