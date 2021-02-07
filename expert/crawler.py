#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-05-08 09:28
# Filename     : crawler.py
# Description  : expert_ckcest专家库爬虫数据迁移
#******************************************************************************

from pymongo import MongoClient
import pymongo
import datetime
from bson import ObjectId
import re


mongo_url = 'mongodb://xxx'
client = MongoClient(mongo_url)


source_col = client["xxx"]["xxx"]
target_col = client["xxx"]["xxx"]

es = source_col.find({},no_cursor_timeout=True).sort([("crawl_time",1)])
total = es.count()
batch = []
count = 0
for e in es:
    if e["book"] or e["journal_article"] or e["tech_achievement"] or e["patent"]:   
        batch.append(e)
    count += 1
    if count %100 ==0 or count == total:
        if batch:
            target_col.insert_many(batch)
        batch = []
        print("expert [{}], id = [{}]".format(e["expert_name"], str(e["_id"])))
        print("处理数据[{}]条".format(count))




  


client.close()