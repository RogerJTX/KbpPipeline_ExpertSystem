#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-

# 会议处理数据暂时从ES人工整理过的导出，重新清洗
from elasticsearch import Elasticsearch
from pymongo import MongoClient

mongo_url = "mongodb://xxx"
es_url = "http://xxx"
index_name = "xxx"

es = Elasticsearch([es_url])
mongo_conn = MongoClient(mongo_url)

res_kb = mongo_conn["res_kb"]["res_kb_conference_es"]


doc = {'query': {'match_all': {}}}
_searched = es.search(index=index_name,body=doc,size=500)
hits = _searched["hits"]["hits"]


count = 0
for hit in hits:
    source = hit["_source"]
    if not "create_time" in source:
        continue
    source["es_id"] = hit["_id"]
    res_kb.insert_one(source)
    count += 1
    print(count)
