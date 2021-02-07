from pymongo import MongoClient
import pymongo
import datetime
from tqdm import tqdm

mongo_url = 'mongodb://xxx'
client = MongoClient(mongo_url)


# 机构数据字段添加
source = client["res_kb"]["res_kb_company_task"]
target = client["res_kb"]["res_kb_company"]

datas = source.find({"source":"投资机构","crawled_basic":{"$in":["success","exist"]}},no_cursor_timeout=True)
count = 0
for data in tqdm(datas):
    item = target.find_one({"search_key":data["company_name"]})
    try:
        if item and type(item["tag"])==list and "投资机构" in item["tag"]:
            continue 
        if item and "投资机构" in item["tag"]:
            oid = item["_id"]
            tag = item["tag"].split("，")
            target.update_one({"_id":oid},{"$set":{"tag": tag}})
            count += 1
        elif item and ("投资机构" not in item["tag"]):
            oid = item["_id"]
            tag = item["tag"]
            if type(tag)==str:
                tag = [tag]+["投资机构"]
            elif type(tag)==list:
                tag = tag + ["投资机构"]
            tag = list(filter(lambda x:len(x)>0, tag))
            target.update_one({"_id":oid},{"$set":{"tag": tag}})
            count += 1
            
    except Exception as e:
        print(item["_id"])
        print(e)
print(count)
client.close()
        
