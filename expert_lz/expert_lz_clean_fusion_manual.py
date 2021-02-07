#coding: utf-8
## 人工整理专家简介写入数据库，不部署，手动执行脚本
from pymongo import MongoClient
from tqdm import tqdm
from datetime import datetime
from bson import ObjectId
import os
import re 
dir_path = os.path.dirname(__file__)


MongoUrl = "xxx"

def load_expert_resume():
    with open(os.path.join(dir_path,"expert_resume_manual.txt"),"r") as fr:
        datas = fr.readlines()
    datas = list(map(lambda x:x.strip(), datas))
    return datas


client = MongoClient(MongoUrl)
res_kb_process_expert_lz = client["res_kb_process"]["res_kb_process_expert_lz"]

lines = load_expert_resume()
for line in tqdm(lines[1:]):
    items = line.split("\t")
    if len(items)<8:
        items = items + [""] * (8-len(items))
        print("oid {} name {} org {} resume {} email {} phone {} url {}".format(items[0], items[1], items[2], items[3], items[4],items[5], items[7]))
    oid, name, org, resume, email, phone, _, url = items 
    oid = oid.strip()
    name = name.strip()
    orgs = [ o.strip() for o in org.split("；")]
    resume = resume.strip()
    email = email.strip()
    phone = phone.strip()
    url = url.strip()
    update = {
        "resume":resume,
        "email": email,
        "telephone":phone,
        "url": url 
    }
    for org in orgs:
        data = res_kb_process_expert_lz.find_one({"name":name,"organizations":{"$in":[org]}})
        if data:
            res_kb_process_expert_lz.update_one({"_id":data["_id"]},{"$set":update})
            print("专家[{}]完成信息更新，ID=[{}]".format(data["name"], data["_id"]))
            break
    
    
                                 
print("专家简介更新完成")
    