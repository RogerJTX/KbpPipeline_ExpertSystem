#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-

# 迁移lz_data_yewu 中financing_quanguo的融资数据；匹配AI企业

from pymongo import MongoClient
import pymongo
import datetime
from bson import ObjectId
import re
import logging
import os


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

def company_from_ai():
    '''读取候选企业'''
    with open(os.path.join(kbp_path,"ai_companys.txt"), "r") as fr:
        companys_read = fr.readlines()
    
    companys = list(map(lambda x:x.strip(), companys_read))

    return companys



companys = company_from_ai()

mongo_url = 'xxx'
client = MongoClient(mongo_url)

source_col = client["xxxx"]["xxxx"]

new_col = client["xxxx"]["xxxx"]

no_finance = []
for company in companys:

    finds = source_col.find({"company_name":company})
    if finds.count()==0:
        no_finance.append(company)
        
    logger.info("公司[{}]找到融资事件[{}]条".format(company, finds.count()))
    for find in finds:
        new_col.insert_one(find)

with open(os.path.join(dir_path,"no_financing_company.txt"),"w") as fw:
    fw.write("\n".join(no_finance))

