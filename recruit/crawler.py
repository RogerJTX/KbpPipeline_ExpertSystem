#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-

from pymongo import MongoClient
import pymongo
from pymongo import errors
import datetime
from bson import ObjectId
import re
import logging
import os


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
print(dir_path)
kbp_path = os.path.dirname(dir_path)
print(kbp_path)
file_path = os.path.join(kbp_path,"necar_companys.txt")
print(file_path)


def company_from_file():
    '''读取候选企业'''
    with open(file_path, "r") as fr:
        companys_read = fr.readlines()
    
    companys = list(map(lambda x:x.strip(), companys_read))

    return companys



mongo_url = 'mongodb://xxx/'
client = MongoClient(mongo_url)
source_col = client["xxx"]["xxx"]
target_col = client["xxx"]["xxx"]

companys = company_from_file()

no_info = []
c = 0 


# 迁移依据 company_name，不是search_key


for company in companys:
    print(company)
    # 检查爬虫库是否已有该公司数据
    exist = target_col.find_one({"company_name":company})
    if not exist:
        zn_name = company.replace("(","（").replace(")","）")
        exist = target_col.find_one({"company_name":zn_name})
    if exist:
        c += 1
        continue 

    # 没有公司数据，查询combine库，迁移到爬虫库
    rs = source_col.find({"company_name":company})
    if rs.count()==0:
        zn_name = company.replace("(","（").replace(")","）")
        rs = source_col.find({"company_name":zn_name})

    print("公司{}找到岗位需求[{}]个".format(company,rs.count()))

    c += 1
    if c%100==0 or c == len(companys):
        logger.info("正迁移前[{}]家企业招聘信息".format(c))

    if rs.count() != 0:
        data = []
        for r in rs:
            data.append(r)
        try:
            insert_res = target_col.insert_many(data)
            count = len(insert_res.inserted_ids)
            logger.info("招聘信息迁移，企业名=[{}]，招聘记录[{}]条".format(company,count))
        except errors.DuplicateKeyError:
            logging.warn("重复数据")
            continue 
    else:
        no_info.append(company)
        logger.info("无招聘信息，公司名=[{}]".format(company))

with open(os.path.join(dir_path,"necar_no_recruit_company.txt"), "w") as fw:
    fw.write("\n".join(no_info))

    

    
  


client.close()