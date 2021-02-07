#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-10-04 11:41
# Filename     : expert_lz_clean_award.py
# Description  : 定点采集专家奖项数据融合，目前手动执行，数据从爬虫库流转到清洗库；
# !!! 执行完后需手动执行 expert_lz_kbp_mongo.py 进行清洗库到MONGO构建库的数据同步，
#     确保清洗库数据属性与MONGO构建库一致；
#******************************************************************************
from pymongo import MongoClient
from tqdm import tqdm 
import datetime 
import pymysql
import os
import sys
import re
from dateutil import parser
from bson import ObjectId
import logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
    )

MongoUrl = "xxxx"
# 爬虫库
res_kb = "xxxx"
# 爬虫专家表
res_kb_expert_award = "xxxx"
# 清洗库
res_kb_process = "xxxx"
res_kb_process_expert = "xxxx"


class ExpertLzClean(object):
    def __init__(self):
        self.client = MongoClient(MongoUrl)
        self.res_kb_expert_award = self.client[res_kb][res_kb_expert_award]
        self.res_kb_process_expert = self.client[res_kb_process][res_kb_process_expert]
        self.count_insert = 0 # 新增记录

        
    def query_daily_data(self, process_date):
        if process_date == "yesterday":
            process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif process_date == "today":
            process_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            
        else:
            raise Exception("无效参数的日期")
        
        self.process_date = process_date
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_expert_award.find({},no_cursor_timeout=True).sort([("_id",1)])
        return res
    
    
    def process(self, process_date):
        
        datas = self.query_daily_data(process_date)
        for data in tqdm(datas):
            expert_name = data["expert_name"]
            org = data["research_institution"]
            # 机构字段NONE值过滤
            if not org:
                continue
            award_type = data["award_type"].strip()
            expert = self.res_kb_process_expert.find_one({"name":expert_name, "organizations":{"$regex":org}})
            if expert:
                honors = expert["honors"]
                if award_type not in honors:
                    honors.append(award_type)
                    update = {"honors":honors, "update_time":datetime.datetime.today()}
                    self.res_kb_process_expert.update_one({"_id":expert["_id"]},{"$set":update})
                    logging.info("专家[{}]，ID=[{}]的荣誉标签已更新".format(expert_name, expert["_id"]))
                    
if __name__ == "__main__":
    cleaner = ExpertLzClean()
    if len(sys.argv) > 1:
        cleaner.process(sys.argv[1])
    else:
        cleaner.process("yesterday")                
