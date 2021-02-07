#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-10-04 11:41
# Filename     : expert_lz_clean.py
# Description  : 根据专家task表和工程技术中心生成量知特色专家库初始信息
#******************************************************************************
from pymongo import MongoClient
from tqdm import tqdm
import re
from bson import ObjectId
import sys 
import datetime 
import pymysql
import os
from dateutil import parser
import logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
    )



MongoUrl = "xxx"
# 爬虫库
res_kb = "xxxx"
# 爬虫专家表
res_kb_expert_aminer = "xxxx"
# 清洗库
res_kb_process = "xxxx"
res_kb_process_expert = "xxxx"

# 人工梳理库MySQL
mysql_config = {
    'host': 'xxx',
    'port': 0,
    'user': 'xxx',
    'password': 'xxx',
    'db': 'xxx',
    'charset': 'xxxx',
    'cursorclass': pymysql.cursors.DictCursor
}
prof_title_query = "select name from res_prof_title"


class ExpertFusionClean(object):
    def __init__(self):
        self.client = MongoClient(MongoUrl)
        self.res_kb_expert_aminer = self.client[res_kb][res_kb_expert_aminer]
        self.res_kb_process_expert = self.client[res_kb_process][res_kb_process_expert]
        self.sql_conn = pymysql.connect(**mysql_config)
        self.sql_cur = self.sql_conn.cursor()
        self.count_update = 0 # 更新记录
        
    def _init_prof_title(self):
        self.prof_title = []
        self.sql_cur.execute(prof_title_query)
        for t in self.sql_cur.fetchall():
            self.prof_title.append(t[0])
        self.prof_title.sort(key=lambda x:len(x),reverse=True)
        self.sql_cur.close()
        self.sql_conn.close()
        
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
        res = self.res_kb_expert_aminer.find({"crawl_time": {"$gte": iso_date}, "kId":{"$ne":""}}).sort([("_id",1)])
        return res
    
    def process_resume(self, string):
        clean_string = ""
        if string:
            clean_string = string.replace("\n","；").replace("\t","").replace(u"\xa0"," ").replace("<br><br>","；").replace("<br>"," ").replace("<br />"," ").replace("。；","。").replace("；；","；").strip()
            clean_string = re.sub(" {3,}","", clean_string)
        return clean_string

    def process(self, process_date):
        datas = self.query_daily_data(process_date)
        count = 0
        for expert_info in tqdm(datas):
            count += 1
            kId = expert_info["kId"]
            data = self.res_kb_process_expert.find_one({"kId":{"$in":[kId]}})
            update = {}
            if data: 
                ## 专家别名
                if expert_info["expert_name_en"] and (expert_info["expert_name_en"] not in data["alter_names"]):
                    update["alter_names"] = data["alter_names"]
                    update["alter_names"].append(expert_info["expert_name_en"])

                ## 修改title
                if (not data["professional_title"]) and expert_info["professional_title"]:
                    update["professional_title"] = expert_info["professional_title"]

                ## 修改地址
                if (not data["address"]) and expert_info["address"]:
                    update["address"] = expert_info["address"]

                ## 修改电话
                if (not data["tel"]) and expert_info["phone"]:
                    update["tel"] = expert_info["phone"]

                ## 修改email
                if (not data["email"]) and expert_info["email"]:
                    update["email"] = expert_info["email"]

                ## 修改
                if (not data["image"]) and expert_info["img_url"]:
                    update["image"] = expert_info["img_url"]

                ## 修改raw工作经历
                if (not data["raw_work_experience"]) and expert_info["work_experience"]:
                    clean_work_experience = self.process_resume(expert_info["work_experience"])
                    update["raw_work_experience"] = clean_work_experience

                ## 修改raw教育经历
                if (not data["raw_edu_experience"]) and expert_info["education_experience"]:
                    clean_edu_experience = self.process_resume(expert_info["education_experience"])
                    update["raw_edu_experience"] = clean_edu_experience

                ## 修改性别，以Aminer信息为主
                if expert_info["gender"] and expert_info["gender"] == "male":
                    update["gender"] = "男"
                elif expert_info["gender"] and expert_info["gender"] == "female":
                    update["gender"] = "女"

                ## 修改resume, 默认采用aminer上的简介
                if expert_info["expert_resume"]:
                    new_resume = self.process_resume(expert_info["expert_resume"])
                    update["resume"] = new_resume
            if update:
                update["update_time"] = datetime.datetime.today()
                self.res_kb_process_expert.update_one({"_id":data["_id"]}, {"$set":update})
                self.count_update += 1  
                logging.info("专家[{}]完成信息更新，ID=[{}]".format(data["name"], data["_id"]))
                
        logging.info("日期[{}] Aminer专家实体属性融合完成，更新记录[{}]条".format(self.process_date, self.count_update))
        
if __name__ == "__main__":
    cleaner = ExpertFusionClean()
    if len(sys.argv) > 1:
        cleaner.process(sys.argv[1])
    else:
        cleaner.process("yesterday")
                                 
                    


    