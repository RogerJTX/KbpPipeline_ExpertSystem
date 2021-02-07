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
res_kb_expert_task = "xxxx"
res_kb_expert_ckcest = "xxxx"
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


class ExpertLzClean(object):
    def __init__(self):
        self.client = MongoClient(MongoUrl)
        self.res_kb_expert_task = self.client[res_kb][res_kb_expert_task]
        self.res_kb_expert_ckcest = self.client[res_kb][res_kb_expert_ckcest]
        self.res_kb_process_expert = self.client[res_kb_process][res_kb_process_expert]
        self.sql_conn = pymysql.connect(**mysql_config)
        self.sql_cur = self.sql_conn.cursor()
        self._init_prof_title()
        self.count_insert = 0 # 新增记录
        
    def _init_prof_title(self):
        self.prof_title = []
        self.sql_cur.execute(prof_title_query)
        for t in self.sql_cur.fetchall():
            self.prof_title.append(t["name"])
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
        res = self.res_kb_expert_task.find({"update_time": {"$gte": iso_date}, "kId":{"$ne":[]}},no_cursor_timeout=True).sort([("update_time",1)])
        return res
    
    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            string = s.replace("（","(").replace("）",")").replace("\r","").replace("<br>","").replace("<br","").replace("/>","").strip()
            string = re.sub(" {3,}","",string)
        return string
    
    def process_organization(self, doc):
        '''所在高校机构，处理成数组'''
        res = []
        ins_str = doc["research_institution"]
        if ins_str:
            ins = re.split("[,;/]",ins_str)
            ins = [ i.strip() for i in ins]
            res.extend(ins)
            res = list(filter(lambda x:len(x)>0, res))
        return res
    
    def process_resume(self, doc):
        resume = ""
        cur_resume = doc["expert_resume"]
        if cur_resume and cur_resume!="暂无简历" and ("根据数据搜索表明" not in cur_resume):
            resume = self.process_str(cur_resume)
        return resume
    
    def process_title( self, doc):
        '''匹配一个MYSQL定义的职称'''
        res = ""
        title = doc["professional_title"] if "professional_title" in doc and doc["professional_title"] else ""
        if title:
            for t in self.prof_title:
                if t in title:
                    res = t
                    break
        return res 
    
    def init_schema(self, data):
        clean_data = {
                "_id":"",
                "kId": data["kId"],
                "name": self.process_str(data["expert_name"]),
                "alter_names":[], # 其他论文署名
                "organizations":[ data["research_institution"].strip() ],
                "gender":"",
                "image":"",
                "resume":"",
                "nation":"",
                "country":"中国",
                "address":"",
                "birthday":"",
                "email":"",
                "tel":"",
                "professional_title":"",
                "subject":"",
                "raw_edu_experience":"",
                "edu_experience":[],
                "final_edu_degree":"",
                "raw_work_experience":"",
                "work_experience":[],
                "experiences":[],
                "honors":[],
                "awards":[],
                "prop_score":0.0, # 主要属性完整度
                "url": data["url"],
                "crawl_time": data["update_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }
        return clean_data
    
    def close_mongo(self):
        if self.client:
            self.client.close()
          
    
    def process(self, process_date):
        datas = self.query_daily_data(process_date)
        batch_data = []
        count = 0
        
        for data in tqdm(datas):  
            count += 1        
            clean_data = self.init_schema(data)          
            kIds = data["kId"]
            for kid in kIds:
                expert_info = self.res_kb_expert_ckcest.find_one({"kId":kid})
                if expert_info:
                    # ID 初始化
                    if not clean_data["_id"]:
                        clean_data["_id"] = str(expert_info["_id"])
                    
                    # 研究机构
                    cur_organizations = self.process_organization(expert_info)
                    for org in cur_organizations:
                        if org not in clean_data["organizations"]:
                            clean_data["organizations"].append(org)
                            
                    # 性别
                    if (not clean_data["gender"]) and expert_info["gender"]:
                        clean_data["gender"] = str(expert_info["gender"])
                        if clean_data["gender"] == "1":
                            clean_data["gender"] = "男"
                            
                    # 头像
                    if (not clean_data["image"]) and expert_info["img_url"]:
                        clean_data["image"] = expert_info["img_url"]
                        
                    # 简历
                    if (not clean_data["resume"]) and expert_info["expert_resume"]:
                        clean_data["resume"] = self.process_resume(expert_info)
                        
                    # 学科
                    if (not clean_data["subject"]) and expert_info["subject"]:
                        clean_data["subject"] = self.process_str(expert_info["subject"])
                        
                    # 源网页地址
                    if (not clean_data["url"]) and expert_info["url"]:
                        clean_data["url"] = expert_info["url"]
                        
                    # 出生日期
                    if (not clean_data["birthday"]) and expert_info["birth_day"]:
                        clean_data["birthday"] = expert_info["birth_day"].split(" ")[0]
                        
                    # 联系方式
                    if (not clean_data["tel"]) and expert_info["tel"]:
                        clean_data["tel"] = expert_info["tel"].strip()
                    
                    # 职称   
                    if (not clean_data["professional_title"]) and expert_info["professional_title"]:
                        clean_data["professional_title"] = self.process_title(expert_info)
            
            exist = self.res_kb_process_expert.find_one({"_id":clean_data["_id"]})
            if exist:
                continue 
            logging.info("准备写入专家[{}]，ID = [{}]".format(clean_data["name"], clean_data["_id"]))
            self.res_kb_process_expert.insert_one(clean_data)
            self.count_insert += 1
            
            
        #     batch_data.append(clean_data)
        #     if batch_data and len(batch_data)>=100:
        #         insert_res = self.res_kb_process_expert.insert_many(batch_data)
        #         self.count_insert += len(insert_res.inserted_ids)
        #         logging.info("前[{}]条记录处理完成，写入数据ID RANGE = [{} - {}]".format(count, batch_data[0]["_id"], batch_data[-1]["_id"]))
        #         batch_data = []
        
        # if batch_data:
        #     insert_res = self.res_kb_process_expert.insert_many(batch_data)
        #     self.count_insert += len(insert_res.inserted_ids)
        #     logging.info("前[{}]条记录处理完成，写入数据ID RANGE = [{} - {}]".format(count, batch_data[0]["_id"], batch_data[-1]["_id"]))
        #     batch_data = []
            
        logging.info("日期[{}]专家数据清洗完成，新增记录[{}]条".format(self.process_date, self.count_insert))
        self.close_mongo()           

if __name__ == "__main__":
    cleaner = ExpertLzClean()
    if len(sys.argv) > 1:
        cleaner.process(sys.argv[1])
    else:
        cleaner.process("yesterday")
                
            
                    
                    
                    
                    
                    
            
        
        
        