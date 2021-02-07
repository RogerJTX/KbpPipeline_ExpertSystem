#coding: utf-8
#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-10-05 14:41
# Filename     : expert_fusion_baidu.py
# Description  : 百度百科专家属性信息融合
#******************************************************************************
from pymongo import MongoClient
from tqdm import tqdm 
import datetime 
import pymysql
import os
import sys
import re 
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
res_kb_expert_baike = "xxxx"
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
        self.res_kb_expert_baike = self.client[res_kb][res_kb_expert_baike]
        self.res_kb_process_expert = self.client[res_kb_process][res_kb_process_expert]
        self.sql_conn = pymysql.connect(**mysql_config)
        self.sql_cur = self.sql_conn.cursor()
        self.count_update = 0 # 新增记录
        
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
        res = self.res_kb_expert_baike.find({"crawl_time": {"$gte": iso_date}, "kId":{"$ne":""}}).sort([("_id",1)])
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
            kIds = expert_info["kId"]
            update = {}
            for kid in kIds:
                data = self.res_kb_process_expert.find_one({"kId":{"$in":[kid]}})
                if data:
                    ## 添加专家别名
                    if expert_info["expert_name_en"] and (expert_info["expert_name_en"] not in data["alter_names"]):
                        update["alter_names"] = data["alter_names"] 
                        update["alter_names"].append(expert_info["expert_name_en"])

                    ## 修改title
                    if (not data["professional_title"]) and expert_info["title"]:
                        update["professional_title"] = expert_info["title"]

                    ## 修改性别
                    if (not data["gender"]) and ("男" in expert_info["gender"]):
                        update["gender"] = "男"
                    elif (not data["gender"]) and ("女" in expert_info["gender"]):
                        update["gender"] = "女"
                        
                    ## 出生日期
                    if (not data["birthday"]) and expert_info["birthday"]:
                        update["birthday"] = expert_info["birthday"].strip()

                    ## 修改resume
                    if ((not data["resume"]) or (data["resume"].startswith("根据数据搜索")) ) and expert_info["resume"]:
                        new_resume = self.process_resume(expert_info["resume"])
                        update["resume"] = new_resume
                    
                    ## 找到任一kid的专家实体即可
                    break
            if update:
                update["update_time"] = datetime.datetime.today()
                self.res_kb_process_expert.update_one({"_id":data["_id"]}, {"$set":update})
                self.count_update += 1  
                logging.info("专家[{}]完成信息更新，ID=[{}]".format(data["name"], data["_id"]))
                
        logging.info("日期[{}] baike专家实体属性融合完成，更新记录[{}]条".format(self.process_date, self.count_update))
        
if __name__ == "__main__":
    ## 爬虫时间 2020-09-28
    cleaner = ExpertFusionClean()
    if len(sys.argv) > 1:
        cleaner.process(sys.argv[1])
    else:
        cleaner.process("yesterday")
                                 
                    


    