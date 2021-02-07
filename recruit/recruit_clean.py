#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-09 10:55
# Filename     : recruit_clean.py
# Description  : res_kb_recruit招聘信息清洗；
# 清洗逻辑：
# 1 去重，根据 公司名-职位-URL-省市区信息联合确定，进行去重； 
# 2 薪水统一格式化为按月计算； 
# 3 添加省市区信息； 
# 4 职位类别等符号格式化
#******************************************************************************
from urllib.request import urlopen,quote
import json
import logging
from logging.handlers import RotatingFileHandler
import requests
from pymongo import MongoClient, errors
import datetime
import re
import configparser
import sys
from dateutil import parser
import uuid
import pymysql
import os

main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_path)

from common_utils.address_parser import AddressParser

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

def set_log():
    logging.basicConfig(level=logging.INFO) 
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s")
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)


class RecruitClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_recruit = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_recruit")] # 招聘信息，爬虫库
        self.res_kb_process_recruit = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_recruit")] # 招聘信息，清洗库
        self.count_insert = 0
        self.count_dupl = 0

    def process_publish_time(self, doc):
        '''岗位发布时间，对应爬虫create_time '''
        res = ""
        if "create_time" in doc and doc["create_time"]:
            create_time = doc["create_time"]
            if create_time:
                res = str(doc["crawl_time"])[:4] + "-" + create_time
        return res 




    def process_str(self, s):
        string = ""
        if s:
            string = re.sub("[\t\r\n\xa0]","", s.strip()).replace("（","(").replace("）",")")
            string = string.replace("暂无","")
        
        return string

 


    def query_data(self, crawl_date):

        if crawl_date == "yesterday":
            crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date
            
        else:
            raise Exception("无效参数")
        
        iso_date_str = crawl_date + "T00:00:00+08:00"
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_recruit.find({"crawl_time": {"$gte": iso_date}},no_cursor_timeout = True).sort([("crawl_time",1)])
        # res = self.res_kb_recruit.find({"transfer_time": {"$gte": iso_date}},no_cursor_timeout = True).sort([("crawl_time",1)])
        return res

    def process_edu_require(self, doc):
        res = ""
        if "edu_require" in doc and doc["edu_require"]:
            res = doc["edu_require"]
            if "初中" in res:
                res = "初中"
        return res 

    def process_salary(self, doc):
        res = ""
        if "job_salary" in doc and doc["job_salary"]:
            salary = doc["job_salary"]
            _range = self.salary_type2unit(salary)
            res = self.salary2str(_range)
        return res


    def salary_type2unit(self, _doc_str):
        ''' 对不同形式的工资格式统一成‘xxx-xxx元/月’或‘xxx元/月'''

        if not _doc_str:
            return (0,0)
        _time_s = ["小时","天","月","年"]
        _salary_n = ["元","千","万"]
        _doc_str = _doc_str.replace("以下","").replace("以上","")

        if _doc_str.find("元/月")>-1:
            str_end = _doc_str.index("月")
            _doc_str = _doc_str[:str_end+1]
            return _doc_str
        for _ti in _time_s:
            for _si in _salary_n:
                temp_end = _si+"/"+_ti
                if _doc_str.find(temp_end)>-1:
                    last_rang_s = 0
                    last_rang_e = 0
                    temp_s = _doc_str.rstrip(temp_end)

                    if temp_s.find("-") > -1:
                        s_range = temp_s.split("-")
                        start_s = float(s_range[0])
                        end_s = float(s_range[1])
                    else:
                        start_s = 0
                        end_s = float(temp_s)

                    if _si == "千":
                        last_rang_s = start_s*1000
                        last_rang_e = end_s*1000
                    elif _si == "万":
                        last_rang_s = start_s*10000
                        last_rang_e = end_s*10000
                    else:
                        last_rang_s = start_s
                        last_rang_e = end_s

                    if _ti == "小时":
                        last_rang_s = last_rang_s*8*30
                        last_rang_e = last_rang_e*8*30
                    elif _ti == "天":
                        last_rang_s = last_rang_s*30
                        last_rang_e = last_rang_e*30
                    elif _ti == "年":
                        if last_rang_s !=0:
                            last_rang_s = last_rang_s//12
                        last_rang_e = last_rang_e//12
                    return (int(last_rang_s),int(last_rang_e))
                    
                #end if
            #end for
        #end for
        return (0,0)

    def salary2str(self, salary_range):
        ''' 将(xxx,xxx)转变成xxx-xxx元/月'''
        if type(salary_range) == str:
            return salary_range
        
        if (0,0) == salary_range:
            return ""
        if 0 == salary_range[0]:
            return str(salary_range[1])+"元/月"
        return str(salary_range[0])+"-"+str(salary_range[1])+"元/月"

    def process_job_class(self, doc):
        '''去除\t\r\n等符号，转为统一分隔符/'''
        res = ""
        if "job_class" in doc and doc["job_class"]:
            res = doc["job_class"]
            res = re.sub("[\r\t\n]+","/",res)
        return res

    def process_address(self, doc):
        '''地址分布在meta/address和address里'''
        # 数据规律：meta.address如果只有省信息，address也只有省的信息；meta.address如果是城市-区域信息，address可能包括城市-区域，也可能不包括；
        # 策略：meta.address优先匹配省市区信息，address只做详细补充
        res = {
            "address":"",
            "province":"",
            "city":"",
            "area":""
        }
        if "meta" in doc and doc["meta"] and "address" in doc["meta"] and doc["meta"]["address"] and "异地" not in doc["meta"]["address"]:
            pca_info = self.get_address(doc["meta"]["address"])
            res.update(pca_info)
        if "address" in doc and doc["address"] and "异地" not in doc["address"]:
            res["address"] = self.process_str(doc["address"])
        return res 


    def get_address(self, _adress):
        ''' 手动提取省市区'''

        adress_dict = {}
        adress_dict['country'] = '中国'
        if '西藏' == _adress or '广西' == _adress:
            adress_dict['province'] = _adress
            adress_dict['city'] = ''
            adress_dict['area'] = ''
        elif _adress.find('省')>-1:
            adress_dict['province'] = _adress.strip('省')
            adress_dict['city'] = ''
            adress_dict['area'] = ''
        elif _adress.find('-')>-1:
            adress_dict['province'] = ''
            city_area = _adress.split('-')
            adress_dict['city'] = city_area[0]
            adress_dict['area'] = city_area[1]
        elif _adress.find('异地')>-1:
            adress_dict['province'] = ''
            adress_dict['city'] = ''
            adress_dict['area'] = ''
        else:
            adress_dict['province'] = ''
            adress_dict['city'] = _adress
            adress_dict['area'] = ''

        return adress_dict

    def company_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"necar_companys.txt"), "r") as fr:
            companys_read = fr.readlines()       
        companys = list(map(lambda x:x.strip(), companys_read))
        return companys

    def insert_batch(self, batch_data):
        '''根据name和省市区信息去重'''
        new_batch = []
        for data in batch_data:
            # 根据name和省市区信息去重
            exist = self.res_kb_process_recruit.find_one({"name":data["name"],"province":data["province"],"city":data["city"],"area":data["area"]})
             
            if exist:
                # 重点关注字段 job details 
                if data["job_details"] and (not exist["job_details"]):
                    # update and replace this record
                    self.res_kb_process_recruit.remove({"_id":exist["_id"]})
                    self.res_kb_process_recruit.insert_one(data)
                    logger.info("岗位需求[{}]的job_detail字段找到有数据的记录，已更新，新ID=[{}]".format(data["name"], data["_id"]))

                self.count_dupl += 1
                continue
            new_batch.append(data)
        if new_batch:
            insert_res = self.res_kb_process_recruit.insert_many(new_batch)
            self.count_insert += len(insert_res.inserted_ids)
            logger.info("批数据新增完成")
        else:
            logger.info("批量数据去重后无数据新增")


    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于process_date以后的招聘数据
        '''

        count = 0
        res = self.query_data(crawl_date)
        total = res.count()

        # 手动执行步骤
        # res = self.company_from_file()
        # total = len(res)

        batch_data = []
       
        for doc in res: 
        # 手动执行步骤
        # for name in res:
            
        #     exist = self.res_kb_process_recruit.find_one({"company_name": name})
        #     if exist:
        #         continue

        #     # 匹配下中英文符号公司名称
        #     docs = self.res_kb_recruit.find({"company_name":name})
        #     if not docs:
        #         zn_name = name.replace("(","（").replace(")","）")
        #         docs = self.res_kb_recruit.find({"company_name":zn_name})

        #     for doc in docs: 

            logger.info("处理招聘记录，企业名=[{}]".format(doc["company_name"]))

            update_doc = {
                "_id": str(doc["_id"]), # 直接用ObjectId的值作为后续统一的ID值
                "job_title": self.process_str(doc["job_title"]),
                "job_class": self.process_job_class(doc),
                "company_name": self.process_str(doc["company_name"]),
                "search_key": doc["search_key"],
                "edu_require": self.process_edu_require(doc),
                "job_details": self.process_str(doc["job_details"]) if "job_details" in doc else "",
                "job_salary": self.process_salary(doc),
                "publish_time":self.process_publish_time(doc),
                "scale": self.process_str(doc["scale"]),
                "experience_require": self.process_str(doc["experience_require"]),
                "recruit_number": self.process_str(doc["recruit_number"]),  
                "url": doc["url"],
                "source": doc["source"],
                "html":doc["html"],
                "crawl_time": doc["crawl_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }

            update_doc["name"] = update_doc["company_name"] + "_" + update_doc["job_title"] + "_" + update_doc["url"]
            address_res = self.process_address(doc)
            update_doc.update(address_res)        
            count += 1

            batch_data.append(update_doc)

            if count % 100 == 0 or count == total:
                # 批量写入
                logger.info("正在写入前[{}]条岗位招聘信息的处理结果".format(count))
                self.insert_batch(batch_data)
                batch_data = []



        logger.info("招聘爬虫数据处理完毕，找到[{}]个招聘数据，已存在数据[{}]条，写入清洗库[{}]个招聘".format(
                        total, self.count_dupl, self.count_insert ))



if __name__ == "__main__":

    # 最早日期  2019-05-29
    
	cleaner = RecruitClean()

	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
