#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-05-13 19:49
# Filename     : trademark_clean.py
# Description  : res_kb_trademark企业商标信息清洗，主要是值的清洗和字段按新schema数据格式组织重新插入
#******************************************************************************
from urllib.request import urlopen,quote
import json
import logging
import requests
from pymongo import MongoClient
import datetime
import re
import configparser
import sys
from dateutil import parser
import uuid
import os
from logging.handlers import RotatingFileHandler
import copy

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

def set_log():
    logging.basicConfig(level=logging.INFO) 
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)

class TrademarkClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_trademark = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_trademark")] 
        self.res_kb_process_trademark = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_trademark")] # 商标清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
     

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            if s == "-":
                s = ""
            string = s.replace("（","(").replace("）",")").replace("\r","")
        return string


    def process_str_list(self, p):

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;；,，；\n\t ]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list


    def process_addr(self, doc):
        '''申请人地址和邮编'''
        addr_dict = {
            "applicant_addr":"",
            "applicant_code":""
        }
        if doc["applicant_zip"]:
            addr_dict["applicant_zip"] = doc["applicant_zip"]

        else:
            addr = doc["applicant_addr"].split(" ")
            if len(addr) > 1:
                addr_dict["applicant_zip"] = addr[0]
                addr_dict["applicant_addr"] = addr[1]
            elif len(addr)  == 1:
                addr_dict["applicant_addr"] = addr[0]

        return addr_dict

    def insert_batch(self, batch_data):
        '''过滤无效数据，根据商标号去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        unique_vals = [] # 记录去重的唯一标识字段的值，这里是商标注册申请号
        new_batch = []

        for data in batch_data:

            if not data["application_number"]:
                self.count_ignore += 1
                logging.info("跳过商标，缺少关键字段-商标注册申请号。ObjectId=[{}]".format(data["_id"]))
                continue

            application_num = data["application_number"]
            if application_num in unique_vals:                   
                self.count_inner_dupl += 1
                continue 
            new_batch.append(data)
            unique_vals.append(application_num)

        logger.info("批量数据批内去重，组内去重[{}]条数据".format(self.count_inner_dupl))

        logger.info("批量数据与MongoDB去重中...")
        
        dupl_datas = self.res_kb_process_trademark.find({"application_number":{"$in":unique_vals}})
        for dupl_data in dupl_datas:
            index = unique_vals.index(dupl_data["application_number"])
            unique_vals.pop(index)
            new_batch.pop(index)
            self.count_dupl += 1

        if not new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_trademark.insert_many(new_batch)
        self.count_insert += len(insert_res.inserted_ids)
        logger.info("批量数据写入完成")



    def query_daily_data(self, crawl_date):
        if crawl_date == "yesterday":
            crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date
            
        else:
            raise Exception("无效参数")
        
        iso_date_str = crawl_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_trademark.find({'crawl_time': {'$gte': iso_date}},no_cursor_timeout=True).sort([("crawl_time",1)])
        return res


    def company_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"necar_companys.txt"), "r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

    def process_right_date(self, doc):
        '''专利权起止日期拆分'''
        res = {
            "right_from_date":"",
            "right_to_date":""
        }
        right_d = doc["exclusive_right_date"]
        if "至" in right_d and len(right_d)>1:
            res["right_from_date"] = right_d.split("至")[0]
            res["res_to_date"] = right_d.split("至")[1]

        return res

    def process_history_status(self, doc):
        res = []
        history = doc["status"]
        if type(history)==list:
            for h in history:
                if type(h)==dict:
                    h["status"] = self.process_str(h["status"])
                    h["date"] = self.process_str(h["date"])
                    res.append(h)
                elif type(h)==str: # 爬虫有部分数据缺失URL，旧字段没更新，需手动更改成dict
                    new_h = {
                        "status": self.process_str(h),
                        "date":""
                    }
                    res.append(new_h)
        return res 

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()
     

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的商标数据
        '''

        count = 0
        res = self.query_daily_data(crawl_date)
        total = res.count()
        logger.info("商标爬虫资源库本次查询待梳理数据[{}]条".format(total))

        # 手动执行步骤
        # res = self.company_from_file()
        # total = len(res)

        batch_data = []
        
        for doc in res:

        #手动执行步骤
        # for name in res:
            
        #     exist = self.res_kb_process_trademark.find_one({"search_key": name})
        #     if exist:
        #         continue

            
        #     docs = self.res_kb_trademark.find({"search_key":name})
        #     if not docs:
        #         zn_name = name.replace("(","（").replace(")","）")
        #         docs = self.res_kb_trademark.find({"search_key":zn_name})

        #     for doc in docs:

            logging.info("正在处理商标，商标名=[{}]，ObjectId=[{}]".format(doc["trademark_name"], str(doc["_id"])))
            
            clean_trademark = {
                "_id": str(doc["_id"]),  
                "name": self.process_str(doc["trademark_name"]), #商标名
                "img_url": doc["img_url"], # 商标图片链接
                "img_base64": str(doc["image_base64"]), # 图片压缩编码
                "application_number": self.process_str(doc["application_number"]), # 申请注册号 
                "application_date": self.process_str(doc["application_date"]), # 申请日期
                "status": self.process_str(doc["status_end"]) if "status_end" in doc else "", # 当前状态
                "history_status": self.process_history_status(doc), # 历史状态，从近到远
                "international_category": self.process_str(doc["international_category"]), # 国际分类    
                "similar_group": self.process_str_list(doc["similar_group"]), # 类似群
                "applicant_name_zh": self.process_str(doc["applicant_name_zh"]), #申请人名称（中文）
                "applicant_name_en": self.process_str(doc["applicant_name_en"]), #申请人名称（英文），公司英文名称，目前共1703条数据，可留 
                "applicant_adddress_zh": self.process_str(doc["applicant_address_zh"]), #申请人地址中文
                "applicant_address_en": self.process_str(doc["applicant_address_en"]), #申请人地址英文，地址英文翻译，不用
                "first_trial_number": self.process_str(doc["first_trial_number"]), #初审公告期号
                "first_trial_date": self.process_str(doc["first_trial_date"]),#初审公告日期
                "registered_number": self.process_str(doc["registered_number"]), #注册公告期号
                "registered_date": self.process_str(doc["registered_date"]),# 注册公告日期
                "is_common": self.process_str(doc["is_common_trademark"]), #是否共有商标
                "type": self.process_str(doc["trademark_type"]), # 商标类型
                "trademark_form": self.process_str(doc["trademark_form"]),# 商标形式
                "international_registration_date": self.process_str(doc["international_registration_date"]),#国际注册日期，该条数据基本没有，29万数据只有3条有数据
                "late_specified_date":self.process_str(doc["late_specified_date"]), #后期指定日期，数据基本没有，只有一条数据
                "priority_date": self.process_str(doc["priority_date"]), #优先权日期，只有102条数据，续期优先权
                "agency": self.process_str(doc["agency"]), # 代理机构
                "service": [ self.process_str(s) for s in doc["product_service"]], #商品/服务              
                "source": self.process_str(doc["source"]),
                "url": doc["url"],
                "html": doc["html"],
                "search_key": self.process_str(doc["search_key"]),
                "crawl_time": doc["crawl_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }

            right_date = self.process_right_date(doc)
            clean_trademark.update(right_date)

            batch_data.append(clean_trademark)
            count += 1

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]家企业信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logging.info("[{}] 企业商标数据清洗完毕，共找到商标[{}]条，跳过无商标号的数据[{}]条，批内去重[{}]条，已存在数据[{}]条，清洗库入库[{}]条".format(
                    crawl_date, total, self.count_ignore, self.count_inner_dupl, self.count_dupl, self.count_insert) )
        self.close_connection()


if __name__ == "__main__":

    # 商标最早爬虫日期为 2020-01-16
    
	cleaner = TrademarkClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
