#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-13 16:09
# Filename     : product_clean.py
# Description  : res_kb_product企业产品信息清洗，过滤掉无产品名或产品名为公司的数据，主要是值的清洗和字段按新schema数据格式组织重新插入
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

class ProductClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_product = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_product")] 
        self.res_kb_process_product = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_product")] # 产品清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
     

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            string = s.replace("（","(").replace("）",")").replace("\r","")
            string = string.strip("-").strip()  # 产品名字段特殊处理，去掉开头的-符号
        return string
            

    def process_date(self,datetime_str):
        '''
        日期清洗，将日期字符串格式转为ISODate格式存储
        '''
        if not datetime_str: # like ""
            return None
        if type(datetime_str)== datetime.datetime: # some like datetime.datetime type
            datetime_str = str(datetime_str)[:19]
            
        tmp = datetime_str.split(" ")
        d = ""
        if len(tmp) == 1:
            tmp1 = re.split("[-./]",tmp[0])

            if len(tmp1) == 3: # like 2020-03-25 or 2020/03/27
                if "-" in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y-%m-%d")
                elif "." in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y.%m.%d")
                elif "/" in tmp[0]:
                    d = datetime.datetime.strptime(tmp[0], "%Y/%m/%d")

            elif len(tmp1) == 4: # like 2020-03-25-10:3
                d = datetime.datetime.strptime('-'.join(tmp1[:3]) + ' ' + tmp1[3], "%Y-%m-%d %H:%M")
            else:
                d = datetime_str
                
        elif len(tmp) == 2:
            
            if len(re.split("[-/]",tmp[0])) == 3:
                d1 = tmp[0].replace("/","-")
            else:
                d1 = tmp[0]
            if len(tmp[1].split(':')) == 2:
                d = datetime.datetime.strptime(d1 + ' ' + tmp[1], "%Y-%m-%d %H:%M")
            elif len(tmp[1].split(':')) == 3:
                d = datetime.datetime.strptime(d1 + ' ' + tmp[1], "%Y-%m-%d %H:%M:%S")
                
        else:
            d = datetime_str
        if type(d) == datetime.datetime:
            return d
        return parser.parse(d)



    def insert_batch(self, batch_data):
        '''过滤无效数据，根据产品名称和公司名称去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        uniqe_field_vals = [] # 产品名称，公司名称记录
        new_batch = [] # 产品数据
    
        for data in batch_data:

            if (not data["name"] or data["name"] == "-"):
                self.count_ignore += 1
                logging.info("跳过无产品名的数据，ObjectId=[{}]".format(data["_id"]))
                continue

            if (data["name"].endswith("公司") or 
                data["name"].endswith("网站") or 
                data["name"].endswith("集团") or 
                "官网" in data["name"]  
                or ( data["name"].endswith("首页") and len(data["introduction"])<50 )):
                self.count_ignore += 1
                logging.info("跳过产品名为公司的数据，产品名=[{}]，ObjectId=[{}]".format(data["name"], data["_id"]))
                continue


            compare_item = [data["name"], data["company_name"]]
            if compare_item in uniqe_field_vals:
                self.count_inner_dupl += 1 
                exist_index = uniqe_field_vals.index(compare_item)
                logger.info("批内去重，产品(名称=[{}]，公司=[{}]，ObjectId=[{}])被去重，主产品(名称=[{}]，公司=[{}]，ObjectId=[{}])".format(
                    data["name"],
                    data["company_name"],
                    data["_id"],
                    new_batch[exist_index]["name"],
                    new_batch[exist_index]["company_name"],
                    new_batch[exist_index]["_id"])
                )
            else:           
                uniqe_field_vals.append( compare_item )
                new_batch.append(data)

        logger.info("批量数据与MongoDB去重中...")
        
        db_dupl = 0
        insert_new_batch = []
        for item in copy.deepcopy(new_batch):
            dupl_data = self.res_kb_process_product.find_one({"name":item["name"], "company_name":item["company_name"]})
            if dupl_data:
                logger.info("产品清洗库已存在数据去重，产品(名称=[{}]，公司=[{}]，ObjectId=[{}])被去重，主产品(名称=[{}]，公司=[{}]，ObjectId=[{}])".format(
                                        item["name"], 
                                        item["company_name"], 
                                        item["_id"], 
                                        dupl_data["name"], 
                                        dupl_data["company_name"],
                                        dupl_data["_id"]))
                continue 
                db_dupl += 1
                self.count_dupl += 1
            else:
                insert_new_batch.append(item)

        if not insert_new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_product.insert_many(insert_new_batch)
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
        res = self.res_kb_product.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        # res = self.res_kb_product.find({"create_time":{"$gte": iso_date},"crawl_time":{"$gte":parser.parse("2019-12-27T07:23:12+08:00")}}).sort([("crawl_time",1)])
        # res = self.res_kb_product.find({}).sort([("crawl_time",1)])
        return res 


    def company_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"necar_companys.txt"), "r") as fr:
            companys_read = fr.readlines()       
        companys = list(map(lambda x:x.strip(), companys_read))
        return companys
        

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的产品数据
        '''

        count = 0

        res = self.query_daily_data(crawl_date)
        total = res.count()

        # 手动执行步骤
        # res = self.company_from_file()
        # total = len(res)

        batch_data = []
        
        for doc in res:

        # 手动执行步骤
        # for name in res:
            
        #     exist = self.res_kb_process_product.find_one({"company_name": name})
        #     if exist:
        #         continue

        #     # 匹配中英文符号公司名称
        #     docs = self.res_kb_product.find({"company_name":name})
        #     if not docs:
        #         zn_name = name.replace("(","（").replace(")","）")
        #         docs = self.res_kb_product.find({"company_name":zn_name})

        #     for doc in docs: 

            logging.info("正在处理产品，产品名=[{}]，ObjectId=[{}]".format(doc["product_name"], str(doc["_id"])))


            clean_product = {
                "_id": str(doc["_id"]), 
                "name": self.process_str(doc["product_name"]),
                "introduction": self.process_str(doc["introduction"]),
                "company_name": self.process_str(doc["company_name"]),
                "url": doc["url"] if doc["url"] else "",
                "product_url": doc["product_url"] if doc["product_url"] else "",
                "source": doc["source"] if doc["source"] else "",
                "html": doc["html"] if ("html" in doc and doc["html"]) else "",  # 部分数据无html的key
                "crawl_time": doc["crawl_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }

        
            batch_data.append(clean_product)
            count += 1

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]条产品数据".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logging.info("[{}] 企业产品数据清洗完毕，共找到产品[{}]条，跳过无效产品数据[{}]条，批内去重[{}]条，已存在数据[{}]条，清洗库入库[{}]条".format(
                    crawl_date, total, self.count_ignore, self.count_inner_dupl, self.count_dupl, self.count_insert) )


if __name__ == "__main__":

    # 产品最早爬虫日期为 2019-12-26
    
	cleaner = ProductClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
