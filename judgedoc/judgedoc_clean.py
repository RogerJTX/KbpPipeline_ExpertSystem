#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-05-13 16:09
# Filename     : judgedoc_clean.py
# Description  : res_kb_judgedoc企业裁判文书信息清洗，主要是值的清洗和日期转换
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

class JudgedocClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_judgedoc = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_judgedoc")] 
        self.res_kb_process_judgedoc = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_judgedoc")] # 裁判文书清洗库
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
            elif isinstance(s,list):
                s = s[0]
            string = s.replace("（","(").replace("）",")").replace("\r","")
        return string


    def process_str_list(self, p):

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t ]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list

    def process_case_amount(self, doc):
        '''把××元转化为金额和币种，这里都为人民币元的币种'''
        res = {
            "case_amount":"",
            "case_amount_currency":""
        }
        case_amount = doc["case_amount"]
        match = re.search("^[\d.]+元$",case_amount)
        if match:
            res["case_amount_currency"] = "人民币元"
            res["case_amount"] = case_amount[:-1]

        return res

    def char2num(self, string):
        '''将年份月份日期的文字转化为数字形式，比如   二十二  转为 22， 一九七八转为1978'''
    
        date_map = {
                '零':"0",
                '一':"1",
                '二':"2",
                '三':"3",
                '四':"4",
                '五':"5",
                '六':"6",
                '七':"7",
                '八':"8",
                '九':"9"
            }
        
        if len(string) == 4:
            for c in date_map:
                string = string.replace(c, date_map[c])       
        if len(string) == 3:
            for c in date_map:
                string = string.replace(c, date_map[c]).replace("十","")
        elif len(string) == 2:
            for c in date_map:
                string = string.replace(c, date_map[c]).replace("十","1")
        elif len(string)== 1:
            if string == "十":
                string = "10"
            else:
                string = "0" + date_map[string]
                
        return string

    def process_judgment_date(self, doc):
        '''将文字格式的时间格式化，比如 二零一八年十月九日 转为 2018-10-09 '''
        res = ""
        judgment_date = doc["judgment_date"]
        if "年" in judgment_date and "月" in judgment_date and "日" in judgment_date:
            splits = judgment_date.split("年")
            if len(splits) == 2:
                year, remain = splits
                month, remain= remain.split("月")
                day = remain.split("日")[0]
                res = self.char2num(year) + "-" + self.char2num(month) + "-" + self.char2num(day)
        return res



 

    def insert_batch(self, batch_data):
        '''根据裁决文书编号去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        case_nums = []
        new_batch = []

        for data in batch_data:
            
            if not data["case_num"]:
                self.count_ignore += 1
                #logging.info("跳过裁判文书，缺少裁决文书编号。裁判文书ObjectId=[{}]".format(data["_id"]))
                continue
            case_num = data["case_num"]
            if case_num in case_nums:
                self.count_inner_dupl += 1
                continue 

            new_batch.append(data)
            case_nums.append(case_num)

        logger.info("批量数据批内去重，组内去重[{}]条数据".format(self.count_inner_dupl))

        logger.info("批量数据与MongoDB去重中...")
        
        dupl_datas = self.res_kb_process_judgedoc.find({"case_num":{"$in":case_nums}})
        for dupl_data in dupl_datas:
            index = case_nums.index(dupl_data["case_num"])
            case_nums.pop(index)
            new_batch.pop(index)
            self.count_dupl += 1

        if not new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_judgedoc.insert_many(new_batch)
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
        res = self.res_kb_judgedoc.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        return res


    def company_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"ai_companys.txt"), "r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

            

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的裁判文书数据
        '''

        count = 0
        res = self.query_daily_data(crawl_date)
        total = res.count()

        # 手动执行步骤
        # res = self.company_from_file()
        # total = len(res)

        batch_data = []
        
        for doc in res:

            count += 1
            #logging.info("正在处理裁判文书，裁判文书名=[{}]，ObjectId=[{}]".format(doc["case_name"], str(doc["_id"])))

            clean_judgedoc = {
                "_id": str(doc["_id"]),
                "case_num": self.process_str(doc["case_num"]),
                "case_name": self.process_str(doc["case_name"]),
                "instrument_type": self.process_str(doc["instrument_type"]),
                "company_name": self.process_str(doc["company_name"]),
                "party_information": [self.process_str(i) for i in doc["party_information"]],
                "trial": self.process_str(doc["trial"]),
                "search_key": self.process_str(doc["search_key"]),
                "court_think": self.process_str(doc["court_think"]),
                "tag": self.process_str(doc["tag"]),
                "executive_court": self.process_str(doc["executive_court"]),
                "publish_date": doc["publish_date"],
                "case": self.process_str(doc["case"]),
                "judgment_result": self.process_str(doc["judgment_result"]),
                "judgment_date": self.process_judgment_date(doc),
                "source": self.process_str(doc["source"]),
                "url": doc["url"] if "url" in doc and doc["url"] else "",
                "html": doc["html"] if "html" in doc and doc["html"] else "",
                "crawl_time": doc["crawl_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }

            case_amount_res = self.process_case_amount(doc)
            clean_judgedoc.update(case_amount_res)
    
            batch_data.append(clean_judgedoc)

            # MongoDB批量写入
            if count % 500 == 0 or count == total:
                logger.info("正在写入前[{}]家企业信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logging.info("[{}] 企业裁判文书数据清洗完毕，共找到裁判文书[{}]条，跳过无裁判文书号的数据[{}]条，批内去重[{}]条，已存在数据[{}]条，清洗库入库[{}]条".format(
                    crawl_date, total, self.count_ignore, self.count_inner_dupl, self.count_dupl, self.count_insert) )


if __name__ == "__main__":

    # 裁判文书最早爬虫日期为 2020-02-19
    
	cleaner = JudgedocClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
