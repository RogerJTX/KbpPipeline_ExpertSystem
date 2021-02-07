#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-29 16:09
# Filename     : patent_clean.py
# Description  : res_kb_patent企业专利信息清洗，主要是值的清洗和字段按新schema数据格式组织重新插入
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

class PatentClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_patent = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_patent")] 
        self.res_kb_process_patent = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_patent")] # 专利清洗库
        self.count_ignore = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0
     

    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        if s:
            string = s.replace("（","(").replace("）",")").replace("\r","")
        return string


    def process_str_list(self, p):

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t ]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list

    def process_ipc(self, doc):
        '''
        专利类别
        '''
        ipc_str = doc["ipc_category"]

        if not ipc_str:
            if "meta" in doc:
                for key in doc["meta"]:
                    if "国际专利分类号" in key:
                        ipc_str = doc["meta"][key]
            
        ipc = self.process_str_list(ipc_str)
        return ipc
            

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
        try:
            d = parser.parse(d)
        except Exception as e:
            logger.info("日期无法解析，日期=[{}]".format(d))
            return ""
        return d

    def process_require(self, doc):
        '''主权项解析成list'''

        requires = []
        if "meta" in doc:
            meta_data = doc["meta"]
            for key in meta_data:
                if key == "claims":
                    requires.extend(meta_data[key])
                    break
                elif "主权项" in key:
                    requires.extend(meta_data[key].split("；"))
                    break
    
        return requires

    def process_agency(self, doc):
        '''解析专利代理机构名称和代码'''
        agency = {
            "agency":"",
            "agency_code":""
        }
        if doc["agency"]:
            temp_agency = doc['agency'].split(' ')
            agency['agency'] = temp_agency[0]
            if len(temp_agency)>1:
                agency['agency_code'] = temp_agency[1].replace(";","")

        return agency

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
        '''过滤无效数据，根据专利号去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        public_codes = []
        new_batch = []

        for data in batch_data:

            if (not data["publish_code"]) or (not data["publish_date"]):
                self.count_ignore += 1
                logging.info("跳过专利，缺少有效信息。专利名=[{}]".format(data["name"]))
                continue

            public_code = data["publish_code"]
            if public_code in public_codes:
                pos = public_codes.index(public_code)

                # 关注abstract字段，如果新数据有，而旧数据没有，则进行更新替换
                exist_data = new_batch[pos]
                if data["abstract"] and ( not exist_data["abstract"]):
                    new_batch[pos] = data
                    
                self.count_inner_dupl += 1
                continue 
            new_batch.append(data)
            public_codes.append(public_code)

        logger.info("批量数据批内去重，组内去重[{}]条数据".format(self.count_inner_dupl))

        logger.info("批量数据与MongoDB去重中...")
        
        dupl_datas = self.res_kb_process_patent.find({"publish_code":{"$in":public_codes}})
        for dupl_data in dupl_datas:
            index = public_codes.index(dupl_data["publish_code"])

            # 关注abstract字段，如果新数据有，而旧数据没有，则进行更新替换
            coming_data = new_batch[index]
            if coming_data["abstract"] and ( not dupl_data["abstract"]):
                coming_data.pop("_id")
                self.res_kb_process_patent.update_one({"_id":dupl_data["_id"]},{"$set":coming_data})

            public_codes.pop(index)
            new_batch.pop(index)
            self.count_dupl += 1

        if not new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_patent.insert_many(new_batch)
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
        res = self.res_kb_patent.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        # 剩余数据全量导入
        # res = self.res_kb_patent.find({'create_time': {'$gte': iso_date},'crawl_time':{'$gte':parser.parse("2019-07-26T04:38:12+08:00")}},no_cursor_timeout=True).sort([("crawl_time",1)])
        return res


    def company_from_file(self):
        '''读取候选企业'''
        with open(os.path.join(kbp_path,"necar_companys.txt"), "r") as fr:
            companys_read = fr.readlines()
        
        companys = list(map(lambda x:x.strip(), companys_read))

        return companys

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

            

    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于crawl_date以后的专利数据
        '''

        count = 0
        res = self.query_daily_data(crawl_date)
        total = res.count()

        # 手动执行步骤
        # res = self.company_from_file()
        # total = len(res)

        batch_data = []
        
        for doc in res:

        #手动执行步骤
        # for name in res:
            
        #     exist = self.res_kb_process_patent.find_one({"search_key": name})
        #     if exist:
        #         continue

            
        #     docs = self.res_kb_patent.find({"search_key":name})
        #     if not docs:
        #         zn_name = name.replace("(","（").replace(")","）")
        #         docs = self.res_kb_patent.find({"search_key":zn_name})

        #     for doc in docs:

            logging.info("正在处理专利，专利名=[{}]，ObjectId=[{}]".format(doc["patent_name"], str(doc["_id"])))

            clean_patent = {
                "_id": str(doc["_id"]),  
                "name": self.process_str(doc["patent_name"]),
                "type": self.process_str(doc["patent_type"]),
                "publish_code": doc["public_id"],
                "publish_date": self.process_date(doc["public_date"]),
                "apply_code": doc["apply_number"],
                "apply_date": self.process_date(doc["apply_date"]),
                "inventors": self.process_str_list(doc["inventor"]),
                "applicant": self.process_str_list(doc["applicant"]),
                "agent": self.process_str_list(doc["agent"]),
                "ipc_category": self.process_ipc(doc),
                "abstract": self.process_str(doc["abstract"]),
                "legal_status": doc["law_status"],
                "require": self.process_require(doc),
                "priority": self.process_str(doc["priority_id"]),
                "content": self.process_str(doc["content"]),
                "source": self.process_str(doc["source"]),
                "url": doc["url"],
                "html": doc["html"],
                "search_key": self.process_str(doc["search_key"]),  # search_key也作为找专利关联公司的依据
                "crawl_time": doc["crawl_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }

            agency = self.process_agency(doc)
            clean_patent.update(agency)

            applicant_addr = self.process_addr(doc)
            clean_patent.update(applicant_addr)

        
            batch_data.append(clean_patent)
            count += 1

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]条专利信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logging.info("[{}] 企业专利数据清洗完毕，共找到专利[{}]条，跳过无专利号的数据[{}]条，批内去重[{}]条，已存在数据[{}]条，清洗库入库[{}]条".format(
                    crawl_date, total, self.count_ignore, self.count_inner_dupl, self.count_dupl, self.count_insert) )
        self.close_connection()


if __name__ == "__main__":

    # 专利最早爬虫日期为 2019-05-24
    
	cleaner = PatentClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
