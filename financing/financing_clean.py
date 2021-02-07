#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-24 16:55
# Filename     : financing_clean.py
# Description  : res_kb_financing融资信息清洗，数据源：清科研究，因果树，鲸准，企查查
# 主要清洗融资轮次，融资金额和字符串、列表的格式化
#******************************************************************************
from urllib.request import urlopen,quote
import json
import logging
from logging.handlers import RotatingFileHandler
import requests
from pymongo import MongoClient
import datetime
import re
import configparser
import sys
from dateutil import parser
import pymysql
import os

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


class FinancingClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_financing = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_financing")] # 融资信息，爬虫库
        self.res_kb_process_financing = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_financing")] # 融资信息，清洗库
        self._init_f_round_schema() # 融资轮次归一
        self._init_currency_schema() # 融资金额归一
        self.count_insert = 0
        self.count_inner_dupl = 0
        self.count_dupl = 0

    def _init_f_round_schema(self):

        self.f_round_schema = {}

        sql_conn = pymysql.connect( host = "xxx" ,
                user = "xxx",
                passwd = "xxx",
                port = 0 ,
                db = "xxx",
                charset = "utf8" )
        sql_cur = sql_conn.cursor() 
        currency_sql = "select name,alter_names from res_invest_round"
        sql_cur.execute(currency_sql)
        for c in sql_cur.fetchall():
            f_round = [c[0]]
            if c[1]:
                f_round.extend(c[1].split("|")) 
            self.f_round_schema[c[0]] = f_round

        sql_cur.close()
        sql_conn.close()


    def _init_currency_schema(self):

        self.currency_world_code = {}

        sql_conn = pymysql.connect( host = "xxx" ,
                user = "xxx",
                passwd = "xxx",
                port = 0 ,
                db = "xxx",
                charset = "utf8" )
        sql_cur = sql_conn.cursor() 
        currency_sql = "select name,alter_names,name_en from res_currency"
        sql_cur.execute(currency_sql)
        for c in sql_cur.fetchall():
            keywords = [c[0],c[2]]
            keywords.extend(c[1].split("|")) 
            keywords = list(set(keywords))
            self.currency_world_code[c[0]] = keywords

        sql_cur.close()
        sql_conn.close()

    def process_financing_type(self,doc):
        '''公司类型mappings'''
        financing_type = doc["financing_type"]
        for t in self.financing_types:
            if financing_type in self.financing_types[t]:
                return t
        return ""

    def process_str(self, s):
        string = ""
        if s:
            try:
                if isinstance(s,list):
                    s = s[0]
                string = s.strip().replace("（","(").replace("）",")")
                string = re.sub("[\n\r\t]","",string)
                string = string.replace("暂无","")
            except Exception as e:
                print("input:",e)
                raise e
        
        return string

    def process_price(self, doc):
        '''价格清洗'''

        capital = {
            "price_amount": self.get_capital_amount(doc["price"]),
            "price_currency": self.get_currency_type(doc["price"]),
        }

        return capital

    def get_capital_amount(self, _doc_str):

        if not _doc_str or not _doc_str.strip().strip('-'):
            return None
        currency_num = None
        han_to_num = {
            "千万":10000000,    
            "百万":1000000,    
            "十万":100000,    
            "万"  :10000,
            "千亿":100000000000,    
            "百亿":10000000000,    
            "十亿":1000000000,    
            "亿"  :100000000
        }
        tmp_currency = _doc_str.strip().replace(',','')
        pat = re.compile(r'\d+[\.]?\d+')#取有效数字，可含小数点
        res = pat.search(tmp_currency)

        if not res:
            for mon_key in han_to_num:
                if tmp_currency.find(mon_key)>-1:
                    currency_num = han_to_num[mon_key]
                    break
            return currency_num
        if res.group(0):
            num = res.group(0)
            if '.' == num[0]:
                return None
        currency_num = float(res.group(0))
        for mon_k in han_to_num:
            if tmp_currency.find(mon_k)>-1:
                currency_num = currency_num*(han_to_num[mon_k])
                break
        return currency_num

    def get_currency_type(self, str_doc):

        if not str_doc or not str_doc.strip().strip('-'):
            return None

        # 常用货币    
        currency_type_key = {
            "人民币元":['￥','人民币','CNY'],
            "美元":['美元','美金',"美币",'USD'],
            "香港元":['香港元','港元','HKD','港币'],
            "英镑":['英镑','GBP'],
            "欧元":['欧元','EUR'],
            "澳大利亚":['澳大利亚元','澳元',"澳币","AUD"],
            "日元":['日元','日币',"JPY"]
        }


        for key in currency_type_key:
            for k_i in currency_type_key[key]:
                if str_doc.find(k_i)>-1:
                    return key
        
        # 数据库加载其他货币
        for cur_code in self.currency_world_code:
            c_key = self.currency_world_code[cur_code]
            for key in c_key:
                if str_doc.find(key)>-1:
                    return cur_code
        
        #if str_doc.find('元')>-1:
        #    return 156
        return None


    def insert_batch(self, batch_data):
        '''根据公司名称和融资轮次去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        uniqe_field_vals = [] # 主公司名称，轮次记录
        unique_ids = [] # 主记录ID，用于批内去重日志显示
        new_batch = []
        for index, data in enumerate(batch_data):
            compare_item = (data["company"], data["finance_date"])
            if compare_item in uniqe_field_vals:
                self.count_inner_dupl += 1 
                logger.info("批内去重，融资记录(轮次=[{}]，公司=[{}]，ObjectId=[{}])被去重，主记录(轮次=[{}]，公司=[{}]，ObjectId=[{}])".format(
                                        data["finance_round"], 
                                        data["company"], 
                                        data["_id"], 
                                        data["finance_round"], 
                                        data["company"], 
                                        unique_ids[uniqe_field_vals.index(compare_item)]
                                    )
                            )
            else:           
                uniqe_field_vals.append( compare_item )
                unique_ids.append(data["_id"])
                new_batch.append(data)

        logger.info("批内去重完成，批内累计去重[{}]条数据".format(self.count_inner_dupl))

        logger.info("批量数据与MongoDB去重中...")
        
        db_dupl = 0
        tmp_batch = []
        for ix , item in enumerate(new_batch):
            dupl_data = self.res_kb_process_financing.find_one({"finance_date":item["finance_date"], "company":item["company"]})
            if dupl_data:
                logger.info("融资清洗库已存在数据去重，记录(轮次=[{}]，公司=[{}]，ObjectId=[{}])被去重，主记录(轮次=[{}]，公司=[{}]，ObjectId=[{}])".format(
                                        item["finance_round"], 
                                        item["company"], 
                                        item["_id"], 
                                        dupl_data["finance_round"], 
                                        dupl_data["company"], 
                                        dupl_data["_id"])
                            )
                new_batch.pop(ix - db_dupl)
                db_dupl += 1
                self.count_dupl += 1
            else:
                tmp_batch.append(item)

        #if not new_batch:
        if not tmp_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_financing.insert_many(tmp_batch)
        self.count_insert += len(insert_res.inserted_ids)
        logger.info("批量数据写入完成")
            

        

    def process_f_round(self, doc):

        f_round = doc["financing_round"]
        if f_round:
            f_round = f_round.upper()
        for key in self.f_round_schema:
            for word in self.f_round_schema[key]:
                if word == f_round:
                    return key

        # 如果没有匹配到，返回空
        return ""

    def process_str_list(self, p):

        if isinstance(p,list):
            p = p[0]
        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t ]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list

    def process_investor(self, doc):
        '''投资方字段处理，移除无数据的值'''
        investors = []
        if type(doc["investor"]) == list:
            for investor in doc["investor"]:
                investor = self.process_str(investor)
                if investor in ["未透露","投资方未知","投资方未透露"]:
                    continue
                elif not investor or isinstance(investor,list) or len(investor.strip())==1:
                    continue
                else:
                    investors.append(investor)
        else:
            logger.error("企业投资方数据格式错误，企业名=[{}]，ObjectId=[{}]".format(doc["company_name"], str(doc["_id"])))

        return investors
        


    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于process_date以后的融资数据
        '''

        count = 0
        
        if crawl_date == "yesterday":
            crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date
            
        else:
            raise Exception("无效参数")
        
        iso_date_str = crawl_date + 'T00:00:00+08:00'
        #iso_lte_date = parser.parse('2019-04-01' + 'T00:00:00+08:00')
        iso_date = parser.parse(iso_date_str)
        #res = self.res_kb_financing.find({'crawl_time': {'$gte': iso_date,'$lt': iso_lte_date}}).sort([("crawl_time",1)])
        res = self.res_kb_financing.find({'crawl_time': {'$gte': iso_date}}).sort([("crawl_time",1)])
        # 余量融资信息迁移
        # res = self.res_kb_financing.find({"create_time":{'$gte': iso_date},"crawl_time":{"$gte":parser.parse("2019-10-29T03:30:56+08:00")}}).sort([("crawl_time",1)])
        total = res.count()

        batch_data = []
       
        for doc in res:        
            if self.res_kb_process_financing.find_one({'_id':str(doc['_id'])}):
                continue
            logger.info("正在处理融资，公司名=[{}]，ObjectId=[{}]".format(doc["company_name"], str(doc["_id"])))

            update_doc = {
                "_id": str(doc["_id"]), # 直接用ObjectId的值作为后续统一的ID值
                "url": doc["url"],
                "industry": self.process_str(doc["industry"]),
                "investors": self.process_investor(doc),
                "finance_round": self.process_f_round(doc),
                "company_tag": self.process_str_list(doc["company_tag"]) if "company_tag" in doc and doc["company_tag"] else "",
                "finance_date": doc["financing_date"],
                "html": doc["html"],
                "crawl_time": doc["crawl_time"],
                "source": doc["source"],
                "product_name":self.process_str(doc["product_name"]),
                "news": self.process_str(doc["news"]),
                "company": self.process_str(doc["company_name"]),
                "stock_proportion": doc["stock_proportion"] if "stock_proportion" in doc else "",
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }

            capitals = self.process_price(doc)
            update_doc.update(capitals)          
            batch_data.append(update_doc)
            count += 1

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]家融资信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

            


        logger.info("融资爬虫数据处理完毕，找到[{}]个融资数据，其中批内去重[{}]个，清洗库已有记录[{}]个，写入清洗库[{}]个融资".format(
                        total, self.count_inner_dupl, self.count_dupl, self.count_insert ))


if __name__ == "__main__":

    # 最早日期  2019-10-23
    
	cleaner = FinancingClean()

	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
