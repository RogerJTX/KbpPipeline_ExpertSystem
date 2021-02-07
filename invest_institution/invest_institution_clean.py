#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-09 10:55
# Filename     : invest_institution_clean.py
# Description  : res_kb_invest_institution投资机构信息清洗
# 主要是添加投资机构的label、中英文别称、曾用名数组化和省市区经纬度地理信息
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
import uuid
import pymysql
import os

main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_path)

from common_utils.address_parser import AddressParser

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

def set_log():
    logging.basicConfig(level=logging.INFO) 
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"invest_institution_log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)


class InvestInstitutionClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_invest_institution = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_invest_institution")] # 投资机构信息，爬虫库
        self.res_kb_process_invest_institution = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_invest_institution")] # 投资机构信息，清洗库
        # 投资机构tags标签，人工整理
        self.sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                user = self.config.get("mysql","user"),
                passwd = self.config.get("mysql","passwd"),
                port = self.config.getint("mysql","port") ,
                db = self.config.get("mysql","db"),
                charset = self.config.get("mysql","charset") )
        self.sql_cur = self.sql_conn.cursor() 
        self.address_parser = AddressParser()
        self.alter_url = self.config.get("url","alter_name")
        self._init_currency_schema()
        self._init_company_type_schema()
        self._init_f_round_schema() # 融资轮次归一
        self.count_dupl_company = 0
        self.count_insert = 0
        self.count_inner_dupl = 0

    def _init_f_round_schema(self):

        self.f_round_schema = {}
        self.f_round_sort = {}

        sql_conn = pymysql.connect( host = "xxxx" ,
                user = "xxxx",
                passwd = "xxxx",
                port = 0000 ,
                db = "xxxx",
                charset = "xxxx" )
        sql_cur = sql_conn.cursor() 
        currency_sql = "select name,alter_names,sequence from res_invest_round where sequence is not null"
        sql_cur.execute(currency_sql)
        for c in sql_cur.fetchall():
            f_round = [c[0]]
            self.f_round_sort[c[0]] = c[2]
            if c[1]:
                f_round.extend(c[1].split("|")) 
            self.f_round_schema[c[0]] = f_round

        sql_cur.close()
        sql_conn.close()

    def _init_currency_schema(self):

        self.currency_world_code = {}
        currency_sql = "select name,alter_names,name_en from res_currency"
        self.sql_cur.execute(currency_sql)
        for c in self.sql_cur.fetchall():
            keywords = [c[0],c[2]]
            keywords.extend(c[1].split("|")) 
            keywords = list(set(keywords))
            self.currency_world_code[c[0]] = keywords

    def _init_company_type_schema(self):

        self.company_types = {}
        sql = "select name,alter_names from res_company_type"
        self.sql_cur.execute(sql)
        for c in self.sql_cur.fetchall():
            keywords = c[1].split("|")
            self.company_types[c[0]] = keywords


    def process_company_type(self,doc):
        '''公司类型mappings'''
        company_type = doc["company_type"]
        for t in self.company_types:
            if company_type in self.company_types[t]:
                return t
        return ""
        

    def process_f_round(self, labels):

        res_key = ''
        if labels:
            f_rounds = [f_round.upper() for f_round in labels]
        else:
            return res_key
        rount_num = -1
        for key in self.f_round_schema:
            for word in self.f_round_schema[key]:
                if word in f_rounds:
                    if rount_num < self.f_round_sort[key]:
                        rount_num = self.f_round_sort[key]
                        res_key = key

        # 如果没有匹配到，返回空
        return res_key

    def process_alter_name(self, doc):
        '''
        根据公司全称生成别称及英文别称
        '''

        alter_names = []

        # 可能的英文别称
        if doc["description"]:
            alter_names.extend([ alter.lower() for alter in re.findall("^[A-Za-z]+",doc["description"])])
        if alter_names:
            # 去除从介绍中抽取的一个字母的数据
            alter_names = list(filter(lambda x:len(x)>1,alter_names))

        # 中文别称生成
        name = doc["company_name"].replace('（','(').replace('）',')')
        post_data = {
            "full_name": name
        }
        try:
            res = requests.post(self.alter_url, data=json.dumps(post_data))

            if res.status_code == 200:
                alter_names.extend(res.json().get("body"))
        except Exception as e:
            logger.error("公司中文别称生成异常，公司名=[{}]".format(name),e)

        return alter_names


    def process_used_name(self,doc):
        
        used_names = []

        used_name = doc["used_name"]
        if not used_name:
            return used_names

        elif used_name == '-':
            return used_names

        elif len(used_name)>1:
            used_names.extend(list(map(lambda x: x.strip(), re.split(" ", used_name))))

        else:
            logger.warning("used_names格式异常，used_names=[{}]，公司名=[{}]".format(used_name, doc["name"]))

        # remove blank string, such as ["123","","456"]
        used_names = list(filter(lambda x: len(x)>0, used_names))
        return used_names

    def process_tags(self, doc):
        '''
        根据人工整理的公司标签表添加tags
        '''
        tags = []
        company_name = doc["company_name"].replace("（","(").replace("）",")")
        if company_name in self.company_tags_schema:
            tags = self.company_tags_schema[company_name]
        return tags

    def process_labels(self,doc):
        '''
        爬虫爬取时带的标签标记清洗
        '''
        labels = []
        label = doc["label"]
        if label:
            if "" in label:
                label.remove("")
            labels = list(map(lambda x: re.sub("[| ]","",x),label))
        return labels

    def process_publish_time(self, _doc_str):
        ''' '''

        if not _doc_str or not _doc_str.strip():
            return None
        pat = re.compile(r'(\d{4})[年-](\d{2})[月-](\d{2})[日 ]?')
        res = pat.search(_doc_str.strip())

        if not res:
            return None
        return res.group(1)+'-'+res.group(2)+'-'+res.group(3)

    def process_str(self, s):
        string = ""
        if s:
            string = re.sub("[ \t\r]","", s.strip()).replace("（","(").replace("）",")")
            string = string.replace("暂无","")
        
        return string

    def process_business_term(self, doc):
        business_terms = {}
        time_term = doc["business_term"]
        if time_term:
            seg_str = ['至']
            for seg in seg_str:
                if time_term.find(seg)>-1:
                    from_t = ''
                    to_t = ''
                    from_t,to_t = time_term.split(seg)
                    from_t, to_t = from_t.strip(), to_t.strip()
                    if not self.is_date_type(from_t):
                        from_t = None
                    if not self.is_date_type(to_t):
                        to_t = None
                    business_terms["from_date"] = from_t
                    business_terms["to_date"] = to_t
        return business_terms

    def is_date_type(self, _time_str):
        ''' 判断是否符合日期格式'''

        pat = re.search(r"^(\d{4}-\d{1,2}-\d{1,2})$",_time_str)
        if pat:
            return True
        return False

    def process_capital(self, doc):
        '''注册金额和实付金额 货币清洗'''

        capital = {
            "reg_capital": self.get_capital_amount(doc["registered_capital"]),
            "reg_capital_currency": self.get_currency_type(doc["registered_capital"]),
            "paid_capital": self.get_capital_amount(doc["paid_capital"]),
            "paid_capital_currency": self.get_currency_type(doc["paid_capital"])
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
        return float(currency_num)

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

    def process_leader_position(self, doc):
        ''' 获取高管职业列表'''

        leaders = doc["leader"]

        lead_keys = [
            ["代表","首席代表","一般代表"],
            ["负责人"],
            ["副董事长","执行董事","独立董事","董事长","董事"],
            ["副总经理","总经理","经理"],
            ["监事会主席","监事主席","职工监事","监事长","监事"]
        ]

        for leader in leaders:
            if not leader:
                continue
            pos = leader["position"]
            leader_pos = []    
            for lay in range(len(lead_keys)):
                for key in lead_keys[lay]:
                    if key in pos:
                        leader_pos.append(key)
                        break
            leader["position"] = leader_pos

        return leaders

    def process_register_status(self, doc):
        '''营业状态：吊销和存续状态归一'''

        reg_status = doc["register_status"]
        if reg_status:
            if "吊销" in reg_status:
                reg_status = "吊销"
            elif re.search("开业|在业|存续", reg_status):
                reg_status = "存续（在营、开业、在册）"
        else:
            reg_status = ""

        return reg_status

    def insert_batch(self,batch_data):
        '''根据公司全称去重，并写入'''

        logger.info("批量数据组装完成，准备去重...")
        company_names = []
        new_batch = []
        for data in batch_data:
            if not data["crawl_time"]: # 旧数据中有部分研究机构数据不完整，且爬虫时间均为空，过滤掉
                logger.info("跳过数据，ObjectId=[{}]，缺少必须字段crawl_time".format(data["_id"]))
                self.count_skip += 1
                continue 
            if data["name"] in company_names:

                # 关注投资机构简介的字段是否完整，不完整选取有值的进行替换
                exist_index = company_names.index(data["name"])
                exist_data = new_batch[exist_index]
                if data["desc"] and ( not exist_data["desc"]):
                    new_batch[exist_index] = data

                self.count_inner_dupl += 1
                continue
            else:
                company_names.append(data["name"])
                new_batch.append(data)
        
        dupl_datas = self.res_kb_process_invest_institution.find({"name":{"$in":company_names}})
        for dupl_data in dupl_datas:
            index = company_names.index(dupl_data["name"])

            # 关注投资机构简介的字段是否完整，不完整选取有值的进行更新
            coming_data = new_batch[index]
            if coming_data["desc"] and (not dupl_data["desc"]):
                coming_data.pop("_id") # ID不换，数据更新
                self.res_kb_process_invest_institution.update_one({"_id":dupl_data["_id"]},{"$set":coming_data})

            company_names.pop(index)
            new_batch.pop(index)
            self.count_dupl_company += 1

        if not new_batch:
            logger.info("去重完成，无数据写入")
            return

        logger.info("去重完成，准备写入...")
        insert_res = self.res_kb_process_invest_institution.insert_many(new_batch)
        self.count_insert += len(insert_res.inserted_ids)
        logger.info("批量数据写入完成")

    def query_datas(self, crawl_date):

        if crawl_date == "yesterday":
            crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date
            
        else:
            raise Exception("无效参数")
        
        self.process_date = crawl_date
        iso_date_str = crawl_date + 'T00:00:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_invest_institution.find({'crawl_time': {'$gte': iso_date}}).sort([("_id",1)])
        # res = self.res_kb_invest_institution.find({'create_time': {'$gte': iso_date}},no_cursor_timeout=True).sort([("crawl_time",1)])
        return res 

    def query_from_file(self):
        '''读取候选投资机构'''
        with open(os.path.join(kbp_path,"remain20200527.txt"), "r") as fr:
            invest_institutions_read = fr.readlines()
        
        invest_institutions = list(map(lambda x:x.strip(), invest_institutions_read))

        return invest_institutions

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

        



    def process(self, crawl_date):
        '''
        清洗爬虫时间大于等于process_date以后的投资机构数据
        '''
        count = 0
        
        res = self.query_datas(crawl_date)
        total = res.count()
        logger.info("日期[{}]，开始清洗投资机构数据，共[{}]条".format(self.process_date, total))

        # 手动执行步骤
        # res = self.query_from_file()
        # total = len(res)

        batch_data = []
       
        # 手动执行步骤
        # for name in res:    

        #     doc = self.res_kb_invest_institution.find_one({"company_name": name})
        #     if not doc:
        #         doc = self.res_kb_invest_institution.find_one({"company_name":name.replace("(","（").replace(")","）")})    

        for doc in res:

            count += 1
            logger.info("正在处理投资机构，投资机构名=[{}]".format(doc["company_name"]))

            update_doc = {
                "_id": str(doc["_id"]), # 直接用ObjectId的值作为后续统一的ID值
                # "uuid": str(uuid.uuid1()),
                "name": doc["company_name"].replace('（','(').replace('）',')'),
                "name_en": doc["name_en"],              
                "alter_names": self.process_alter_name(doc),
                "used_names": self.process_used_name(doc),
                "labels": self.process_labels(doc),
                # "tags": self.process_tags(doc), # 投资机构人工梳理标签添加，不在清洗时处理，改为relation的时候将这些人工梳理的标签添加到关系中；
                "address": self.process_str(doc["address"]),
                "establish_date": self.process_publish_time(doc["establish_date"]),
                "approve_date": self.process_publish_time(doc["approve_date"]),
                "email": self.process_str(doc["email"]),
                "website": self.process_str(doc["website"]),
                "logo_img": doc["img_logo"],
                "business_scope":self.process_str(doc["business_scope"]),
                "desc": self.process_str(doc["description"]),
                "registration": self.process_str(doc["registration"]),
                "registration_id": self.process_str(doc["registration_id"]),
                "register_status": self.process_register_status(doc),
                "profession": self.process_str(doc["profession"]),
                "social_credit_code": self.process_str(doc["social_credit_code"]),
                "organization_code": doc["organization_code"],
                "legal_person": self.process_str(doc["legal_representative"]),
                "tax_code": self.process_str(doc["taxpayer_id"]),
                "insured_number": doc["insured_number"],
                "url": doc["url"],
                "source": doc["source"],
                "html":doc["html"],
                "crawl_time": doc["crawl_time"],
                "tag": doc["tag"],
                "search_key": doc["search_key"].strip(),
                "company_type": self.process_company_type(doc),
                "leaders": self.process_leader_position(doc),
                "employee_size":self.process_str(doc["staff_size"]),
                "phone": self.process_str(doc["phone"]),
                "shareholder": doc["shareholder"],
                "financing_round":self.process_f_round(doc["label"]),
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }


            addr_query = {
                "name": update_doc["name"],
                "province": doc["area"],
                "address": update_doc["address"]
            }
            address_res = self.address_parser.parse(addr_query)
            update_doc.update(address_res)

            business_terms = self.process_business_term(doc)
            update_doc.update(business_terms)

            capitals = self.process_capital(doc)
            update_doc.update(capitals)          
            batch_data.append(update_doc)
            

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                logger.info("正在写入前[{}]家投资机构信息".format(count))
                self.insert_batch(batch_data)
                batch_data = []

        logger.info("投资机构爬虫数据处理完毕，找到[{}]个投资机构数据，其中批内去重[{}]个，清洗库已有记录[{}]个，写入清洗库[{}]个投资机构".format(
                        total, self.count_inner_dupl, self.count_dupl_company, self.count_insert ))
        self.close_connection()
        

if __name__ == "__main__":

    # 最早日期  2019-07-03
    
	cleaner = InvestInstitutionClean()

	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
