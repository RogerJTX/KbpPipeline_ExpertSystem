#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-05-08 17:28
# Filename     : expert_clean.py
# Description  : res_kb_expert专家信息清洗逻辑： 
# 1 book,patent,journal_article,tech_achievement都没有数据的记录信息不足，过滤掉 
# 2 去重按照规范化后的姓名和生日去重(性别数据太少，不采用)； 
# 3 专利等数据空值处理  
# 4 教育经历起止日期、出生日期、职称、学历等规范化
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
import pymysql


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

class ExpertClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_expert = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_expert")] 
        self.res_kb_process_expert = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_expert")] # 专家清洗库
        self.sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                user = self.config.get("mysql","user"),
                passwd = self.config.get("mysql","passwd"),
                port = self.config.getint("mysql","port") ,
                db = self.config.get("mysql","db"),
                charset = self.config.get("mysql","charset") )
        self.sql_cur = self.sql_conn.cursor()
        self._init_edu_degree()
        self._init_prof_title()
        self._init_award()
        self._init_high_level_honor()
        self.count_skip = 0
        self.count_insert = 0
        self.count_dupl = 0
        self.count_inner_dupl = 0

    def _init_edu_degree(self):

        self.edu_degree_schema = {}       
        sql_state = self.config.get("mysql","edu_degree_query")
        self.sql_cur.execute(sql_state)
        for c in self.sql_cur.fetchall():
            self.edu_degree_schema[c[0]] = c[1]


    def _init_prof_title(self):
        self.prof_title = []
        sql_state = self.config.get("mysql","prof_title_query")
        self.sql_cur.execute(sql_state)
        for t in self.sql_cur.fetchall():
            self.prof_title.append(t[0])
        self.prof_title.sort(key=lambda x:len(x),reverse=True)
        self.sql_cur.close()
        self.sql_conn.close()
        
    def _init_award(self):
        with open(os.path.join(dir_path,"award.txt"),"r") as fr:
            datas = fr.readlines()
        self.awards = list(map(lambda x: x.strip(), datas))
        self.awards.sort(key=lambda x:len(x), reverse=True)
        
    def _init_high_level_honor(self):
        with open(os.path.join(dir_path, "high_level_honor.txt"),"r") as fr:
            datas = fr.readlines()
        self.high_level_honors = list(map(lambda x: x.strip(), datas))
        self.high_level_honors.sort(key=lambda x:len(x), reverse=True)


    def process_str(self, s):
        '''字符串常规处理'''
        string = ""
        # 空值的特殊表达添加到List中
        null_equal = ["暂无简历"]
        
        if s:
            s = s.strip()
            string = s.replace("（","(").replace("）",")").replace("\r","").replace(";","；").replace(".","。").replace(",","，")
            if string in null_equal:
                string = ""
            
        return string


    def process_str_list(self, p):

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t ]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list

    def process_name(self, doc):
        '''姓名清洗，英文姓名的分隔符统一用空格；带有实验室的姓名去除实验室名称'''
        name = doc["expert_name"]
        
        if "实验室" in name:
            name = name.split(" ")[0]
        elif "工程院院士" in name:
            name = name.split(" ")[0]
        elif "科学院院士" in name:
            name = name.split(" ")[0]

        name = name.replace(","," ")
        return name

    def process_institution(self, doc):
        '''所在高校机构，处理成数组'''
        res = []
        ins_str = doc["research_institution"]
        if ins_str:
            ins = re.split("[,;/]",ins_str)
            ins = [ i.strip() for i in ins]
            res.extend(ins)
        return res

    def process_field(self, doc):
        '''研究领域数组化'''
        res = []
        field = doc["research_field"]
        if field:
            fields = re.split("[;]",field)
            fields = [i.strip() for i in fields]
            res.extend(fields)
        return res

    def process_birthday(self, doc):
        res = ""
        birthday = doc["birth_day"]
        if birthday:
            res = birthday[:10]
        return res
    
    def extract_birthday(self, data):
        birthday = ""   
        expert_name = data["name"]
        s = data["resume"]
        
        patterns = [
            "(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)生",
            "(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)出生",
            "出生年月：?(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)",
            expert_name + "\((\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)",
            "生于(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)"
        ]
        
        for pattern in patterns:
            birthdays = re.findall(pattern, s)
            if len(birthdays)>0:
                birthday = birthdays[0]
                break
            
        birthday = re.sub("[年月日.]","-",birthday).strip("-")   
        return birthday
    
    
    def extract_nation(self, data):
        nation = ""
        s = data["resume"]
        patterns = [
            "[，：。](\w{1,5}族)[，。；]"
        ]
        for p in patterns:
            nations = re.findall(p,s)
            if len(nations)>0:
                nation = nations[0]
                break
        return nation
    
    
    def extract_honors(self, data):
        resume = data["resume"]
        honors = []
        sentences = re.split("[。；！]", resume)
        honor_key_words = ["奖","荣誉","称号","优秀","先进","人才","模范","新星","标兵","突出贡献","特殊津贴","杰出"]
        
        for sentence in sentences:
            items = self.process_honor(sentence)
            for item in items:
                for key in honor_key_words:
                    if key in item and item not in honors:
                        honors.append(item)
                        break 
                
        return honors
    
    def extract_awards(self, data):
        res = []
        resume = data["resume"]               
        resume = resume.replace("科学技术","科技")
        for a in self.awards:
            matches = re.findall(a, resume)
            for m in matches:
                if m not in res:
                    res.append(m)
                    resume = resume.replace(m, "")
        return res 
    
    def extract_high_level_honors(self, data):
        res = []
        resume = data["resume"]              
        for h in self.high_level_honors:
            matches = re.findall(h, resume)
            for m in matches:
                if m not in res:
                    res.append(m)
                    resume = resume.replace(m, "")
        return res 
            

    def process_honor(self, sentence):
        '''判断一个句子是否含有荣誉项，并输出荣誉List'''
        honor_items = []
        patterns = [
            "获.*?奖",
            "获.*?荣誉",
            "被评为.*?先进",
            "被评为.*?优秀",
            "入选.*?计划"
        ]
        for p in patterns:
            match = re.findall(p, sentence)
            if match:
                if "，" in sentence:
                    honor_items = list(map(lambda x:x.strip(),sentence.split("，"))) 
                # elif "、" in sentence:
                #     honor_items = list(map(lambda x:x.strip(),sentence.split("、"))) 
                else:
                    honor_items = [sentence]
                break
            
        honor_items = list(filter(lambda x:len(x)>0, honor_items))     
        return honor_items
            
    def extract_native_place(self, data):
        resume = data["resume"]
        place = ""
        patterns = [
            "(\w{2,9})人$",
            "出生于(\w{2,}$)"      
        ]
        for s in re.split("[，。：；]", resume):
            for p in patterns:
                match = re.findall(p, s)
                if match != []:
                    place = match[0]
                    return place           
        return place    

    def process_edu_degree(self, string):
        '''学历信息根据MYSQL处理'''
        res = ""
        for key in self.edu_degree_schema:
            if key in string:
                res = key 
                break
        return res 

    def process_edu_date(self, string):
        '''学历起止时间处理'''
        res = ""
        if string:
            numbers = "".join(re.findall("\d",string))
            if len(numbers) == 4: # like 2020
                res = numbers
            if len(numbers) == 6:  # like 202002
                res = numbers[:4] + "-" + numbers[4:]
            if len(numbers) == 8: # like 20200809
                res = numbers[:4] + "-" + numbers[4:6] + "-" + numbers[6:]
            if len(numbers) >= 14: # like 198309010000000
                res = numbers[:4] + "-" + numbers[4:6] + "-" + numbers[6:8]
        return res

    def process_education(self, doc):
        '''教育经历清洗，学历，时间格式化'''
        res = []
        educations = doc["education"]
        if educations:
            for edu in educations:
                item = {
                    "major": edu["major"].strip(),
                    "degree": self.process_edu_degree(edu["degree"]),
                    "start_time": self.process_edu_date(edu["start_time"]),
                    "end_time": self.process_edu_date(edu["end_time"]),
                    "college": self.process_str(edu["college"])
                }
                res.append(item)
        return res


    def process_subject(self, doc):
        res = ""
        subject = doc["subject"]
        if subject:
            # 过滤乱码数据
            res = "".join(re.findall("[\u4e00-\u9fa5、]", subject))
            if len(subject)<=2:
                res = ""

        res = self.process_str(res)

        return res

    def process_department(self, doc):
        '''移除乱码和无意义符号'''
        res = ""
        department = doc["department"]
        if department:
            res = re.sub("[\r\n\t]","", department)
            res = "".join(re.findall("[\u4e00-\u9fa5]",res))
            if len(res)<=2:
                res = ""
        return res

    def process_nation(self, doc):
        '''清洗乱码和空字符'''
        res = ""
        nation = doc["nation"]
        if nation:
            res = "".join(re.findall("[\u4e00-\u9fa5]",nation))
        return res

    def process_native_place(self, doc):
        '''乱码清洗，省市区分割'''
        res = {
            "native_place":"",
            "province":"",
            "city":"",
            "area":""
        }
        native_place = doc["native_place"]
        if native_place:
            res["native_place"] = "".join(re.findall("[\u4e00-\u9fa5 ]",native_place))
        if res["native_place"]:
            places = res["native_place"].split(" ")
            if len(places) == 3:
                res["province"] = places[0]
                res["city"] = places[1]
                res["area"] = places[2]
            if len(places) == 2 and "省" in places[0]:
                res["province"] = places[0]
                res["city"] = places[1]
            if len(places) == 2 and places[0] in ["北京市","上海市","天津市","重庆市"]:
                res["province"] = places[0].strip("市")
                res["city"] = places[0]
                res["area"] = places[1]
            if len(places) == 1:
                res["province"] = places[0].strip("市")

        return res
                

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

    def process_articles(self, doc):
        '''清洗内部的论文标题'''
        res = []
        articels = doc["journal_article"]
        for a in articels:
            if a["name"]: # name=null的数据需要过滤
                a["name"] = re.sub("[\n\t\r]","",a["name"])
                res.append(a)
        return res
     


    def process_patent(self, doc):
        origins = doc["patent"]
        if origins:
            for p_dict in origins:
                p_dict["name"] = self.process_str(p_dict["name"])
        return origins

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def insert_batch(self, batch_data):
        '''剔除无效数据，根据名称和所在单位进行批内去重，数据库去重'''
        new_batch = []
        unique_vals = []
        for data in batch_data:
            if data["patent"] == [] and data["papers"] == [] and data["book"] == [] and data["tech_achievement"] == []:
                self.count_skip += 1
                logger.warning("跳过本条数据，ObjectId=[{}]，专家=[{}]。原因：专家的成果（专利，论文，书籍，科研项目）均为空，信息不足。".format(data["_id"],data["name"]))
                continue
            # 姓名和所在单位初步去重
            exist = self.res_kb_process_expert.find_one({"name": data["name"], "research_institution": data["research_institution"]})
            if exist:
                self.count_dupl += 1
                #logger.info("跳过本条数据，原因：专家=[{}]，所在单位=[{}]数据在清洗库中已存在".format(data["name"], data["research_institution"]))
                continue
            if [data["name"],data["research_institution"]] in unique_vals:
                self.count_inner_dupl += 1
                #logger.info("跳过本条数据，原因：专家=[{}]，所在单位=[{}]数据批内记录已存在".format(data["name"], data["research_institution"]))
                continue

            unique_vals.append([data["name"], data["research_institution"]])
            new_batch.append(data)
        if new_batch:
            insert_res = self.res_kb_process_expert.insert_many(new_batch)
            self.count_insert += len(insert_res.inserted_ids)
        else:
            logger.info("本批数据处理后清洗库无新增")

    def query_data(self, crawl_date):
        '''
        获取爬虫时间大于等于crawl_date的专家数据
        '''

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
        self.process_date = crawl_date
        res = self.res_kb_expert.find({'crawl_time': {'$gte': iso_date}}, no_cursor_timeout=True)#.sort([("crawl_time",1)])
        return res

    def process(self, crawl_date):
        
        count = 0
        res = self.query_data(crawl_date)
        total = res.count()
        logging.info("共查询到爬虫库专家数据{}条，准备处理...".format(total))
        finish_flag = False
        batch_data = []
        
        for doc in res:

            #logging.info("正在处理专家，专家名=[{}]，ObjectId=[{}]".format(doc["expert_name"], str(doc["_id"])))
            count += 1 # 当前处理数据数量
            if count%1000==0:
                logger.info("正在处理第{}条数据...".format(count))
           
            clean_expert = {
                "_id": str(doc["_id"]), 
                "search_key": doc["search_key"], 
                "name": self.process_name(doc),
                "kId": self.process_str(doc["kId"]) if "kId" in doc else "",
                "tel": doc["tel"] if "tel" in doc and doc["tel"] else "",
                "gender": doc["gender"] if doc["gender"] else "",
                "img_url": doc["img_url"],
                "research_institution": self.process_institution(doc),
                "research_field": self.process_field(doc),
                "resume": self.process_str(doc["expert_resume"]),
                "key_word": doc["key_word"],
                "professional_title": self.process_title(doc),
                "birthday": self.process_birthday(doc),
                "education": self.process_education(doc),
                "department": self.process_department(doc), # 学部
                "subject": self.process_subject(doc),  # 学科
                "category": doc["category"], # 爬虫自己分类的字段，不做处理，可以供专家分类参考
                "patent": self.process_patent(doc),
                "book": doc["book"], # 研究专著
                "papers": self.process_articles(doc), 
                "tech_achievement": doc["tech_achievement"],
                "nation": self.process_nation(doc),
                "honors":[],
                "awards":[],
                "high_level_honors":[],
                "url": doc["url"],
                "html": doc["html"],
                "source": doc["source"],
                "crawl_time": doc["crawl_time"],
                "create_time": datetime.datetime.today(),
                "update_time": datetime.datetime.today()
            }


            native_addr = self.process_native_place(doc)
            clean_expert.update(native_addr)
            
            # 以下为解析抽取字段
            if clean_expert["resume"] != "":
                
                if clean_expert["birthday"] == "" :
                    clean_expert["birthday"] = self.extract_birthday(clean_expert)
                    
                if clean_expert["nation"] == "" :
                    clean_expert["nation"] = self.extract_nation(clean_expert)
                    
                if clean_expert["native_place"] == "":
                    clean_expert["native_place"] = self.extract_native_place(clean_expert)
                
                clean_expert["honors"] = self.extract_honors(clean_expert)    
                clean_expert["awards"] = self.extract_awards(clean_expert)    
                clean_expert["high_level_honors"] = self.extract_high_level_honors(clean_expert)
                                 
            batch_data.append(clean_expert)

            # MongoDB批量写入
            if count % 100 == 0 or count == total:
                self.insert_batch(batch_data)
                batch_data = []


        
        self.close_connection()
        logging.info("[{}] 专家数据清洗完毕，共找到专家[{}]条，跳过缺失有效信息的数据[{}]条，数据库已存在数据[{}]条，批内去重[{}]条，清洗库入库[{}]条".format(
                    self.process_date, total, self.count_skip, self.count_dupl, self.count_inner_dupl, self.count_insert) )


if __name__ == "__main__":

    # 专家最早爬虫日期为 2019-05-24
    
	cleaner = ExpertClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
