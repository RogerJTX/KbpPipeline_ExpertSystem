#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-05-08 17:28
# Filename     : leader_clean.py
# Description  : res_kb_leader高管信息清洗逻辑： 
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
from tqdm import tqdm 
from bson import ObjectId

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
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)

class LeaderClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.res_kb_leader = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_leader")] 
        self.res_kb_process_leader = self.mongo_con[self.config.get("mongo","res_kb_process")][self.config.get("mongo","process_leader")] # 高管清洗库
        self.sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                user = self.config.get("mysql","user"),
                passwd = self.config.get("mysql","passwd"),
                port = self.config.getint("mysql","port") ,
                db = self.config.get("mysql","db"),
                charset = self.config.get("mysql","charset") )
        self.sql_cur = self.sql_conn.cursor()
        self._init_edu_degree()
        self._init_prof_title()
        self._init_position()
        self._init_oversea_university()
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
        
    def _init_position(self):
        with open(os.path.join(dir_path, "position.txt"), "r") as fr:
            positions = fr.readlines()
        self.positions = list(map(lambda x:x.strip(), positions))
        self.positions.sort(key= lambda x:len(x), reverse=True)
        
    def _init_oversea_university(self):
        with open(os.path.join(dir_path, "oversea_university.txt"), "r") as fr:
            positions = fr.readlines()
        self.oversea_university = list(map(lambda x:x.strip().lower(), positions))
        
    def check_university_oversea(self, s):
        oversea = ""
        if s:
            if "university" in s:
                oversea = "1"
            else:
                for key in self.oversea_university:
                    if key in s:
                        oversea = "1"
                        break
        return oversea 
        

    def process_str(self, s):
        '''字符串常规处理'''
        s = str(s)
        string = ""
        if s:
            s = s.strip()
            string = s.replace("（","(").replace("）",")").replace("\r","").replace(";","；").replace(".","。").replace(",","，")
        return string


    def process_str_list(self, p):

        tmp_list = list( filter(lambda x:len(x)>0, re.split("[;,，；\n\t ]",p) ) )
        res_list = list(map(self.process_str, tmp_list))    
        return res_list

    def process_name(self, doc):
        '''姓名清洗，英文姓名的分隔符统一用空格；带有实验室的姓名去除实验室名称'''
        name = doc["leader_name"]
        
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
    
    def extract_birthday(self, s):
        birthday = ""   
        
        patterns = [
            "(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)生",
            "(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)出生",
            "出生年月：?(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)",
            "生于(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)"
        ]
        
        for pattern in patterns:
            birthdays = re.findall(pattern, s)
            if len(birthdays)>0:
                birthday = birthdays[0]
                break
            
        birthday = re.sub("[年月日.]","-",birthday).strip("-")   
        return birthday
    
    
    def extract_nation(self, s):
        nation = ""
        patterns = [
            "[，：。](\w{1,5}族)[，。；]"
        ]
        for p in patterns:
            nations = re.findall(p,s)
            if len(nations)>0:
                nation = nations[0]
                break
        return nation
            
    
    def extract_final_edu_degree(self, resume):
        
        final_edu_degree = ""
        edu_degree = ["博士","硕士","学士","本科"]
        
        for d in edu_degree:
            if d in resume:
                final_edu_degree = d
                break
        if final_edu_degree == "本科":
            final_edu_degree = "学士"
            
        return final_edu_degree 
    
    def check_exist(self, item, items):
        '''
        功能：字典数据重复检查；检查item对应key的数据是否在items中存在
        '''
        exist = False
        new_items = []       
        check_keys = [ k for k in item if item[k] ]
        
        for i in items:
            new_i = dict([(key, i[key]) for key in check_keys])
            new_items.append(new_i)
            
        if item in new_items:
            exist = True
        return exist
    
    def tail_filter(self, s):
        '''保留短句后面的内容'''
        s = re.sub("[“”]", "", s)
        if re.findall("[。；，：、]",s):
            s = re.split("[。；，：、]", s)[-1]
        return s

    def head_filter(self, s):
        '''保留短句前面的内容'''
        s = re.sub("[“”]", "", s)
        if re.findall("[,，]",s):
            s = re.split("[，,]", s)[0]
        s = s.strip("、")
        return s  
    
    def process_edu_experiences(self, s):
        edu_experiences = []
        for sentence in re.split("[。；！]", s):
            edu_items = self.extract_edu_experience(sentence)
            for item in edu_items:
                if not self.check_exist(item, edu_experiences):
                    edu_experiences.append(item)
        return edu_experiences
    
    def extend_patterns(self, patterns):
        new_patterns = [] 
              
        candidate = ["学院","分校","所","university"]
        for c in candidate:
            new_p = [ p.replace("大学", c) for p in patterns]
            new_patterns.extend(new_p)
        new_patterns.extend(patterns)
        
        return new_patterns
            
    
    def extract_edu_experience(self, s):
        edu_experience = []
        
        # 年份 学校 学位
        patterns = [
            "(\d{4}[年.-]?\d{,2}[月.-]?[至~-]?\d{,4}年?\d{,2}月?)，在(\w*?大学)攻?读?(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?[至-]?\d{,4}年?\d{,2}月?)，?就读于(\w*?)，[获取]得?(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?[至-]?\d{,4}年?\d{,2}月?)，?就读于(\w*?大学)\w*，[获取]?得?(\w*?本科)",
            "(\d{4}[年.-]?\d{,2}[月.-]?[至-]?\d{,4}年?\d{,2}月?)，(\w*?大学)\w*，[获取]?得?(\w*?本科)",         
            "(\d{2,4}[年.-]?\d{,2}[月.-]?)，?[在于从](\w*?大学)\w*[获取]得?(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?[至-]?\d{,4}年?\d{,2}月?)毕业于(\w*?大学)，(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?)毕业于(\w*?大学).*?[，]?[获取]得?(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?)留学(\w*?大学)\w*?[，]?[获取]得?(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?)赴(\w*?大学).*?攻读(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?)在(\w*?大学)[获取]得?(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?)[取获]得?(\w*?大学)(\w*?[硕博学]士)",
            "(\d{4}[年.-]?\d{,2}[月.-]?[至~-]?\d{,4}年?\d{,2}月?)[，： ]?(\w+大学)(\w+本科)",
            "(\d{4}[年.-]?\d{,2}[月.-]?[至~-]?\d{,4}年?\d{,2}月?)[，： ]?(\w+大学)(\w+[硕博]士)",
            "([硕博学]士)，(\d{4}年?\d{,2}[月.-]?)毕业于(\w*?大学)",
            "(\d{4}[年.-]?\d{,2}[月.-]?)(本科)毕业于(\w+大学)"
        ]
        patterns = self.extend_patterns(patterns)
        
        for p in patterns:
            whole_p = re.sub("[()]", "", p)
            match = re.findall(whole_p, s)
            
            for sentence_match in match:
                m = re.findall(p, sentence_match)
                for each_m in m:
                    item = {}
                    if re.findall("\d{2,4}", each_m[0]):
                        if re.findall("本科", each_m[1]):
                            item = {"duration": each_m[0],
                                    "university": each_m[2],
                                    "edu_degree": each_m[1].replace("本科","学士"), 
                            }
                        else:
                            item = { "duration": each_m[0], 
                                    "university": self.tail_filter(each_m[1]).strip("[完成了]"), 
                                    "edu_degree": self.tail_filter(each_m[2]).replace("本科","学士")
                                    }
                    elif re.findall("[硕博学]士", each_m[0]):
                        item = { "duration": each_m[1], 
                                "university": each_m[2], 
                                "edu_degree": each_m[0]
                                }
                    if not self.check_exist(item, edu_experience):
                        item["oversea"] = self.check_university_oversea(item["university"])
                        edu_experience.append(item)
                        
                s = s.replace(sentence_match, "")
                
        # 年份 学校
        patterns = [
            "(\d{2,4}年?\d{,2}月?)毕业于(\w*大学)",
            "(\d{2,4}年?\d{,2}月?)考入(\w*大学)",
            "(\d{2,4}年?\d{,2}月?[至-]?今?\d{,4}年?\d{,2}月?)，?就读于(\w*大学)",
            "(\d{2,4}年?)毕业于(\w*大学)",
            "(\d{2,4}年?)考入(\w*大学)",
            "(\d{2,4}年?\d{,2}月?)留学(.*大学)",
        ]
        patterns = self.extend_patterns(patterns)
        for p in patterns:
            whole_p = re.sub("[()]", "", p)
            match = re.findall(whole_p, s)
                       
            for sentence_match in match:
                m = re.findall(p, sentence_match)
                for each_m in m:
                    item = {"duration": each_m[0], "university": self.tail_filter(each_m[1]), "edu_degree": ""}
                    if not self.check_exist(item, edu_experience):
                        item["oversea"] = self.check_university_oversea(item["university"])
                        edu_experience.append(item)
                s = s.replace(sentence_match, "")
                
        # 年份 学位
        patterns = [
            "(\d{4}年?\d{,2}月?)[获取]得?(.*?[博硕学]士)",
            
        ]
        patterns = self.extend_patterns(patterns)
        for p in patterns:
            whole_p = re.sub("[()]", "", p)
            match = re.findall(whole_p, s)
            for sentence_match in match:
                m = re.findall(p, sentence_match)
                for each_m in m:
                    item = {"duration": each_m[0], "university": "", "edu_degree": self.tail_filter(each_m[1])}
                    if not self.check_exist(item, edu_experience):
                        item["oversea"] = self.check_university_oversea(item["university"])
                        edu_experience.append(item)
                s = s.replace(sentence_match, "")
                
        # 学校  学位
        patterns = [
            "毕业于([\w ]*?大学)，?[取获]?得?(.*?[博硕学]士)",
            "毕业于([\w ]*?大学)\w*，(本科)学历",
            "毕业于([\w ]*?)，[获取]得?(\w*?[博硕学]士)",
            "拥有([\w ]*?大学)的(\w*?[博硕学]士)",
            "(\w*?大学)毕业，[获取]得?(.*?[博硕学]士)",
            "[获取]?得?([\w ]*?大学)([\w（）]*[博硕学]士)",
            "[获取]?得?([\w ]*?大学)([\w（）]*本科)",
            "在([\w ]*?大学)[获取]得?(\w*?[博硕学]士)"          
        ]
        patterns = self.extend_patterns(patterns)
        for p in patterns:
            whole_p = re.sub("[()]","",p)
            match = re.findall(whole_p,s)
            for sentence_match in match:
                s = s.replace(sentence_match,"")
                m = re.findall(p,sentence_match)[0] 
                university = self.tail_filter(m[0]).strip("[拥有|有|是|他是|为|后取得|之后在|同年到|毕业于|并且在|并在]")
                university = "" if university == "大学" else university 
                item = { "duration": "", 
                        "university": university, 
                        "edu_degree": self.tail_filter(m[1]).replace("本科","学士").strip("[与]")
                        }
                if not self.check_exist(item, edu_experience):
                    item["oversea"] = self.check_university_oversea(item["university"])
                    edu_experience.append(item)
                    
        # 学校 学位
        patterns = [
            "([硕博学]士)毕业于(\w*?大学)",
            "(本科)毕业于(\w*?大学)"
        ]
        patterns = self.extend_patterns(patterns)
        
        for p in patterns:
            whole_p = re.sub("[()]", "", p)
            match = re.findall(whole_p, s)
            for sentence_match in match:
                m = re.findall(p, sentence_match)
                for each_m in m:
                    item = { "duration": "", 
                            "university": self.head_filter(each_m[1]), 
                            "edu_degree": self.tail_filter(each_m[0]).replace("本科","学士")
                            }
                    if not self.check_exist(item, edu_experience):
                        item["oversea"] = self.check_university_oversea(item["university"])
                        edu_experience.append(item)
                s = s.replace(sentence_match, "")
        
        # 学校
        patterns = [
            "毕业于(.*?大学)",
            "毕业于([\w ]*?)",
            "([\w ]*?大学)毕业"
        ]
        patterns = self.extend_patterns(patterns)
        for p in patterns:
            whole_p = re.sub("[()]", "", p)
            match = re.findall(whole_p, s)
            for sentence_match in match:
                m = re.findall(p, sentence_match)
                for each_m in m:
                    item = {"duration": "", "university": self.tail_filter(each_m), "edu_degree": ""}
                    if not self.check_exist(item, edu_experience):
                        item["oversea"] = self.check_university_oversea(item["university"])
                        edu_experience.append(item)
                s = s.replace(sentence_match, "")
                
        return edu_experience 
    
    def process_work_experiences(self, s):
        work_experiences = []
        
        # 途径1： 正则匹配
        for sentence in re.split("[。；！]", s):
            work_items = self.extract_work_experience(sentence)
            for item in work_items:
                if not self.check_exist(item, work_experiences):
                    work_experiences.append(item)
                    
        # 不规则公司职位匹配
        # if work_experiences == []:
        #     first_sentence = re.split("[。！；]",s)[0]
        #     work_experience = self.extract_api_work_experience(first_sentence)
        #     for item in work_experience:
        #         if not self.check_exist(item, work_experiences):
        #             work_experiences.append(work_experience)
                    
        # 梳理公司职位匹配
        if work_experiences == []:
            first_sentence = re.split("[。！；]",s)[0]
            work_experience = self.extract_manual_work_experience(first_sentence)
            for item in work_experience:
                if not self.check_exist(item, work_experiences):
                    work_experiences.append(item)
                    
        return work_experiences
    
    def extract_manual_work_experience(self, s):   
        work_items = []
        
        sub_sentences = re.split("[，,、]", s)
        for sub_sentence in sub_sentences:
            # 半句处理，假定半句只涉及一个公司，可有多个职位
            origin_sub_sentence = copy.deepcopy(sub_sentence)
            pos_indexes = []
            positions = []
            
            for position in self.positions:
                pos_index = sub_sentence.find(position)
                if pos_index > 0:
                    pos_indexes.append(pos_index)
                    positions.append(position)
                    sub_sentence = sub_sentence.replace(position,"遮"*len(position))
                    
            if pos_indexes:
                pos_min_index = min(pos_indexes)
                item = {
                    "duration":"",
                    "company": origin_sub_sentence[:pos_min_index].strip("的").strip("现任").strip("多年").strip("曾担任"),
                    "position": "、".join(positions)
                }
                if len(item["company"]) > 1 and item["company"]!="注册" and item["company"]!="财务":
                    work_items.append(item)
                
        return work_items
    
    def extract_work_experience(self, s):
        work_experience = []
    
        # 时间 公司 职位
        patterns = [
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?[就任]职于(.*?)，?先?后?担?任(\w+)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?担?任(.*?公司)(\w+)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?担?任(.*?[所院])(\w+)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?担?任(.*?大学)(\w+)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?担?任(.*?集团)(\w+)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?于(.*?)担?任(.*)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?在(.*?)工?作?，?担?任(.*)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)[加进]入(.*?)，?[历担]?任(.*)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?就职于(.*?)历任(.*)"
        ]
        for p in patterns:
            whole_p = re.sub("[()]","",p)
            match = re.findall(whole_p,s)
            for sentence_match in match:
                s = s.replace(sentence_match,"")
                ms = re.findall(p,sentence_match)
                for m in ms:     
                    item = {"duration":m[0],
                            "company":self.head_filter(m[1]),
                            "position":self.head_filter(m[2])
                            }
                    if not self.check_exist(item, work_experience):
                        work_experience.append(item)
                    
        # 时间 职位
        patterns = [
            "(\d{4}年?\d{,2}月?[至-]?今?\d{,4}年?\d{,2}月?)历任(.*)",
        ]
        for p in patterns:
            whole_p = re.sub("[()]","",p)
            match = re.findall(whole_p,s)
            for sentence_match in match:
                s = s.replace(sentence_match, "")
                ms = re.findall(p, sentence_match)
                for m in ms:          
                    item = {"duration":m[0], "company":"", "position":self.head_filter(m[1])}
                    if not self.check_exist(item, work_experience):          
                        work_experience.append(item)
                        
        # 时间 公司
        patterns = [
            "(\d{4}年?\d{,2}月?[至-]?今?\d{,4}年?\d{,2}月?)[任就]职于(.*)", 
            "(\d{4}年?\d{,2}月?[至-]?今?\d{,4}年?\d{,2}月?)加入(.*)",
            "(\d{4}年?\d{,2}月?起?[至-]?今?\d{,4}年?\d{,2}月?)，?在(.*?)任职"     
        ]
        for p in patterns:
            whole_p = re.sub("[()]","",p)
            match = re.findall(whole_p,s)
            for sentence_match in match:
                s = s.replace(sentence_match,"")
                ms = re.findall(p,sentence_match) 
                for m in ms:          
                    item = {"duration":m[0], "company":self.head_filter(m[1]), "position":"" }
                    if not self.check_exist(item, work_experience):          
                        work_experience.append(item)
                            
        # 公司 职位
        patterns = [
            "现任(.*?公司)([\w ]*)",
            "、([\w ]*?公司)(\w )*?",
            "为(\w+公司)([\w ]*)",
            "现任(.*大学)(.*)",
            "曾[就任]职?于?(.*厂)(.*)",
            "曾?[就任]?职?于?(.*公司)，?担?任?([\w 、]*)",
            "曾先?后?受聘于(.*公司)，?从事(.*)工作",
            "任职于?(.*?)，?从事(.*)工作",
            "曾在(.*银行).*?担?任(.*)工?作?",
            "(\w+)工作，担?任(\w+)",
            "在(\w*?大学)担?任(\w*?)" 
        ]
        for p in patterns:
            whole_p = re.sub("[()]","",p)
            match = re.findall(whole_p,s)
            for sentence_match in match:
                s = s.replace(sentence_match,"")
                ms = re.findall(p,sentence_match)  
                for m in ms:        
                    item = {"duration":"", 
                            "company":m[0].strip("留"),
                            "position":self.head_filter(m[-1])
                            }
                    if not self.check_exist(item, work_experience):
                        work_experience.append(item)
                        
        # 公司
        patterns = [
            "曾供职于(.*)",
            "曾在(.*?)工作"
        ]
        for p in patterns:
            whole_p = re.sub("[()]","",p)
            match = re.findall(whole_p,s)
            for sentence_match in match:
                s = s.replace(sentence_match,"")
                ms = re.findall(p,sentence_match)
                for m in ms:         
                    item = {"duration":"", "company":self.head_filter(m), "position":"" }
                    if not self.check_exist(item, work_experience):
                        work_experience.append(item)
                    
        return work_experience
    
    
    def extract_api_work_experience(self, s):
        
        work_experiences = []
        
        obj = { "str": [s] }
        req_str = json.dumps(obj).encode()
        url = "https://texsmart.qq.com/api"
        try:
            r = requests.post(url, data=req_str)
        except Exception as e:
            print("接口请求出错，请求句子{}".format(s))
            return work_experiences
        
        if r.status_code == 200:
            r.encoding = "utf-8"
            response = json.loads(r.text)
            response = response["res_list"]
            format_sentences = []
            company_type = ["组织","企业","大学","学校","高校","平台"]
            company_exclude = ["供应商","运营商","服务提供商","IEEE"]
            position_type = ["职位"]
            for sentence_res in response:
                item = {
                    "duration":"",
                    "company":"",
                    "position":""
                }
                entity_list = sentence_res["entity_list"]
                entity_list = list(filter(lambda x:( x["type"]["i18n"] in (company_type + position_type)) 
                                                    and (x["str"] not in company_exclude), entity_list))
                for entity in entity_list:
                    
                    if entity["type"]["i18n"] in company_type:
                        # 槽值更新出队
                        if item["company"] and item["position"]:
                            work_experiences.append(copy.deepcopy(item))
                            item = {"duration":"", "company":entity["str"], "position":""}
                        # 更新公司槽值
                        item["company"] = entity["str"]
                    
                    elif entity["type"]["i18n"] in position_type:
                        if item["company"] and item["position"]:
                            item["position"] = item["position"] + "、" + entity["str"]
                        elif item["company"] and (not item["position"]):
                            item["position"] = entity["str"]
                            
                # 收尾出队
                if item["company"] and item["position"]:
                    work_experiences.append(copy.deepcopy(item))
                        
        return work_experiences
   

    def query_data(self, crawl_date):
        '''
        获取爬虫时间大于等于crawl_date的高管数据
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
        res = self.res_kb_leader.find({'crawl_time': {'$gte': iso_date}}, no_cursor_timeout=True).sort([("_id",1)])
        # res = self.res_kb_leader.find({"_id":{"$gt":ObjectId("5f1f5f562514973fbcaaa9c0")}}, no_cursor_timeout=True).sort([("_id",1)])
        return res

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def insert_batch(self, batch_data):
        '''剔除无效数据，根据名称和所在单位进行批内去重，数据库去重'''
        new_batch = []
        unique_vals = []
        for data in batch_data:
            # 姓名和所在单位初步去重
            exist = self.res_kb_process_leader.find_one({"name": data["name"], "company_full_name": data["company_full_name"]})
            if exist:
                self.count_dupl += 1
                logger.info("跳过本条数据，原因：高管=[{}]，所在单位=[{}]数据在清洗库中已存在".format(data["name"], data["company_full_name"]))
                continue
            if [data["name"],data["company_full_name"]] in unique_vals:
                self.count_inner_dupl += 1
                logger.info("跳过本条数据，原因：高管=[{}]，所在单位=[{}]数据批内记录已存在".format(data["name"], data["company_full_name"]))
                continue

            unique_vals.append([data["name"], data["company_full_name"]])
            new_batch.append(data)
        if new_batch:
            insert_res = self.res_kb_process_leader.insert_many(new_batch)
            self.count_insert += len(insert_res.inserted_ids)
        else:
            logger.info("本批数据处理后清洗库无新增")
            
    def process_full_name(self, doc):
        full_names = []
        # 鲸准平台转换的企业全称字段
        if "company_full_name" in doc:
            full_names = [ self.process_str(name) for name in doc["company_full_name"]]
        else:
            full_names = [ self.process_str(doc["company_name"])]
        return full_names
    
    def update_record(self, exist, doc):
        update = {}
        # 高管经历合并
        raw_work_experiences = exist["raw_work_experiences"]
        cur_work = {
            "object_id": str(doc["_id"]),
            "company": self.process_str(doc["company_full_name"][0])  if "company_full_name" in doc and doc["company_full_name"] else self.process_str(doc["company_name"]),
            "position": self.process_str(doc["position"])
        }
        if cur_work not in raw_work_experiences:
            raw_work_experiences.append(cur_work)
            update["raw_work_experiences"] = raw_work_experiences
        
        # 简历信息更新
        if (not exist["resume"]) and doc["introduction"]:
            update["resume"] = self.process_str(doc["introduction"])
            resume_str = re.sub( "[“”]", "", update["resume"]).lower()              
            update["birthday"] = self.extract_birthday(resume_str)                   
            update["nation"] = self.extract_nation(resume_str)                   
            update["final_edu_degree"] = self.extract_final_edu_degree(resume_str)
            update["edu_experiences"] = self.process_edu_experiences(resume_str)
            update["work_experiences"] = self.process_work_experiences(resume_str) 
            
        # 更新数据
        if update:
            update["crawl_time"] = doc["crawl_time"]
            update["update_time"] = datetime.datetime.today()
            self.res_kb_process_leader.update_one({"_id":exist["_id"]},{"$set":update})
            logger.info("高管信息更新成功，person_id = [{}]".format(exist["person_id"]))
                      

    def process(self, crawl_date):
        
        res = self.query_data(crawl_date)
        total = res.count()
        logging.info("共查询到爬虫库高管数据{}条，准备处理...".format(total))
        batch_data = []
        count = 0
        
        for doc in tqdm(res):
            count += 1
            person_id = doc["person_id"]
            exist = self.res_kb_process_leader.find_one({"person_id":person_id})
            
            # 新增数据插入
            if not exist:
                clean_leader = {
                    "_id": str(doc["_id"]),
                    "search_key": doc["search_key"],
                    "name": self.process_str(doc["person_name"]),
                    "resume": self.process_str(doc["introduction"]),
                    "person_id": self.process_str(doc["person_id"]),
                    "url": doc["url"],
                    "crawl_time": doc["crawl_time"],
                    "source": doc["source"],
                    "birthday":"",
                    "nation":"",
                    "final_edu_degree":"",
                    "edu_experiences":[],
                    "work_experiences":[],
                    "raw_work_experiences":[],
                    "create_time": datetime.datetime.today(),
                    "update_time": datetime.datetime.today() 
                }
                
                # 高管经历
                cur_work = {
                    "object_id": str(doc["_id"]),
                    "company": self.process_str(doc["company_full_name"][0])  if "company_full_name" in doc and doc["company_full_name"] else self.process_str(doc["company_name"]),
                    "position": self.process_str(doc["position"])
                }
                clean_leader["raw_work_experiences"] = [cur_work]
                
                # 简历抽取字段
                if clean_leader["resume"]:
                    resume_str = re.sub( "[“”]", "", clean_leader["resume"]).lower()              
                    clean_leader["birthday"] = self.extract_birthday(resume_str)                   
                    clean_leader["nation"] = self.extract_nation(resume_str)                   
                    clean_leader["final_edu_degree"] = self.extract_final_edu_degree(clean_leader)
                    clean_leader["edu_experiences"] = self.process_edu_experiences(resume_str)
                    clean_leader["work_experiences"] = self.process_work_experiences(resume_str) 
                    
                batch_data.append(clean_leader)
                if batch_data and len(batch_data)%100 == 0:
                    self.res_kb_process_leader.insert_many(batch_data)
                    logger.info("前[{}]条记录处理完成，ID range = [{} - {}]".format(count,batch_data[0]["_id"], batch_data[-1]["_id"]))
                    batch_data = []  
                                      
            # personID已存在，高管经历合并         
            else:                 
                self.update_record(exist, doc)
                
                
        if batch_data:
            self.res_kb_process_leader.insert_many(batch_data)
            batch_data = []
       
        self.close_connection()
        logging.info("[{}] 高管数据清洗完毕，共找到高管[{}]条，数据库已存在数据[{}]条，批内去重[{}]条，清洗库入库[{}]条".format(
                    self.process_date, total, self.count_dupl, self.count_inner_dupl, self.count_insert) )


if __name__ == "__main__":

    # 高管最早爬虫日期为 2020-07-25
    
	cleaner = LeaderClean()
	if len(sys.argv) > 1:
		cleaner.process(sys.argv[1])
	else:
		cleaner.process("yesterday")
