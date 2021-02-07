#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-05-27 19:55
# Filename     : company_driven_relation_refresh.py
# Description  : 手动执行脚本： 给定公司名称，在Arango平台对该公司的专利，软著，招聘岗位实体添加与公司的链接关系
#******************************************************************************

import sys
from pymongo import MongoClient
from pymongo import errors
from pyArango.connection import Connection as ArangoConnection
from pyArango.theExceptions import AQLFetchError
import pymysql
from dateutil import parser
import datetime
import json
import logging
import re
import copy
import requests
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

arango_con = ArangoConnection(arangoURL="xxx",
                              username= "xxx",
                              password="xxx")
arango_db = arango_con["ikc_kb_build"]
kb_patent = arango_db["kb_patent"]
kb_software = arango_db["kb_software"]
kb_recruit = arango_db["kb_recruit"]
kb_company = arango_db["kb_company"]



def query_from_file():
    '''读取候选企业'''
    with open(os.path.join(kbp_path,"remain20200527.txt"), "r") as fr:
        companys_read = fr.readlines()
    
    companys = list(map(lambda x:x.strip(), companys_read))

    return companys

companys = query_from_file()

for company in companys:
    # 更新专利关系
    patent_aql = " for c in patent_view search c.properties.applicant == '{}'return c ".format(company)
    try:
        patents = arango_db.fetch_list(patent_aql)
        for p in patents:
            patent_company_rels = []
            patent_key = p["_key"]
            applicant = [p["properties"]["search_key"]]
            applicant.extend(p["properties"]["applicant"])
            for a in applicant:
                c = kb_company.fetchFirstExample({"name": a})
                if not c:
                    continue
                c = c[0] # company返回的是cursor
                patent_company_rel = {
                    "relation_type":"concept_relation/100001",
                    "object_name": c["name"],
                    "object_type": "company",
                    "object_id": c["_id"]
                }
                patent_company_rels.append(patent_company_rel)
            try:
                doc = kb_patent[patent_key]
                doc["relations"] = patent_company_rels
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                logging.info("专利关系更新完毕，公司={}".format(company))

            except Exception as e:
                logging.error("专利关系添加失败，专利名=[{}]".format(p["name"]))
    except Exception as e:
        logging.warning("无专利数据，公司名=[{}]".format(company))

    # 更新软著关系
    software_aql = "for c in kb_software filter c.properties.company_name == '{}' return c ".format(company)
    try:
        softwares = arango_db.fetch_list(software_aql)
        for s in softwares:
            software_company_rels = []
            software_key = s["_key"]
            software_company = s["properties"]["company_name"]
            c = kb_company.fetchFirstExample({"name": software_company})
            if not c:
                continue
            c = c[0] # company返回的是cursor
            software_company_rel = {
                "relation_type":"concept_relation/100002",
                "object_name": c["name"],
                "object_type": "company",
                "object_id": c["_id"]
            }
            software_company_rels.append(software_company_rel)
            try:
                doc = kb_software[software_key]
                doc["relations"] = software_company_rels
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                logging.info("软著关系更新完毕，公司={}".format(company))

            except Exception as e:
                logging.error("软著关系添加失败，软著名=[{}]".format(s["name"]))
    except Exception as e:
        logging.warning("无软著数据，公司名={}".format(company))

    # 更新招聘岗位关系
    recruit_aql = "for c in kb_recruit filter c.properties.company_name == '{}'return c ".format(company)
    try:
        recruits = arango_db.fetch_list(recruit_aql)
        for r in recruits:
            recruit_company_rels = []
            recruit_key = r["_key"]
            recruit_company = r["properties"]["company_name"]
            c = kb_company.fetchFirstExample({"name": software_company})
            if not c:
                continue
            c = c[0] # company返回的是cursor
            recruit_company_rel = {
                "relation_type":"concept_relation/100005",
                "object_name": c["name"],
                "object_type": "company",
                "object_id": c["_id"]
            }
            recruit_company_rels.append(recruit_company_rel)
            try:
                doc = kb_recruit[recruit_key]
                doc["relations"] = recruit_company_rels
                doc["update_time"] = datetime.datetime.today()
                doc.save()
                logging.info("岗位招聘关系更新完毕，公司={}".format(company))

            except Exception as e:
                logging.error("岗位招聘关系添加失败，岗位名=[{}]".format(r["name"]))

    except Exception as e:
        logging.warning("无岗位招聘数据，公司名={}".format(company))
    






