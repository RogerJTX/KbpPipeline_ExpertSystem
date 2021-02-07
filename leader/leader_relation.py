#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-03-31 19:05
# Last modified: 2020-04-09 14:18
# Filename     : leader_kbp.py
# Description  : 高管清洗库转移到高管知识库，处理：产业/产业领域标签添加、高管标签schema添加
#******************************************************************************

import configparser
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
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

class RelationPipeline(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.arango_db = self.arango_con[self.config.get("arango","db")]
        self.kb_leader = self.arango_db[self.config.get("arango","kb_leader")]
        self.kb_company = self.arango_db[self.config.get("arango","kb_company")]
        self.count_graph_update = 0 # arango更新关系数据数量
        
    def process_company_rel(self, properties):
        '''高管所在企业关系'''
        company_rels = []
        raw_work_experiences = properties["raw_work_experiences"]
        companys = [ item["company"] for item in raw_work_experiences]
        companys = list(set(companys))
        
        for company_name in companys:
            company = self.kb_company.fetchFirstExample({"name": company_name})
            if company:
                company = company[0] # company返回的是cursor
                company_rel = {
                    "relation_type":"concept_relation/100020",
                    "object_name": company["name"],
                    "object_type": "company",
                    "object_id": company["_id"]
                }
                company_rels.append(company_rel)
                
        return company_rels   


    def process_relations(self, properties,_key):
        '''
        添加关系
        '''
        relations = []

        # 企业关联
        company_rel = self.process_company_rel(properties)
        relations.extend(company_rel)

        return relations


    def query_process_leader(self, start_date, end_date):
        
        if start_date == "yesterday":
            start_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = datetime.datetime.today().strftime("%Y-%m-%d")
       
        elif start_date == "today":
            start_date = datetime.datetime.today().strftime("%Y-%m-%d")
            
        elif len(start_date.split("-")) == 3:
            pass
            
        else:
            raise Exception("无效参数")
        
        self.process_date = start_date

        if end_date:
            aql = "FOR leader IN {} FILTER leader.update_time >= '{}' and leader.update_time < '{}' sort leader._key asc return ".format(
                        self.config.get("arango","kb_leader"), start_date, end_date)
        else:
            aql = "FOR leader IN {} FILTER leader.update_time >= '{}' sort leader._key asc return ".format(
                        self.config.get("arango","kb_leader"), start_date) 
        
        end_str_sql = "{ '_key':leader._key, 'name':leader.name,'properties':{'raw_work_experiences': leader.properties.raw_work_experiences } }"

        try:
            res = self.arango_db.AQLQuery(aql + end_str_sql, rawResults=True, batchSize=100)
        except AQLFetchError as e:
            '''没有查到数据时，fetch_list会抛出异常'''
            res = []
            logger.warn("Arango高管库没有查到数据",e)

        return res


    def process(self, start_date, end_date = None):

        process_leaders = self.query_process_leader(start_date, end_date)
        count = 0
        doc_list = []

        for leader in process_leaders:
            count += 1
            insert_flag = False
            leader_key = leader["_key"]
            relations = self.process_relations(leader["properties"], leader_key)
            if relations:
                insert_flag = True
                 
            if insert_flag:
                doc = {}
                doc["_key"] = leader_key
                doc["relations"] = relations
                doc["update_time"] = datetime.datetime.today()
                doc_list.append(doc)
                self.count_graph_update += 1
                
            # 处理过程日志打印
            if count % 100 == 0:
                logger.info("前[{}]条高管记录关系组装完成".format(count))
             
            # 写入数据日志打印  
            if self.count_graph_update % 500 == 0 and doc_list!= []:
                logger.info("批量写入高管关系：ID_range = [{} - {}]".format(doc_list[0]["_key"], doc_list[-1]["_key"]))
                self.kb_leader.bulkSave(doc_list, onDuplicate = "update" )
                doc_list = []
                logger.info("数据库写入：前[{}]条高管记录关系添加完成".format(count))

        if doc_list:
            logger.info("前[{}]条高管记录关系组装完成".format(count))
            self.kb_leader.bulkSave(doc_list , onDuplicate = "update")
            logger.info("数据库写入：前[{}]条高管记录关系添加完成".format(self.count_graph_update))
            
        logger.info("日期[{}]清洗库共找到高管[{}]个，arango高管库更新关系[{}]个".format(
            self.process_date, count, self.count_graph_update))

if __name__=="__main__":

    # 最早日期 2019-10-09

    rel = RelationPipeline()
    if len(sys.argv) == 2:
        rel.process(sys.argv[1])
    elif len(sys.argv) == 3:
        rel.process(sys.argv[1], sys.argv[2])
    else:
        rel.process("yesterday")
