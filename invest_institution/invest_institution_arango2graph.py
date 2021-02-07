#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Create       : 2020-03-31 19:05
# Last modified: 2020-04-09 14:18
# Filename     : organization_kbp.py
# Description  : 高校机构-->企业 关系添加
#******************************************************************************

import configparser
import sys
from pyArango.connection import Connection as ArangoConnection
from pyArango.theExceptions import AQLFetchError
from py2neo import Graph
from dateutil import parser
import datetime
import json
import logging
import re
import copy
import os
from tqdm import tqdm 

main_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(main_path)
from common_utils.neo4j_helper import Neo4jHelper

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

class GraphPipeline(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.arango_con = ArangoConnection(arangoURL=self.config.get("arango","arango_url"),username= self.config.get("arango","user"),password=self.config.get("arango","passwd"))
        self.arango_db = self.arango_con[self.config.get("arango","db")]
        self.kb_invest_institution = self.arango_db[self.config.get("arango","kb_invest_institution")]
        self.label = "InvestInstitution"
        self.total = 0 # 待导入图谱数量


    def query_process_data(self, process_date):
        
        if process_date == "yesterday":
            process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif process_date == "today":
            process_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            
        else:
            raise Exception("无效参数")
        
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)

        aql = "FOR i IN {} FILTER i.update_time >= '{}' SORT i.update_time return i".format(
                        self.config.get("arango","kb_invest_institution"), iso_date) 
        try:
            res = self.arango_db.fetch_list(aql)
        except AQLFetchError as e:
            '''没有查到数据时，fetch_list会抛出异常'''
            res = []
            logger.warn("Arango投资机构库没有查到数据",e)

        self.total = len(res)
        self.process_date = process_date
        logger.info("[{}]，投资机构知识库查到待处理数据[{}]个".format(process_date, self.total))
        return res
 
    def process(self, scan_date):

        datas = self.query_process_data(scan_date)
        self.graph = Graph(self.config.get("neo","neo_url"), 
                    username = self.config.get("neo","user"),
                    password = self.config.get("neo","passwd")                          
                    )
        neo4j_helper = Neo4jHelper(self.graph)
        count = 0
        
        for data in tqdm(datas):
            properties = data["properties"]
            properties["_key"] = data["_key"]
            properties["alter_names"] = data["alter_names"]
            properties["create_time"] = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            properties["update_time"] = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            properties.pop("leaders")
            properties.pop("shareholder")
            neo4j_helper.upsert_node(self.label, properties)
            count += 1

        logger.info("日期[{}]投资机构构建库共找到记录[{}]个，图数据库更新结点[{}]个".format(
            self.process_date, self.total, count))

if __name__=="__main__":

    # 最早日期 2019-07-03

    pipe = GraphPipeline()
    if len(sys.argv) > 1:
        pipe.process(sys.argv[1])
    else:
        pipe.process("yesterday")