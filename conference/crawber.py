#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-13 15:34
# Filename     : conference_crawber.py
# Description  : res_kb_conference会议信息生成，目前是转移企业相关的会议信息，实际这一步是用爬虫替换
#******************************************************************************

import configparser
import sys
from pymongo import MongoClient
from pymongo import errors
import pymysql
from dateutil import parser
from datetime import datetime, date, timedelta
import json
import logging
import re
import copy
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path,"config.ini")

class ConferenceCrawber(object):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo","mongo_url"))
        self.crawber_conference = self.mongo_con[self.config.get("mongo","conference_db")][self.config.get("mongo","crawber_conference")]
        self.res_kb_conference = self.mongo_con[self.config.get("mongo","res_kb_db")][self.config.get("mongo","res_kb_conference")]
        self.transfer_count = 0


    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    
    def process(self):

        process_conferences = self.crawber_conference.find({})


        for i, conference in enumerate(process_conferences):

            if conference:
                insert_res = self.res_kb_conference.insert_one(conference)
                self.transfer_count += 1
            if( i%100 == 0):
                logger.info("开始处理第{}个会议".format(i)) 

        self.close_connection() 


if __name__=="__main__":
    cc = ConferenceCrawber()
    cc.process()
