#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-04-27 15:08
# Filename     : expert_arango2hbase.py
# Description  : 专家学者实体；从分区中读取数据调用pipeline处理，并存入HBASE结构化文档库；每天跑数据
#******************************************************************************

from pymongo import MongoClient
import happybase
from pyhive import hive
from dateutil import parser
import requests
import sys
from tqdm import tqdm
import json
import logging
from logging.handlers import RotatingFileHandler
import configparser
from datetime import datetime,  date, timedelta
import pyArango.connection as ArangoDb
from pyArango.theExceptions import AQLFetchError
import pymysql
import re
import os


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

class EntityPipeline(object):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.init_concpet_classid() # 加载实体类别定义关键词映射
 
        self.nlpipe_count = 0 # 经过PIPE的实体数
        self.hbase_count = 0 # 导入HBASE的实体数
        self.hbase_dupl_count = 0 # 监测rowkey已存在的实体数
        self.no_hbase_count = 0 # 找不到HBASE rowkey的实体，分类去重颠倒导致
        self.hive_count = 0 # 导入HIVE实体数
        self.duplicate_count = 0 # PIPE去重模块检测到的重复实体数
        self.ignore_count = 0 # 无关实体数
        self.hive_dupl_count = 0 # 监测HIVE中已存在日志的实体数

    def init_concpet_classid(self):
        '''
        加载MYSQL中定义的实体类别表
        '''
        self.news_class_ids = {}
        sql_conn = pymysql.connect( host = self.config.get("mysql","host") ,
                            user = self.config.get("mysql","user") ,
                            passwd = self.config.get("mysql","passwd"),
                            port = self.config.getint("mysql","port") ,
                            db = self.config.get("mysql","db"),
                            charset = "utf8" )
        sql_cur = sql_conn.cursor()
        sql_state = self.config.get("mysql","entity_type_query").replace("eq","=")
        sql_cur.execute(sql_state)
        mysql_res = sql_cur.fetchall()
        for name, class_id, patent_id in mysql_res:
            self.news_class_ids[name] = class_id
        logger.info("MYSQL实体类别定义加载完成")
        sql_cur.close()
        sql_conn.close()

    def process_expert_hbase_data(self,item):
        ''' 将读取的arango记录转换成hbase存储结构'''

        classid = self.news_class_ids['专家学者']
        rowkey_str = classid+'|'+item['_key']
        rowkey = bytes(rowkey_str, encoding="utf8")
        alter_names = []
        tags = []
        crawl_t = ""
        if 'crawl_time' in item:
            craw_t = item["properties"]["crawl_time"]
        if 'alter_names' in item:
            alter_names = item['alter_names']
        if 'tags' in item:
            tags = item['tags']
        column_family = {#专家学者暂无别名 默认空
            b"info:alter_names": bytes(json.dumps(alter_names, ensure_ascii=False), encoding="utf8"),
            b"info:entity_type_id": bytes(classid, encoding="utf8"),
            b"info:entity_name": bytes(item["name"], encoding="utf8"),
            b"info:uuid": bytes(item["_key"], encoding="utf8"),
            b"info:entity_properties": bytes(json.dumps(item["properties"], ensure_ascii=False), encoding="utf8"),#.getStore() for doc
            b"info:tags": bytes(json.dumps(tags, ensure_ascii=False), encoding="utf8"),
            b"info:relations": bytes(json.dumps(item["relations"], ensure_ascii=False), encoding="utf8"),
            #b"info:crawl_time": bytes(json.dumps(crawl_t), encoding="utf8"),
            b"info:insert_time": bytes(datetime.today().strftime("%Y-%m-%d %H:%M:%S"), encoding="utf8")
        }
        return rowkey,column_family

    def link_arangoDb(self, date_str, end_date=""):

        logging.info("正在处理[专家] arango数据...")
        query_time = ""
        colletion_name = "xxxx"
        if date_str == "xxxx":
            process_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = date.today().strftime("%Y-%m-%d")
            #end_date = datetime.datetime.today().strftime("%Y-%m-%d")
        elif len(date_str.split("-")) == 3:
            process_date = date_str
        else:
            raise Exception("无效日期参数") 

        query_time = parser.parse(process_date+"T00:00:00+08:00")
        if end_date:
            end_process_date = end_date
            iso_end_date_str = end_process_date + 'T00:00:00+08:00'
            iso_end_date = parser.parse(iso_end_date_str)

        if date_str == "yesterday" or end_date:
            aql_arango = "FOR expert IN {} FILTER expert.update_time >= '{}' and expert.update_time < '{}' return expert".format(
                        self.config.get("arango","kb_expert"), query_time,iso_end_date)
        else:
            aql_arango = "FOR entity IN %s FILTER entity.update_time >= '%s' return entity" % (colletion_name,query_time)

        conn = ArangoDb.Connection(\
                        arangoURL=self.config.get("arango","arango_url"),
                        username=self.config.get("arango","user") ,
                        password=self.config.get("arango","passwd")
                    )

        db_name = conn[self.config.get("arango","db")]
        collection = db_name[colletion_name]
        #if "startTime"==data_str:
        #    aql_arango = "FOR entity IN %s return entity" % colletion_name
        try:
            query = db_name.fetch_list(aql_arango)
        except AQLFetchError as e:
            query = []
            logger.warn("Arango[专家] 库没有查到数据",e)

        logger.info("HBASE共含有[专家] %s条记录，本次查询%s条记录！" % (collection.figures()['count'],len(query)))
        num = 0
        num_ins = 0
        pre_send_rowkey = []
        pre_send_data = []

        for item in query:
            num += 1
            ####################################################
            rowkey, data = self.process_expert_hbase_data(item)#
            ####################################################
            if rowkey not in pre_send_rowkey:
                pre_send_rowkey.append(rowkey)
                pre_send_data.append(data)
        zip_list = zip(pre_send_rowkey, pre_send_data)
        logger.info("数据封装完毕，开始写入hbase...")
        self.hbase_pool = happybase.ConnectionPool( host = self.config.get("hbase","host"),
                        port = self.config.getint("hbase","port"),
                        timeout=None,
                        autoconnect=True,
                        size = self.config.getint("hbase","pool_size"))

        with self.hbase_pool.connection() as hbase_conn:
            self.hbase_entity_table = hbase_conn.table(self.config.get("hbase","entity"))

            with self.hbase_entity_table.batch(batch_size=200) as bat:
                for send_rowkey, send_data in zip_list:
                    bat.put(send_rowkey, send_data)
                    self.hbase_count += 1
                    if self.hbase_count % 500 == 0:
                        logger.info("已经写入写入hbase...{}条".format(self.hbase_count))

            #print(rowkey,data)
        logger.info("HBASE批量数据导入完成,共插入%s条专家学者记录" % self.hbase_count)

if __name__ == '__main__':
    entity_pipe = EntityPipeline()
    if len(sys.argv) > 1:
        tmp_param = sys.argv[1]
    else:
        tmp_param = "yesterday"
    entity_pipe.link_arangoDb(tmp_param)
