#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-09-03 15:37
# Filename     : invest_institution_kbp.py
# Description  : 投资机构kbp
#******************************************************************************

import sys
import logging
import time
from tqdm import tqdm
import datetime
from pymongo import MongoClient
from pyArango.connection import Connection as ArangoConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 投资机构数据库
MONGO_HOST = "xxx"
MONGO_PORT = 0
MONGO_DB = "xxx"
MONGO_COLLECTION = "xxx"
MONGO_USER = "xxx"
MONGO_PASSWD = "xxx"

## 导入目标arangodb数据库
ARANGO_URL = "xxx"
ARANGO_USER = "xxx"
ARANGO_PASSWD = "xxx"
ARANGO_DB = "xxx"
ARANGO_INVEST_INSTITUTION_COLLECTION = "xxxx"      ## 投资机构库

class InvestInstitutionKBP:

    def process(self, date_str):
        process_date = None
        next_date = None

        ## 默认处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集时间为: {} 的投资机构kbp".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
  
        start_time = time.time()
        mongo_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
        admin_db = mongo_client["admin"]
        admin_db.authenticate(MONGO_USER, MONGO_PASSWD)
        mongo_collection = mongo_client[MONGO_DB][MONGO_COLLECTION]
        
        arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                            username=ARANGO_USER,
                                            password=ARANGO_PASSWD)
        arango_db = arango_connector[ARANGO_DB]

        results = mongo_collection.find({"crawl_time": {"$gte": process_date, "$lte": next_date}}, no_cursor_timeout=True, batch_size=50)
        # results = mongo_collection.find(no_cursor_timeout=True, batch_size=50)
        source_count = 0
        target_count = 0

        for result in tqdm(results):
            source_count += 1
            
            ## 组装
            doc = {}
            doc["_key"] = str(result["_id"])
            doc["name"] = result["name"]
            doc["alter_names"] = []
            doc["alter_names"].append(result["short_name"])
            doc["alter_names"].append(result["name_en"])
            doc["create_time"] = result["crawl_time"]
            doc["update_time"] = result["crawl_time"]

            doc["properties"] = {
                "short_name":           result["short_name"],
                "name_en":              result["name_en"],
                "establish_date":       result["establish_date"],
                "type":                 result["type"],
                "description":          result["description"],
                "url":                  result["url"],
                "logo":                 result["logo"],
                "source":               result["source"],
                "phone":                result["phone"],
                "email":                result["email"],
                "management_capital":   result["management_capital"],
                "funds":                result["funds"],
                "members":              result["members"]
            }

            doc["tags"] = []
            doc["relations"] = []

            ## 导入arangodb，覆盖重复数据
            try:
                ## 删除同id数据
                arango_collection = arango_db[ARANGO_INVEST_INSTITUTION_COLLECTION]
                query = arango_collection.fetchByExample({"_key": doc["_key"]}, batchSize=1)
                for q in query:
                    q.delete()

                arango_collection.createDocument(doc).save()
                target_count += 1
            except Exception as e:
                logger.error("导入arangodb错误, id: {}".format(doc["_key"]))

        end_time = time.time()
        logger.info("本次投资机构kbp完成, 耗时: {} 秒".format(int(end_time - start_time)))
        logger.info("其中清洗库有: {} 条数据, 导入arangodb有 {} 条数据".format(source_count, target_count))


if __name__ == "__main__":
    investInstitutionKBP = InvestInstitutionKBP()
    if len(sys.argv) > 1:
        investInstitutionKBP.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")