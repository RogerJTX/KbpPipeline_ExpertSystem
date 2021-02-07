#!/home/apollo/anaconda3/bin/python3
# -*- coding: utf-8 -*-
# ******************************************************************************
# Author       : jtx
# Last modified: 2020-10-12 11:09
# Filename     : expert_project_kId_clean.py
# Description  : 科研项目共同作者添加KID
# ******************************************************************************
import logging
import requests
from pymongo import MongoClient
import datetime
import re
import configparser
import sys
from dateutil import parser
import os
from logging.handlers import RotatingFileHandler
import copy
from tqdm import tqdm

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

dir_path = os.path.dirname(__file__)
kbp_path = os.path.dirname(dir_path)
config_path = os.path.join(kbp_path, "config.ini")


def set_log():
    logging.basicConfig(level=logging.INFO)
    file_log_handler = RotatingFileHandler(os.path.join(dir_path, "expert_project_kid_log.txt"),
                                           maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)


set_log()
logger = logging.getLogger(__name__)


class ProjectClean(object):

    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.mongo_con = MongoClient(self.config.get("mongo", "mongo_url"))
        self.res_kb_process_expert_project = self.mongo_con[self.config.get("mongo", "res_kb_process")][
            self.config.get("mongo", "process_expert_project")]  # 科研项目清洗库
        self.res_kb_expert_index = self.mongo_con[self.config.get("mongo", "res_kb_db")][
            self.config.get("mongo", "res_kb_expert_index")]
        self.count_update = 0
        self.load_university()

    def load_university(self):
        self.university = {}
        with open(os.path.join(dir_path, "expert_university_en_zh.txt"), "r") as fr:
            lines = fr.readlines()
        for line in lines:
            name_cn, name_en = line.strip().split("\t")
            name_cn = name_cn.replace("（", "(").replace("）", ")")
            name_en = name_en.lower()
            self.university[name_cn] = name_en

        logger.info("高校信息加载完成")

    def process_kId(self, author):
        '''给author添加KID，返回KID字符串'''
        university = author.get("university", "")
        name = author["name"]
        research_institution = author["research_institution"]
        kId = ""

         # 优先用university匹配
        if university:
            research_institution = university 
        regex_str = ".*" + research_institution

        # 查找库中已有数据
        exist_data = self.res_kb_process_expert_project.find_one({"authors": {
            "$elemMatch": {"name": name, "research_institution": {"$regex":regex_str}, "kId": {"$regex": ".{2,}"}}}})
        if exist_data:
            kId = list(filter(lambda x: x["name"] == name and "research_institution" in x and re.findall(regex_str, x["research_institution"]), exist_data["authors"]))[0]["kId"]
       
        # 查找index中数据
        else:
            index_data = self.res_kb_expert_index.find_one({"expert_name": name, "research_institution": {"$regex":regex_str}})
            if index_data:
                kId = index_data["kId"]

        return kId

    def query_daily_data(self, crawl_date):
        if crawl_date == "yesterday":
            crawl_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        elif crawl_date == "today":
            crawl_date = datetime.today().strftime("%Y-%m-%d")

        elif len(crawl_date.split("-")) == 3:
            crawl_date = crawl_date

        else:
            raise Exception("无效参数的日期")

        self.process_date = crawl_date
        iso_date_str = crawl_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_process_expert_project.find({"crawl_time": {"$gte": iso_date}, "authors.kId": ""}, no_cursor_timeout=True).sort(
            [("_id", 1)])
        return res

    def close_connection(self):
        if self.mongo_con:
            self.mongo_con.close()

    def process(self, crawl_date):
        '''
        科研项目数据作者KID更新
        '''

        count = 0
        docs = self.query_daily_data(crawl_date)
        total = docs.count()
        logging.info("日期[{}]查到待处理数据[{}]条".format(self.process_date, total))

        for doc in tqdm(docs):
            count += 1
            authors = doc["authors"]
            new_authors = []
            for author in authors:
                # 添加高校名称字段
                author["university"] = ""
                for universe in self.university:
                    if universe in author["research_institution"]:
                        author["university"] = universe
                        break

                # 添加KID
                if (not author["kId"]) and author["research_institution"]:
                    author["kId"] = self.process_kId(author)

                new_authors.append(author)

            update = {"authors": new_authors,"update_time":datetime.datetime.today()}
            result = self.res_kb_process_expert_project.update_one({"_id": doc["_id"]},
                                                                  {"$set": update})
            logger.info("科研项目[{}]更新作者kId完成".format(doc["_id"]))
            self.count_update += result.modified_count

        logging.info("[{}]科研项目数据KID添加完毕，共找到科研项目[{}]条，更新[{}]条".format(
            self.process_date, total, self.count_update))
        self.close_connection()


if __name__ == "__main__":

    # 科研项目最早爬虫日期为 2020-09-14

    cleaner = ProjectClean()
    if len(sys.argv) > 1:
        cleaner.process(sys.argv[1])
    else:
        cleaner.process("yesterday")
