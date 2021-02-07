#!/home/apollo/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-08-13 17:46
# Filename     : financing_clean.py
# Description  : 融资信息清洗，数据源：清科研究，因果树，鲸准，企查查
# 主要清洗融资轮次，融资金额和字符串、列表的格式化
#******************************************************************************

import sys
import logging
import re
import time
import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
import pymysql

## 投融资轮次的schema表
MYSQL_HOST      =   "xxx"
MYSQL_PORT      =   0
MYSQL_DB        =   "xxx"
MYSQL_USER      =   "xxx"
MYSQL_PASSWD    =   "xxx"

## 事件库的来源和目标库
MONGO_HOST = "xxx"
MONGO_PORT = 0
MONGO_USER = "xxx"
MONGO_PASSWD = "xxx"
SOURCE_MONGO_DB = "xxx"
SOURCE_MONGO_COLLECTION = "xxx"
TARGET_MONGO_DB = "xxx"
TARGET_MONGO_COLLECTION = "xxx"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class FinancingClean(object):
    
    def __init__(self):
        self.finance_round_schema = {}
        sql_connector = pymysql.connect(host=MYSQL_HOST,
                                        port=MYSQL_PORT,
                                        user=MYSQL_USER,
                                        passwd=MYSQL_PASSWD,
                                        db=MYSQL_DB,
                                        charset="utf8mb4")

        query = "select name, alter_names from res_financing_event where type=\"投融资\" OR type=\"上市\""
        sql_cursor = sql_connector.cursor()
        sql_cursor.execute(query)
        for n in sql_cursor.fetchall():
            self.finance_round_schema[n[0]] = n[1].split("|")


    def get_financing_round(self, round):
        ## 为空
        if not round or not len(round.strip()):
            return ""

        round = round.upper()
        for key in self.finance_round_schema:
            for word in self.finance_round_schema[key]:
                ## 如果有@, 则用正则的match匹配, 反之字符匹配
                if "@" in word:
                    if re.match(word[1:], round):
                        return key
                else:
                    if word == round:
                        return key

        return ""


    ## 投资金额的处理
    def get_financing_money(self, price):

        financing_money = None
        ## 为空
        if not price or not len(price.strip()):
            return financing_money
        
        ## 货币种类
        financing_base = 1  ## 默认人民币, 基数为1
        if "人民币" in price or "￥" in price or "CNY" in price:
            financing_base = 1
        elif "美元" in price or "$" in price or "美金" in price or "USD" in price or "美币" in price:
            financing_base = 6.91
        elif "香港元" in price or "港币" in price or "港元" in price or "HKD" in price:
            financing_base = 0.89
        elif "英镑" in price or "GBP" in price:
            financing_base = 9.07
        elif "欧元" in price or "EUR" in price:
            financing_base = 8.16
        elif "日元" in price or "日币" in price or "JPY" in price:
            financing_base = 0.065
        else:
            logger.error("未出现币种, 请检查: {}".format(price))

        ## 货币数值枚举匹配
        ENUM_MONEY = {
            "数千亿":       100000000000,
            "数百亿":       10000000000,
            "数十亿":       1000000000,
            "数亿":         100000000,
            "数千万":       10000000,
            "数百万":       1000000,
            "数十万":       100000,
            "数万":         10000,
            "千亿级":       100000000000,
            "百亿级":       10000000000,
            "十亿级":       1000000000,
            "亿级":         100000000,
            "千万级":       10000000,
            "百万级":       1000000,
            "十万级":       100000,
            "万级":         10000,
            "亿元及以上":    100000000,
            "近亿元":       100000000,
        }

        for m in ENUM_MONEY:
            if m in price:
                financing_money = ENUM_MONEY[m]
                break
        ## 金额正则匹配
        if not financing_money:
            ZH2NUM = {
                "万"        : 10000,
                "十万"      : 100000,
                "百万"      : 1000000,
                "千万"      : 10000000,
                "亿"        : 100000000,
                "十亿"      : 1000000000,
                "百亿"      : 10000000000,
                "千亿"      : 100000000000
            }
            price = price.strip().replace(",", "")
            pattern = r'\d+([\.]\d+)?'
            result = re.search(pattern, price)

            if result:
                money_num = float(result.group(0))
                money_base = 1                  ## 金额单位
                for z in ZH2NUM:
                    if z in price:
                        money_base = ZH2NUM[z]
                        break
                
                money_num *= money_base
                financing_money = money_num

        if financing_money:
            financing_money *= financing_base       ## 乘以汇率
            financing_money = int(financing_money)

        return financing_money
        

    def process(self, date_str):
        source_count = 0                ## 采集库事件数量
        target_count = 0                ## 导入清洗库的数量
        
        process_date = None
        next_date = None
        start_time = time.time()

        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集日期为: {} 的投融资事件的清洗".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)

        connector = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
        admin_db = connector["admin"]
        admin_db.authenticate(MONGO_USER, MONGO_PASSWD)

        source_collection = connector[SOURCE_MONGO_DB][SOURCE_MONGO_COLLECTION]
        target_collection = connector[TARGET_MONGO_DB][TARGET_MONGO_COLLECTION]

        ## 查找某一天采集的投融资事件数据
        results = source_collection.find({"crawl_time": {"$gte": process_date, "$lte": next_date}})
        source_count = results.count()
        logger.info("共找到 {} 条投融资事件需要清洗".format(source_count))

        for result in results:
            ## 判断清洗库中是否存在，如果存在就删除覆盖
            if target_collection.find_one_and_delete({"_id": result["_id"]}):
                logger.info("覆盖原数据, id: {}".format(str(result["_id"])))
            
            ## 投资轮次的清洗
            if not result["financing_round"] or not len(result["financing_round"]):
                financing_round = "未透露"
            else:
                financing_round = self.get_financing_round(result["financing_round"])
                
            if not financing_round:
                logger.error("未找到融资轮次对应表, id: {}, round: {}".format(str(result["_id"]), result["financing_round"]))
                continue

            ## 投资人的清洗, 去除无关投资方以及字段格式的清洗
            investors = []
            if type(result["investor"]) == list:
                for investor in result["investor"]:
                    if type(investor) == list:
                        for x in investor:
                            if x in ["未透露", "投资方未知", "投资方未透露", "投资方未披露", "公开发行"] or len(x.strip()) < 1:
                                continue
                            if x.endswith("领投"):
                                x = x[:-2]
                            if x.endswith("（领投）"):
                                x = x[:-4]
                            investors.append(x)
                    elif type(investor) == str:
                        if investor in ["未透露", "投资方未知", "投资方未透露", "投资方未披露", "公开发行"] or len(investor.strip()) < 1:
                            continue
                        if investor.endswith("领投"):
                            investor = investor[:-2]
                        if investor.endswith("（领投）"):
                            investor = investor[:-4]
                        investors.append(investor)
                    else:
                        logger.error("投资方字段格式错误, id: {}".format(str(result["_id"])))
            elif type(result["investor"]) == str:
                investor = result["investor"]
                if investor in ["未透露", "投资方未知", "投资方未透露", "投资方未披露", "公开发行"] or len(investor.strip()) < 1:
                    pass
                else:
                    if investor.endswith("领投"):
                        investor = investor[:-2]
                    if investor.endswith("（领投）"):
                        investor = investor[:-4]
                    investors.append(investor)
            else:
                logger.error("投资方字段格式错误, id: {}".format(str(result["_id"])))

            for i in investors:
                i = i.replace("\n").strip(" ")


            ## 投资金额的清洗
            financing_money = self.get_financing_money(result["price"].strip())

            doc = {
                "_id":                      result["_id"],                                                  ## id
                "url":                      result["url"],                                                  ## url
                "financing_round":          financing_round,                                                ## 融资轮次
                "investors":                investors,                                                      ## 投资方
                "source":                   result["source"],                                               ## 来源
                "company":                  result["company_name"].strip(),                                 ## 融资企业
                "industry":                 result["industry"] if result["industry"] else "",               ## 行业领域
                "financing_date":           result["financing_date"].strip(),                               ## 融资时间
                "news":                     result["news"],                                                 ## 新闻详情
                "financing_proportion":     result["proportion"],                                           ## 融资占比
                "financing_money":          financing_money,                                                ## 投资金额
                "raw_financing_money":      result["price"].strip(),                                        ## 投资金额原数据
                "crawl_time":               result["crawl_time"],                                           ## 采集时间
                "update_time":              datetime.datetime.now()                                         ## 更新时间
            }

            target_collection.insert_one(doc)
            target_count += 1

        end_time = time.time()
        logger.info("完成采集日期为: {} 的投融资事件清洗, 其中采集库有: {} 条, 导入清洗库: {} 条".format(date_str, source_count, target_count))


if __name__ == "__main__":
    financing_clean = FinancingClean()

    if len(sys.argv) > 1:
        financing_clean.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")
    