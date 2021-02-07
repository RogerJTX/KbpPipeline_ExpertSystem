# 生成待处理公司数据源
import configparser
from pymongo import MongoClient
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)




config = configparser.ConfigParser()
config.read("config.ini")
mongo_con = MongoClient(config.get("mongo","mongo_url"))
# recommend_company = mongo_con[config.get("mongo","recommend_db")][config.get("mongo","recommend_company")]
company_label = mongo_con[config.get("mongo","info_db")][config.get("mongo","company_label")]

process_companys = set()

query = {
"sector_necar":{
    "$ne":[]
}
}

company_res = company_label.find(query)
for company in company_res:
    process_companys.add(company["company_name"])

sorted_process_companys = list(process_companys)
sorted_process_companys.sort()
with open("necar_companys.txt","w") as fw:
    fw.write("\n".join(sorted_process_companys))

logger.info("新能源企业生成完成，共[{}]家企业".format(len(sorted_process_companys)))
