#coding: utf-8
## 根据简介更新抽取字段值和属性完整度
from pymongo import MongoClient
from tqdm import tqdm
import datetime
import re
from bson import ObjectId
import os
import sys
import pymysql
from dateutil import parser
import logging 
main_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(main_path)
from expert_experience_extractor import ExperienceExtractor
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
    )

dir_path = os.path.dirname(__file__)

MongoUrl = "xxx"
# 清洗库
res_kb_process = "xxxx"
res_kb_process_expert = "xxxx"

# 人工梳理库MySQL
mysql_config = {
    'host': 'xxx',
    'port': 0,
    'user': 'xxx',
    'password': 'xxx',
    'db': 'xxx',
    'charset': 'xxxx',
    'cursorclass': pymysql.cursors.DictCursor
}
prof_title_query = "select name from res_prof_title"

class ExpertPropExtractor(object):
    def __init__(self):
        self.client = MongoClient(MongoUrl)
        self.res_kb_process_expert_lz = self.client[res_kb_process][res_kb_process_expert]
        self.load_honors()
        self.load_awards()
        self.exprience_extractor = ExperienceExtractor()
        
    def load_honors(self):
        with open(os.path.join(dir_path,"honor.txt"), "r") as fr:
            honors = fr.readlines()     
        honors = list(map(lambda x:x.strip(), honors))
        self.honors = honors
        self.honors.sort(key = lambda x:len(x), reverse= True)
        
    def load_awards(self):
        with open(os.path.join(dir_path,"award.txt"),"r") as fr:
            awards = fr.readlines()
        self.awards = list(map(lambda x: x.strip(), awards))
        self.awards.sort(key=lambda x:len(x), reverse=True)
        
    def query_daily_data(self, process_date):
        if process_date == "yesterday":
            process_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        elif process_date == "today":
            process_date = datetime.today().strftime("%Y-%m-%d")
            
        elif len(process_date.split("-")) == 3:
            process_date = process_date
            
        else:
            raise Exception("无效参数的日期")
        
        self.process_date = process_date
        iso_date_str = process_date + 'T00:00:00+08:00'
        iso_date = parser.parse(iso_date_str)
        res = self.res_kb_process_expert_lz.find({"update_time": {"$gte": iso_date}, "$or":[{"resume":{"$ne":""}},{"raw_edu_experience":{"$ne":""}}]}).sort([("_id",1)])
        return res

    def process_birthday(self, resume,name):
        birthday = ""   
        patterns = [
            "(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)生",
            "(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)出生",
            "(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)生于"
            "出生年月：?(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)",
            name + "\((\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)",
            "生于(\d{4}[年.-]?\d{,2}[月.-]?\d{,2}日?)"
        ]
        
        for pattern in patterns:
            birthdays = re.findall(pattern, resume)
            if len(birthdays)>0:
                birthday = birthdays[0]
                break
            
        # birthday = re.sub("[年月日.]","-",birthday).strip("-")   
        return birthday
        
    
    
    def process(self, process_date):
        datas = self.query_daily_data(process_date)
        for data in tqdm(datas):
            resume = ""
            if data["resume"] and "根据数据搜索表明" not in data["resume"]:
                resume = data["resume"] + "。"               
            if data["raw_edu_experience"]:
                resume += data["raw_edu_experience"]
                
            update = {}           
            # 性别
            if (not data["gender"]) and "男" in resume:
                update["gender"] = "男"
            elif (not data["gender"]) and "女" in resume:
                update["gender"] = "女"
                
            # 出生日期
            if not data["birthday"]:
                name = data["name"]
                birthday = self.process_birthday(resume, name)
                if birthday:
                    update["birthday"] = birthday
                
            # 最高学位
            final_edu_degree = ""
            edu_degree = ["博士","硕士","学士","本科"]
            for degree in edu_degree:
                if degree in resume:
                    final_edu_degree = degree
                    break 
            
            if final_edu_degree == "本科":
                final_edu_degree = "学士"      
            if final_edu_degree:
                update["final_edu_degree"] = final_edu_degree
                
            # 荣誉标签
            expert_honors = []
            for h in self.honors:
                matches = re.findall(h, resume)
                for match in matches:
                    if match not in expert_honors:
                        expert_honors.append(match)
            if expert_honors:
                update["honors"] = expert_honors
                
            # 所获奖项
            expert_awards = []        
            resume = resume.replace("科学技术","科技")
            for a in self.awards:
                matches = re.findall(a, resume)
                for m in matches:
                    if m not in expert_awards:
                        expert_awards.append(m)
            if expert_awards:
                update["awards"] = expert_awards
                
            # 个人经历
            expriences = []
            text = self.exprience_extractor.gen_text(data)
            expriences = self.exprience_extractor.extract(text)
            if expriences:
                update["experiences"] = expriences
            
            if update:  
                update["update_time"] = datetime.datetime.today()
                # 更新数据
                self.res_kb_process_expert_lz.update_one({"_id":data["_id"]},{"$set":update})
                logging.info("更新专家数据成功，专家ID=[{}]".format(data["_id"]))
                
            
if __name__ == "__main__":
    extractor = ExpertPropExtractor()
    if len(sys.argv) > 1:
        extractor.process(sys.argv[1])
    else:
        extractor.process("yesterday")