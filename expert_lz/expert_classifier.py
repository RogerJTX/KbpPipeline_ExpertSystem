from pymongo import MongoClient
import logging
import os
from tqdm import tqdm 
import copy 
from logging.handlers import RotatingFileHandler
import math 
import numpy as np 
import re 
import datetime

dir_path = os.path.dirname(__file__)

def set_log():
    logging.basicConfig(level=logging.INFO) 
    file_log_handler = RotatingFileHandler(os.path.join(dir_path,"expert_classify_log.txt"), maxBytes=1024 * 1024 * 300, backupCount=10)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)

set_log()
logger = logging.getLogger(__name__)

MONGO_URL = "xxx"
DB = "xxxx"
EXPERT = "xxxx"
PATENT = "xxxx"
PROJECT = "xxxx"
ARTICLE = "xxxx"
PRODUCT_CHAIN = "xxxx"


class ExpertClassifier(object):
    
    def __init__(self):
        # industry_name:str
        # self.industry = industry_name
        self.client = MongoClient(MONGO_URL)       
        self.expert = self.client[DB][EXPERT]
        self.patent = self.client[DB][PATENT]
        self.project = self.client[DB][PROJECT]
        self.article = self.client[DB][ARTICLE]
        self.label = {}
        self.id2name = {}
        self.expert_max_abs = {}
        self.expert_max_normal = {}
        self.expert_min_normal = {}
        self.load_label()
        
        
    def load_label(self):
        client = MongoClient(MONGO_URL)
        collection = client[DB][PRODUCT_CHAIN]
        nodes = collection.find({"is_match":1})
        for node in nodes:
            words = [node["name"]] + node["alter_names"] 
            # ATTENTION: 正则归一化小写，留意 \w 需要小写 
            words = [ w.lower() if re.findall("[\u4e00-\u9fa5]", w) else "[^A-Z]"+w.lower()+"[^A-Z]"  for w in words ] # 英文大小写归一化       
            self.label[node["_id"]] = words
            self.id2name[node["_id"]] = node["name"]
            self.expert_max_normal[node["_id"]] = node["expert_max_normal"]
            self.expert_min_normal[node["_id"]] = node["expert_min_normal"]
        logger.info("产业产品匹配节点加载完成")
        client.close()


    def classify(self, text):
        '''
        Function
        --------
        classify expert description text to industry product labels
        
        Parameter
        ---------
        text: resources text
        
        Return
        ---------
        industry product labels matched, 
        type List<Dict>, contains product_id, product_name, count
        
        '''
        res = []
        label_count = {}
        text = text.strip().lower() # 英文大小写归一化小写
        
        # 产品词频统计
        for key, words in self.label.items():
            count = 0
            for word in words:
                count += len(re.findall(word, text))
            if count > 2:
                label_count[key] = count
                      
        label_count = sorted(label_count.items(), key=lambda x: x[1], reverse=True)
        
        for k,v in label_count:           
            # 组装产品节点结果
            item = {
                "product_id": k,
                "product_name": self.id2name[k],
                "product_count": v
            }
            res.append(item)
        
        return res
    
    def process_stat_all(self):
        
        # 计算专家关联产品节点词频
        # experts = self.expert.find({"properties.resource_count":{"$exists":False}}, no_cursor_timeout=True).sort([("_id",1)])
        experts = self.expert.find({},no_cursor_timeout=True).sort([("_id",1)])
        for expert in tqdm(experts):
            text, resource_count = self.gen_expert_corpus(expert)                           
            ## 文本产品词频统计          
            product_nodes = self.classify(text)
            update = {
                "properties.product_stat": product_nodes,
                "properties.resource_count": resource_count,
                "update_time": datetime.datetime.today()
            }
            self.expert.update_one({"_id":expert["_id"]},{"$set": update})
        
        # 统计产品节点词频分布范围    
        self.stat_save_max()
    
        
    def stat_save_max(self):
        client = MongoClient(MONGO_URL)
        collection = client[DB][PRODUCT_CHAIN]
        nodes = collection.find({"is_match":1})
        # 节点上专家分布统计
        for node in tqdm(nodes):
            node_counts = []
            node_id = node["_id"]
            datas = self.expert.find({"properties.product_stat.product_id": node_id})
            for data in datas:
                node_count = list(filter(lambda x:x["product_id"] == node_id, data["properties"]["product_stat"]))[0]["product_count"]
                node_counts.append(node_count)
            node_counts.sort()
            
            # 计算某一节点的专家词频分布范围
            if node_counts:
                mean = np.mean(node_counts)
                std = np.std(node_counts)
                expert_max_normal = int(mean + 2 * std)
                expert_min_normal = max(int(mean - 2 * std),0)
                
            else:
                node_counts = [0]
                expert_max_normal = 0
                expert_min_normal = 0
                
            # 更新节点正常范围    
            self.expert_max_normal[node_id] = expert_max_normal
            self.expert_min_normal[node_id] = expert_min_normal
            collection.update_one({"_id":node_id},{"$set":{"expert_max_normal":expert_max_normal, "expert_min_normal":expert_min_normal, "expert_max_abs":node_counts[-1]}})
        
        client.close()    
        logger.info("节点正常范围分布更新完成")
        
    def gen_expert_corpus(self, data):
        # 待处理文本
        text = ""
        resource_count = 0
        kids = data["properties"]["kId"]  
        
        # 各个KID数据融合统计    
        for kid in kids:
            # 专利数据
            patents = self.patent.find({"properties.authors.kId":kid})
            for patent in patents:
                resource_count += 1
                text += patent["properties"]["title"]
                if patent["properties"]["abstract"]:
                    text += "。" + patent["properties"]["abstract"]
                    
            # 论文数据
            articles = self.article.find({"properties.authors.kId":kid})
            for article in articles:
                resource_count += 1
                text += article["properties"]["title"]
                if article["properties"]["abstract"]:
                    text += "。" + article["properties"]["abstract"]
                    
            # 科研课题数据
            projects = self.project.find({"properties.authors.kId":kid})
            for project in projects:
                resource_count += 1
                text += project["properties"]["title"]
                if project["properties"]["abstract"]:
                    text += "。" + project["properties"]["abstract"]  
                    
        return text, resource_count      
    
    def process_one(self, data):
        # 主函数：迭代专家进行统计 --》 分布情况统计 --》 评分
        
        text, resource_count = self.gen_expert_corpus(data)
                        
        ## 文本产品词频统计          
        product_nodes = self.classify(text)
        update = {
                "properties.product_stat": product_nodes,
                "properties.resource_count": resource_count,
                "update_time": datetime.datetime.today()
            }
        self.expert.update_one({"_id":data["_id"]},{"$set": update})
        expert_product_relations = self.score_one(update)
        return expert_product_relations
        
        
    def cal_confidence(self, node):
        # 按产品频次绝对值打分  指标归一
        max_abs_normal = self.expert_max_normal[node["product_id"]]
        cur_abs = node["product_count"]
        if cur_abs >= max_abs_normal:
            score = 1.0
        else:
            score = round(cur_abs/max_abs_normal, 3) 
        return score

        
    def score_one(self, data):
        if "properties.product_stat" in data:
            product_nodes = data["properties.product_stat"]
        else:
            product_nodes = data["properties"]["product_stat"]
        expert_product_relations = []
        
        # 阈值判定及评分
        for i, node in enumerate(product_nodes):
            threshold = self.expert_min_normal[node["product_id"]]
            if node["product_count"] > threshold:
                node["score"] = self.cal_confidence(node)
                relation = {
                    "relation_type":"concept_relation/100011",
                    "object_name":node["product_name"],
                    "object_type": "industry_product",
                    "object_id": node["product_id"],
                    "relation_score": node["score"]
                }
                expert_product_relations.append(relation)
       
        expert_product_relations.sort(key = lambda x: x["relation_score"], reverse=True)                  
        return expert_product_relations
        
# if __name__ == "__main__":
#     scorer = ExpertClassifier()
#     scorer.process_stat_all()