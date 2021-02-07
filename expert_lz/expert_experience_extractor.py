import re 
import os 
from pymongo import MongoClient
from tqdm import tqdm 
import datetime

MongoUrl = "xxx"
# 清洗库
res_kb_process = "xxxx"
res_kb_process_expert = "xxxx"
dir_path = os.path.dirname(__file__)

class ExperienceExtractor(object):
    def __init__(self):
        self.load_experience_position()
        
    def load_experience_position(self):
        with open(os.path.join(dir_path,"experience_position.txt"),"r") as fr:
            datas = fr.readlines()
        datas = list(map(lambda x:x.strip(), datas))
        self.experience_positions = datas 
        
    def split_sentence(self,sentences):
        '''根据时间戳将单句分句'''
        sentences = sentences.replace("(","（").replace(")","）").replace(u"\xa0"," ")
        sentences = re.sub("[~～]","-",sentences) 
        sentences = re.sub(" {3,}"," ", sentences)    
        # 时间后置
        # like 曾任东北大学学科建设处处长（1999-2005），现任东北大学研究院院长（2011-至今）
        if re.findall("（\d{4}", sentences):
            return re.split("[,，]", sentences)
        
        res = re.finditer("[1-2]{1}\d{3}",sentences)
        time_index = [r.span()[0] for r in res]
        split_sentences = []
        
        if len(time_index) < 1:
            return [sentences] 
        
        elif len(time_index) == 1:
            start = time_index[0]
            return [sentences[start:]] 
             
        else:
            cur_index = 1
            time_index.append(len(sentences))
            while cur_index < len(time_index):
                candidate_sentence = sentences[time_index[cur_index-1]:time_index[cur_index]].strip()               
                # 判断是否合并时间点为时间段
                if len(candidate_sentence)<=10 and "-" in candidate_sentence:
                    # 溢出判断
                    if cur_index + 1 >= len(time_index):
                        break 
                    sentence = sentences[time_index[cur_index-1]:time_index[cur_index+1]]
                    cur_index += 2
                    sentence = sentence.strip("，").strip("个人简历").strip()
                    split_sentences.append(sentence)

                else:
                    candidate_sentence = candidate_sentence.strip("个人简历").strip("，").strip()
                    split_sentences.append(candidate_sentence)
                    cur_index += 1

        return split_sentences
    
    def gen_text(self, data):
        resume = ""
        if data["resume"] or data["raw_edu_experience"] or data["raw_work_experience"]:
            if "根据数据搜索表明" not in data["resume"]:
                resume += data["resume"] + "。"
            if data["raw_work_experience"]:
                resume += data["raw_work_experience"] + "。"
            if data["raw_edu_experience"]:
                resume += data["raw_edu_experience"] + "。"
        return resume 
        
    
    def extract(self, resume):        
        extract_with_time = []
        extract_res = []       
        resume_list = []       
        
        # 简介描述为空或纯英文，暂不做抽取
        if (not resume) or (not re.findall("[\u4e00-\u9fa5]",resume)):
            return []
        
        # 中文个人经历抽取
        # 分句
        syn_split_list = re.split("[;；。]", resume)
        for s in syn_split_list:
            s_split = self.split_sentence(s)
            s_split = list(filter(lambda x:len(x)>0, s_split))
            if s_split:
                resume_list.extend(s_split)
        resume_list = list(filter(lambda x:len(x)>0, resume_list))
        # 经历抽取排序
        for sentence in resume_list:
            year_list = re.findall("\d{4}", sentence)
            # 过滤掉出生年月的时间
            if len(year_list)==1 and ("出生" in sentence or "生于" in sentence or "年生" in sentence or "月生" in sentence):
                continue
            if year_list:
                for pos in self.experience_positions:
                    if pos in sentence and [year_list[0],sentence] not in extract_with_time:
                        extract_with_time.append([year_list[0], sentence])
                        break 
        if extract_with_time:
            _sorted = sorted(extract_with_time, key=lambda x:x[0])
            extract_res = [i[1] for i in _sorted]
            
        return extract_res
    

    
            
                
            
            