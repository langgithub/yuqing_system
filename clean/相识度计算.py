# encoding: utf-8
"""
--------------------------------------
@describe 
@version: 1.0
@project: yuqing_system
@file: 相识度计算.py
@author: yuanlang 
@time: 2019-08-06 15:13
---------------------------------------
"""
import jieba
# import Levenshtein
import difflib
import numpy as np
import pymysql

# jieba.load_userdict("dict.txt")

class StrSimilarity():

    __stop_words=["苑","园","大厦","大街","None","公寓","里","花园","公园","小区","期","区"]

    def __init__(self, word):
        self.word = word

    # def stopwordslist(self,filepath):
    #     stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
    #     return stopwords

    def stopwordslist(self, filepath):
        stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
        return stopwords

    # 对句子去除停用词
    def movestopwords(self,sentence):
        # stopwords = self.stopwordslist('语料/hlt_stop_words.txt')  # 这里加载停用词的路径
        outstr = ''
        for word in sentence:
            if word not in self.__stop_words:
                if word != '\t' and '\n':
                    outstr += word
                    # outstr += " "
        return outstr

    # Compared函数，参数str_list是对比字符串列表
    # 返回原始字符串分词后和对比字符串的匹配次数，返回一个字典
    def Compared(self, str_list):
        dict_data = {}
        sarticiple = self.movestopwords(jieba.cut(self.word.strip()))
        for strs in str_list:
            num = 0
            for sart in sarticiple:
                if sart in strs:
                    num = num + 1
                else:
                    num = num
            dict_data[strs] = num
        return dict_data

    # NumChecks函数，参数dict_data是原始字符串分词后和对比字符串的匹配次数的字典，也就是Compared函数的返回值
    # 返回出现次数最高的两个，返回一个字典
    def NumChecks(self, dict_data):
        list_data = sorted(dict_data.items(), key=lambda asd: asd[1], reverse=True)
        length = len(list_data)
        json_data = {}
        if length >= 2:
            datas = list_data[:2]
        else:
            datas = list_data[:length]
        for data in datas:
            json_data[data[0]] = data[1]
        return json_data

    # MMedian函数，参数dict_data是出现次数最高的两个对比字符串的字典，也就是NumChecks函数的返回值
    # 返回对比字符串和调节值的字典
    def MMedian(self, dict_data):
        median_list = {}
        length = len(self.word)
        for k, v in dict_data.items():
            num = np.median([len(k), length])
            if abs(length - num) != 0:
                # xx = (1.0/(abs(length-num)))*0.1
                xx = (abs(length - num)) * 0.017
            else:
                xx = 0
            median_list[k] = xx
        return median_list

    # Appear函数，参数dict_data是对比字符串和调节值的字典，也就是MMedian函数的返回值
    # 返回最相似的字符串
    def Appear(self, dict_data):
        json_data = {}
        for k, v in dict_data.items():
            fraction = difflib.SequenceMatcher(None, self.word, k).quick_ratio() - v
            json_data[k] = fraction
        tulp_data = sorted(json_data.items(), key=lambda asd: asd[1], reverse=True)
        return tulp_data[0]


def main(can_zhao_biao,mu_biao_biao):
    conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", passwd="lang1994", db="yuqing_db", charset="utf8")
    cursor = conn.cursor()

    # cursor.execute("select url,title from seed;")
    # lj_items = cursor.fetchall()
    # str_list = []
    # _dict = {}
    # for lj_item in lj_items:
    #     aim = "{0}{1}{2}{3}{4}".format(lj_item[1], lj_item[2], lj_item[3], lj_item[4], lj_item[4])
    #     str_list.append(aim)
    #     _dict[aim] = lj_item[0]

    str_list="2018年10月7日达川区南外济民医院门口突然塌陷事件"

    while True:
        cursor.execute("select url,title from seed limit 1")
        f5_items = cursor.fetchall()
        if len(f5_items) == 0:
            break

        query_str,query_id= '',''
        for f5_item in f5_items:
            query_id=f5_item[0]
            query_str = f5_item[1]

        ss = StrSimilarity(query_str)
        list_data = ss.Compared(str_list)
        num = ss.NumChecks(list_data)
        mmedian = ss.MMedian(num)
        print(query_str+"  ===>  "+ss.Appear(mmedian)[0]+":"+str(ss.Appear(mmedian)[1]))

        # sql="update %s set lj_xiaoqu_id='%s',ration=%12.10f where xiaoqu_id='%s'"%\
        #     (mu_biao_biao,_dict[ss.Appear(mmedian)[0]],ss.Appear(mmedian)[1],query_id)
        # cursor.execute(sql)
        # conn.commit()

if __name__ == "__main__":
    #参照表hs_community_dict_fang
    can_zhao_biao="poi_ration"
    #目标表
    mu_biao_biao="shop_ration"
    main(can_zhao_biao,mu_biao_biao)