# encoding: utf-8
"""
--------------------------------------
@describe 
@version: 1.0
@project: yuqing_system
@file: 关键字提取.py
@author: yuanlang 
@time: 2019-08-07 10:00
---------------------------------------
"""
import jieba
import pymysql
import pandas as pd
import gensim
import numpy
from gensim import corpora, models, similarities

# 导入停用词
stopwords=pd.read_csv("stopwords.txt",index_col=False,quoting=3,sep="\t",names=['stopword'], encoding='utf-8')
stopwords=stopwords['stopword'].values

# 读取新闻内容
# df = pd.read_csv("./data/technology_news.csv", encoding='utf-8')
# df = df.dropna()
# lines=df.content.values.tolist()

conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", passwd="lang1994", db="yuqing_db", charset="utf8")
cursor = conn.cursor()
cursor.execute("select * from context")
lines=cursor.fetchall()


def word_count(lines,stopwords):
    # 词频统计
    segment = []
    for line in lines:
        try:
            text = line[1].replace("\n", "").replace(" ", "").replace("\t", "")
            segs = jieba.__lcut(text)
            for seg in segs:
                if len(seg) > 1 and seg != '\r\n' and seg not in stopwords:
                    segment.append(seg)
            # print(segment)
        except Exception as e:
            print(e)

    words_df = pd.DataFrame({'segment': segment})
    words_stat = words_df.groupby(by=['segment'])['segment'].agg(["size"])
    words_stat = words_stat[1300:]
    # print(words_stat)
    words_stat = words_stat.reset_index().sort_values(by=["size"], ascending=False)
    print(words_stat[:1500])

word_count(lines,stopwords)

def lda(lines,stopwords):
    sentences = []
    for line in lines:
        try:
            text = line[1].replace("\n", "").replace(" ", "").replace("\t", "")
            segs = jieba.__lcut(text)
            segs = filter(lambda x: len(x) > 1, segs)
            segs = [seg for seg in list(segs) if seg not in stopwords]
            sentences.append(segs)
        except Exception as e:
            print(e)

    # 词袋模型
    dictionary = corpora.Dictionary(sentences)
    corpus = [dictionary.doc2bow(_sentence) for _sentence in sentences]
    lda = gensim.models.ldamodel.LdaModel(corpus=corpus, id2word=dictionary, num_topics=20)

    # print(lda.print_topic(3, topn=5))
    for topic in lda.print_topics():
        print (topic)

lda(lines,stopwords)
