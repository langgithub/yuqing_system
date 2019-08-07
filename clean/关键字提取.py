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
import matplotlib.pyplot as plt
from wordcloud import WordCloud#词云包
from gensim import corpora, models, similarities
# 编码问题
plt.rcParams['figure.figsize'] = (5.0, 5.0)
plt.rcParams['font.sans-serif'] = ['simhei']
plt.rcParams['axes.unicode_minus'] = False



# 导入停用词
stopwords=pd.read_csv("stopwords.txt",index_col=False,quoting=3,sep="\t",names=['stopword'], encoding='utf-8')
stopwords=stopwords['stopword'].values

# 读取新闻内容
df = pd.read_csv("type.csv", encoding='utf-8',sep = '&@@&')
x=0
lines=[((++x),item) for item in df.content.values.tolist()]

# 原始数据
# conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", passwd="lang1994", db="yuqing_db", charset="utf8")
# cursor = conn.cursor()
# cursor.execute("select * from context")
# lines=cursor.fetchall()

def db_to_csv(lines):
    """保存到本地"""
    with open("type.csv","w",encoding="utf-8") as f:
        f.writelines("url&@@&content\n")
        for line in lines:
            text = line[1].replace("\n", "").replace(" ", "").replace("\t", "")
            print(text)
            f.writelines("\""+line[0]+"\""+"&@@&"+"\""+text+"\"\n")

# db_to_csv(lines)

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
    words_stat = words_stat.reset_index().sort_values(by=["size"], ascending=False)
    print(words_stat[:1500])
    wordcloud = WordCloud(font_path="simhei.ttf", background_color="white", max_font_size=80)
    word_frequence = {x[0]: x[1] for x in words_stat.head(1500).values}
    wordcloud = wordcloud.fit_words(word_frequence)
    plt.imshow(wordcloud)
    plt.show()

# word_count(lines,stopwords)

def lda(lines,stopwords):
    """lda主题"""
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
    wors={}
    for topic in lda.print_topics():
        words=topic[1].split("+")
        for word in words:
            ss=[ii.replace(" ","").replace("\"","") for ii in word.split("*")]
            print(wors.get(ss[1],0),ss[0],wors.get(ss[1],0)+float(ss[0]))
            wors[ss[1]]=wors.get(ss[1],0)+float(ss[0])
            # print(ss)
    wors={x:float('%.3f'%y) for x,y in wors.items()}

    # 合并词
    data_dic = {'count': wors}
    data_df = pd.DataFrame(data_dic)
    data_df = data_df.reset_index().sort_values(by=["count"], ascending=False)
    print(data_df[:10]["index"])
    print(data_df[:10].index)
    print(data_df[:10]["count"])

    number = numpy.array(data_df[:10]["count"].values)
    work_type = data_df[:10]["index"].values


    labels = tuple(work_type)
    fracs = number

    print(labels)
    plt.pie(x=fracs, labels=labels, autopct='%.0f%%')  # autopct显示百分比
    plt.show()


lda(lines,stopwords)