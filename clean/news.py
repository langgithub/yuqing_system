# encoding: utf-8
"""
--------------------------------------
@describe 
@version: 1.0
@project: yuqing_system
@file: news.py
@author: yuanlang 
@time: 2019-08-06 16:04
---------------------------------------
"""
import time
from newspaper import Article
import requests
import pymysql
import threading
from queue import Queue

conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", passwd="lang1994", db="yuqing_db", charset="utf8")
cursor = conn.cursor()
q=Queue()


def download(url):

    try:
        print(f"fetch url --------> {url}")
        news = Article(url, language='zh')
        reponse = requests.get(url, verify=False,timeout=10)
        if reponse.status_code==404 or reponse.status_code==503:
            sql = "update seed set status=-1 where url='" + url + "'"
            print(sql)
            cursor.execute(sql)
            conn.commit()
            return
        news.set_html(reponse.content)
        news.parse()  # 再解析
        text = news.text
        if text == "":
            sql = "update seed set status=-2 where url='" + url + "'"
            print(sql)
            cursor.execute(sql)
            conn.commit()
            return
        sql="insert into context(url,content) values('"+url+"','"+text+"')"
        print(sql)
        cursor.execute(sql)
        sql = "update seed set status=1 where url='" + url+"'"
        print(sql)
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print("exception"+str(repr(e)))


def spider():


    while True:
        cursor.execute("select url from seed where status = 0 limit 1")
        items = cursor.fetchall()
        for item in items:
            q.put(item[0])
        # result=[]
        # for i in range(20):
        #     url = q.get()
        #     t=threading.Thread(target=download,args=(url,))
        #     t.start()
        #     time.sleep(1)
        #     result.append(t)
        # for t in result:
        #     t.join()
        url = q.get()
        download(url=url)
        # time.sleep(1)

if __name__ == "__main__":
    spider()