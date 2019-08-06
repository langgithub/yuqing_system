# -*- coding: utf-8 -*-
import time
import json
import scrapy
import urllib.parse
from pipelines import MysqlPipline
from scrapy.log import logger
from selenium import webdriver


class ToutiaoSpider(scrapy.Spider):
    """
    烦人的cookie 直接用driver
    """

    name = 'toutiao'
    allowed_domains = ['www.toutiao.com']
    start_urls = ['https://www.toutiao.com/api/search/content/?aid=24&app_name=web_search&format=json&keyword=2018年10月7日达川区南外济民医院门口突然塌陷事件&autoload=true&count=20&en_qc=1&cur_tab=1&from=search_tab&pd=synthesis']
    mysql = MysqlPipline()


    def start_requests(self):
        driver=webdriver.Chrome(executable_path="/Users/yuanlang/work/javascript/chromedriver")
        driver.get("https://www.toutiao.com/search/?keyword=2018%E5%B9%B410%E6%9C%887%E6%97%A5%E8%BE%BE%E5%B7%9D%E5%8C%BA%E5%8D%97%E5%A4%96%E6%B5%8E%E6%B0%91%E5%8C%BB%E9%99%A2%E9%97%A8%E5%8F%A3%E7%AA%81%E7%84%B6%E5%A1%8C%E9%99%B7%E4%BA%8B%E4%BB%B6")
        time.sleep(2)
        for url in self.start_urls:
            for page in range(7,8):
                driver.get(url=f"{url}&offset={20*page}&timestamp={'%d'%(time.time()*1000)}")
                time.sleep(5)
                html=scrapy.Selector(text=driver.page_source)
                content=html.css("body > pre::text").extract_first()
                data=json.loads(content)["data"]
                for item in data:
                    try:
                        if "article_url" not in item:
                            if "display" not in item:
                                print(item)
                                continue
                            print(item["display"])
                            _url = item["display"]["info"]["url"]
                            title = item["display"]["emphasized"]["title"]
                        else:
                            title = item["abstract"]
                            _url = item["article_url"]
                        schame = urllib.parse.urlparse(_url).netloc
                        sql = f"insert into seed(url,title,site_name,type) values('{_url}','{title}','{schame}',1)"
                        self.mysql.excute_sql(sql)
                    except Exception as e:
                        logger.error(f"requests.get(data_dict['url']).url ===>>> {str(e)}")

                # time.sleep(6000)

    def parse(self, response):
        pass

