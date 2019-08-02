# -*- coding: utf-8 -*-
import pymongo
from scrapy import log
from scrapy.conf import settings
import threading
from openpyxl import Workbook
import redis
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
import scrapy
import pymysql
from twisted.enterprise import adbapi
import random
import sys
from scrapy.log import logger
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
# 单例模式创建MongoPipline

Lock = threading.Lock()


class MongoPipeline(object):
    # 定义静态变量实例
    __instance = None

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            try:
                Lock.acquire()
                # double check
                if not cls.__instance:
                    cls.client = pymongo.MongoClient(settings['MONGO_URI'])
                    cls.db = cls.client[settings['MONGO_DATABASE']]
                    cls.__instance = super(MongoPipeline, cls).__new__(cls, *args, **kwargs)
            finally:
                Lock.release()
        return cls.__instance

    def dorp_connection(self,db_name):
        return self.db[db_name].drop()

    def ensure_index(self,db_name,unique_id):
        return self.db[db_name].ensure_index(unique_id,unique=True)


    # def process_item(self, item,spider):
    #     '''
    #     异步增加，修改
    #     :param item:
    #     :param spider:
    #     :return:
    #     '''
    #     if item["operation"]=="insert":
    #
    #         try:
    #             self.db[item["db"]].insert(dict(item["info"]))
    #             log.msg("[{0} line:{1}] insert {2}=====>>>>>种子入库".
    #                     format(self.__class__.__name__, sys._getframe().f_lineno, item["db"]), level=log.INFO)
    #         except Exception as e:
    #             log.msg("[{0} line:{1}] {2}".
    #                     format(self.__class__.__name__, sys._getframe().f_lineno, e),level=log.ERROR)
    #
    #     elif item["operation"]=="upsert":
    #         self.db[item["db"]].update(item["condition"], item["info"], True)
    #         log.msg("[{0} line:{1}] upsert {2}=====>>>>更新种子信息"
    #                 .format(self.__class__.__name__, sys._getframe().f_lineno, item["db"]),level=log.INFO)
    #     elif item["operation"]=="update":
    #         self.db[item["db"]].update(item["condition"], item["info"], False)
    #         log.msg("[{0} line:{1}] update {2}=====>>>>更新种子信息"
    #                 .format(self.__class__.__name__, sys._getframe().f_lineno, item["db"]),level=log.INFO)

    def process_item(self, item,db_name):
        try:
            self.db[db_name].insert(dict(item))
            log.msg("[{0} line:{1}] insert {2}=====>>>>>种子入库".
                    format(self.__class__.__name__, sys._getframe().f_lineno,db_name), level=log.INFO)
        except Exception as e:
            log.msg("[{0} line:{1}] {2}".
                    format(self.__class__.__name__, sys._getframe().f_lineno, e), level=log.ERROR)

    def process_items(self, items, db_name):
        try:
            self.db[db_name].insert(items)
            log.msg("[{0} line:{1}] insert {2}=====>>>>>种子入库".
                    format(self.__class__.__name__, sys._getframe().f_lineno,db_name), level=log.INFO)
        except Exception as e:
            log.msg("[{0} line:{1}] {2}".
                    format(self.__class__.__name__, sys._getframe().f_lineno, e), level=log.ERROR)

    def seed_find(self,db_name,conditions,return_range):
        log.msg("[{0} line:{1}] find {2}=====>>>>>小区列表页种子查询"
                .format(self.__class__.__name__,sys._getframe().f_lineno,db_name),
                level=log.INFO)
        return self.db[db_name].find(conditions,return_range)

    def info_update(self,db_name,conditions,info):
        log.msg("[{0} line:{1}] update {2}=====>>>>更新种子信息"
                .format(self.__class__.__name__,sys._getframe().f_lineno,db_name),
                level=log.INFO)
        return self.db[db_name].update(conditions,info,False)

    def info_upsert(self,db_name,conditions,info):
        log.msg("[{0} line:{1}] update {2}=====>>>>更新种子信息"
                .format(self.__class__.__name__,sys._getframe().f_lineno,db_name),
                level=log.INFO)
        return self.db[db_name].update(conditions,info,True)

    def info_update_many(self,db_name,conditions,info):
        log.msg("[{0} line:{1}] update {2}=====>>>>更新种子信息"
                .format(self.__class__.__name__,sys._getframe().f_lineno,db_name),
                level=log.INFO)
        return self.db[db_name].update_many(conditions,info,False)


    # ######################################链家房产################################
    # ###小区
    # def lianjia_xiaoqu_insert_seed(self, seed):
    #     '''
    #     小区列表页种子入库
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] insert LianJiaXiaoQuSeed=====>>>>>链家小区列表页种子入库".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["LianJiaXiaoQuSeed"].insert(seed)
    #
    # def lianjia_xiaoqu_find_seed1(self):
    #     '''
    #     链家小区列表页种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询链家小区列表页种子")
    #     return self.db["LianJiaXiaoQuSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def lianjia_xiaoqu_find_seed2(self):
    #     '''
    #     链家小区详细信息种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询链家详细信息页种子")
    #     return self.db["LianJiaXiaoQuInfo"].find({"status": 0}, {"xiaoqu_url": 1, "_id": 0})
    #
    # def lianjia_xiaoqu_update_seed(self, seed):
    #     '''
    #     更新小区列表页种子状态
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update LianJiaXiaoQuSeed=====>>>>>更新链家列表页种子".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["LianJiaXiaoQuSeed"].update_one({"url":seed["url"]},
    #                                                    {"$set":{"status":seed["status"],
    #                                                             "ts":seed["ts"]}},True)
    # def lianjia_xiaoqu_img_find(self):
    #     return self.db["LianJiaXiaoQuImg"].find({"status": 0}, {"xiaoquImgs": 1, "_id": 0,"xiaoquId":1})
    #
    # def lianjia_xiaoqu_img_update(self,item):
    #     return self.db["LianJiaXiaoQuImg"].update_many({"xiaoquId":item["xiaoquId"]},
    #                                                    {"$set":{"status":item["status"],
    #                                                             "ts":item["ts"]}})
    #
    # def lianjia_xiaoqu_update_info(self, info):
    #     '''
    #     更新链家详细页种子状态和详细信息
    #     :param info:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update LianJiaXiaoQuInfo=====>>>>>更新家详细页种子状态和详细信息".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["LianJiaXiaoQuInfo"].update({"xiaoqu_url" : info["xiaoqu_url"]},
    #                                                {"$set":{"address": info["address"],
    #                                                         "build_year": info['build_year'],
    #                                                         "build_type": info['build_type'],
    #                                                         "property_cost" : info["property_cost"],
    #                                                         "property_company": info["property_company"],
    #                                                         "developer": info["developer"],
    #                                                         "lou_dong_count" : info["lou_dong_count"],
    #                                                         "house_count": info["house_count"],
    #                                                         "nerber_shop": info["nerber_shop"],
    #                                                         "longitude": info["longitude"],
    #                                                         "latitude": info["latitude"],
    #                                                         "chengjiao_url":info["chengjiao_url"],
    #                                                         "imgs":info["imgs"],
    #                                                         "nerber_xiaoqu":info["nerber_xiaoqu"],
    #                                                         "xiaoqu_name_other":info["xiaoqu_name_other"],
    #                                                         "status": info["status"],
    #                                                         "html":info["html"],
    #                                                         "follow":info["follow"],
    #                                                         "sale_url": info["sale_url"],
    #                                                         "rent_url": info["rent_url"],
    #                                                         "ts":info["ts"]
    #                                                  }},True)
    #
    # ###成交部分
    # def lianjia_chengjiao_insert_seed(self, seed):
    #     '''
    #     链家成交种子保存
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] insert LianJiaChengJiaoFangSeed=====>>>>>插入链家成交列表页种子".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["LianJiaChengJiaoFangSeed"].insert_many(seed)
    #
    # ###二手房部分
    # def lianjia_ershoufang_insert_seed(self, seed):
    #     '''
    #     链家二手房种子保存
    #     :param seed:
    #     :return:
    #     '''
    #     return self.db["LianJiaErShouFangSeed"].insert_many(seed)
    #
    # def lianjia_ershoufang_find_seed1(self):
    #     '''
    #     链家二手房列表页种子提取
    #     :return:
    #     '''
    #     return self.db["LianJiaErShouFangSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def lianjia_ershoufang_find_seed2(self):
    #     '''
    #     链家二手房详细页种子提取
    #     :return:
    #     '''
    #     return self.db["LianJiaErShouFangInfo"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def lianjia_ershoufang_update_seed(self,seed):
    #     '''
    #     链家二手房列表页种子状态更新
    #     :param seed:
    #     :return:
    #     '''
    #     print("update操作======》更新链家二手房列表页状态： "+str(seed))
    #     return self.db["LianJiaErShouFangSeed"].update_one({"url":seed["url"]},
    #                                                    {"$set":{"status":seed["status"],
    #                                                             "ts":seed["ts"]}})
    # def lianjia_ershoufang_update_info_seed(self,seed):
    #     '''
    #     链家二手房详细页状态更新
    #     :param seed:
    #     :return:
    #     '''
    #     print("update操作======》更新链家二手房详细页状态： "+str(seed))
    #     return self.db["LianJiaErShouFangInfo"].update_one({"url":seed["url"]},
    #                                                    {"$set":{"status":seed["status"],
    #                                                             "ts":seed["ts"]}})
    # def lianjia_ershoufang_update_info(self, info):
    #     '''
    #     链家二手房详细信息更新
    #     :param info:
    #     :return:
    #     '''
    #     print("update操作======》更新链家二手房详细信息： " + info['url'])
    #     self.db["LianJiaErShouFangInfo"].update({"url": info['url']},
    #                                             {"$set":{"buyPoint": info["buyPoint"],
    #                                                "layout": info['layout'],
    #                                                "floor" : info["floor"],
    #                                                "buildArea": info["buildArea"],
    #                                                "layoutStructure": info["layoutStructure"],
    #                                                "area": info["area"],
    #                                                "buildType": info["buildType"],
    #                                                "chaoXiang": info["chaoXiang"],
    #                                                "buildStructure": info["buildStructure"],
    #                                                "decoration": info["decoration"],
    #                                                "ladderProportion": info["ladderProportion"],
    #                                                "heatingMode": info["heatingMode"],
    #                                                "propertyRightYear": info["propertyRightYear"],
    #                                                "publishDate": info["publishDate"],
    #                                                "transAttributes": info["transAttributes"],
    #                                                "lastTransaction": info["lastTransaction"],
    #                                                "houseUse": info["houseUse"],
    #                                                "houseYear": info["houseYear"],
    #                                                "propertybelong": info["propertybelong"],
    #                                                "emortgage": info["emortgage"],
    #                                                "backUp": info["backUp"],
    #                                                "houseTag": info["houseTag"],
    #                                                "traffic": info["traffic"],
    #                                                "decoration_desc": info["decoration_desc"],
    #                                                "layout_instru": info["layout_instru"],
    #                                                "longitude": info["longitude"],
    #                                                "latitude": info["latitude"],
    #                                                "ts":info["ts"]
    #                                                }} ,True)
    #
    # def lianjia_chengjiaofang_update_seed(self,seed):
    #     '''
    #     链家成交房列表页种子状态更新
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update LianJiaChengJiaoFangSeed=====>>>>>更新链家成交房列表页状态".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["LianJiaChengJiaoFangSeed"].update_one({"url":seed["url"]},
    #                                                    {"$set":{"status":seed["status"],
    #                                                         "ts":seed["ts"]}})
    #
    # def lianjia_chengjiaofang_find_seed(self):
    #     '''
    #     链家小区交易种子提取
    #     :return:
    #     '''
    #     return self.db["LianJiaXiaoQuInfo"].find({"status": 1,"chengjiao_url":{"$ne":""}}, {"chengjiao_url": 1, "_id": 0})
    #
    # def lianjia_chengjiaofang_find_seed1(self):
    #     '''
    #     链家成交房列表页种子提取
    #     :return:
    #     '''
    #     return self.db["LianJiaChengJiaoFangSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def lianjia_chengjiaofang_find_seed2(self):
    #     '''
    #     链家成交房详细页种子提取
    #     :return:
    #     '''
    #     return self.db["LianJiaChengJiaoFangInfo"].find({"status": 0}, {"chengjiao_url": 1, "_id": 0})
    #
    # def lianjia_chengjiaofang_update_info(self, info):
    #     '''
    #     链家成交房详细信息更新
    #     :param info:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update LianJiaChengJiaoFangInfo=====>>>>>更新链家成交房详细信息".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     self.db["LianJiaChengJiaoFangInfo"].update({"chengjiao_url": info['chengjiao_url']},
    #                                             {"$set":{"xiaoqu_id": info["xiaoqu_id"],
    #                                                      "chengjiao_url": info["chengjiao_url"],
    #                                                      "trade_date": info["trade_date"],
    #                                                      "trade_channel": info["trade_channel"],
    #                                                      "total_price": info['total_price'],
    #                                                      "unit_price" : info["unit_price"],
    #                                                      "list_price": info["list_price"],
    #                                                      "transaction_cycle": info['transaction_cycle'],
    #                                                      "modify_price": info["modify_price"],
    #                                                      "watch": info["watch"],
    #                                                      "follow": info["follow"],
    #                                                      "layout": info["layout"],
    #                                                      "floor": info["floor"],
    #                                                      "build_area": info["build_area"],
    #                                                      "layout_structure": info["layout_structure"],
    #                                                      "area": info["area"],
    #                                                      "build_type": info["build_type"],
    #                                                      "orientation": info["orientation"],
    #                                                      "house_year": info["house_year"],
    #                                                      "build_year": info["build_year"],
    #                                                      "decoration": info["decoration"],
    #                                                      "build_structure": info["build_structure"],
    #                                                      "ladder_ratio": info["ladder_ratio"],
    #                                                      "heating_mode": info["heating_mode"],
    #                                                      "right_year": info["right_year"],
    #                                                      "has_elevator": info["has_elevator"],
    #                                                      "publish_date": info["publish_date"],
    #                                                      "transaction_attr": info["transaction_attr"],
    #                                                      "last_tranfic": info["last_tranfic"],
    #                                                      "house_use": info["house_use"],
    #                                                      "house_year": info["house_year"],
    #                                                      "right_belong": info["right_belong"],
    #                                                      "layout_instru": info["layout_instru"],
    #                                                      "emortgage": info["emortgage"],
    #                                                      "back_up": info["back_up"],
    #                                                      "record": info["record"],
    #                                                      "house_tag": info["house_tag"],
    #                                                      "xiaoqu_instru":info["xiaoqu_instru"],
    #                                                      "sax_analysis": info["sax_analysis"],
    #                                                      "traffic": info["traffic"],
    #                                                      "decoration_desc": info["decoration_desc"],
    #                                                      "layout_instru": info["layout_instru"],
    #                                                      "buy_point": info["buy_point"],
    #                                                      "imgs": info["imgs"],
    #                                                      "ts":info["ts"],
    #                                                      "status":info["status"],
    #                                                      "html":info["html"]
    #                                                }} ,True)
    #
    # def lianjia_xiaoqu_update_chengjiao_seed(self, info):
    #     '''
    #     链家成交房详细信息更新
    #     :param info:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update LianJiaXiaoQuInfo=====>>>>>更新链家小区成交房种子状态 status=2".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     self.db["LianJiaXiaoQuInfo"].update({"chengjiao_url": info['chengjiao_url']},
    #                                             {"$set":{"status": info["status"],
    #                                                      "ts":info["ts"]
    #                                                }} ,True)
    #
    #
    # #房价
    def lianjia_fangjia_insert_seed(self, seed):
        '''
        房价种子入库
        :param seed:
        :return:
        '''
        print("insert操作======》链家房价种子入库")
        return self.db["LianJiaFangJiaSeed"].insert(seed)
    #
    def lianjia_fangjia_update_seed(self,seed):
        '''
        链家成交房列表页种子状态更新
        :param seed:
        :return:
        '''
        print("update操作======》更新链家房价种子状态： "+str(seed))
        return self.db["LianJiaFangJiaSeed"].update_one({"url":seed["url"]},
                                                       {"$set":{"status":seed["status"],
                                                                "ts":seed["ts"]}})
    #
    #
    def lianjia_fangjia_find_seed(self):
        '''
        :return:
        '''
        print("finid操作======》查询链家房价种子")
        return self.db["LianJiaFangJiaSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # ####租房部分
    # def lianjia_zufang_insert_seed(self, seed):
    #     return self.db["LianJiaZuFangSeed"].insert_many(seed)
    #
    # ##未实现循环读取Redis
    # def lianjia_zufang_find_seed(self):
    #     return self.db["LianJiaZuFangSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def lianjia_zufang_update_seed(self,seed):
    #     print("update操作======》更新链家租房url状态： "+str(seed))
    #     return self.db["LianJiaZuFangSeed"].update_one({"url":seed["url"]},
    #                                                    {"$set":{"status":seed["status"],
    #                                                             "ts":seed["ts"]}})
    #
    # #################################我爱我家##################################################
    # ###我爱我家小区
    #
    # def f5j5j_xiaoqu_insert_seed(self, seed):
    #     '''
    #     我爱我家小区列表页种子保存
    #     :param seed:
    #     :return:
    #     '''
    #     return self.db["F5J5JXiaoQuSeed"].insert_many(seed)
    #
    # def f5j5j_xiaoqu_find_seed1(self):
    #     '''
    #     我爱我家小区列表页种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询我爱我家小区列表页种子")
    #     return self.db["F5J5JXiaoQuSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def f5j5j_xiaoqu_find_seed2(self):
    #     '''
    #     我爱我家小区详细信息种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询我爱我家详细信息页种子")
    #     return self.db["F5J5JXiaoQuInfo"].find({"status": 0}, {"xiaoqu_url": 1, "_id": 0})
    #
    # def f5j5j_xiaoqu_update_seed(self, seed):
    #     '''
    #     更新小区列表页种子状态
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update F5J5JXiaoQuSeed=====>>>>>更新我爱我家列表页种子".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["F5J5JXiaoQuSeed"].update_one({"url":seed["url"]},
    #                                                    {"$set":{"status":seed["status"],
    #                                                             "ts":seed["ts"]}})
    #
    # def f5j5j_xiaoqu_update_info(self, info):
    #     '''
    #     更新我爱我家详细页种子状态和详细信息
    #     :param info:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update F5J5JXiaoQuInfo=====>>>>>更新我爱我家详细页种子状态和详细信息".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["F5J5JXiaoQuInfo"].update({"xiaoqu_url" : info["xiaoqu_url"]},
    #                                                {"$set":{"address": info["address"],
    #                                                         "build_year": info['build_year'],
    #                                                         "build_type": info['build_type'],
    #                                                         "lou_dong_count": info["lou_dong_count"],
    #                                                         "house_count": info["house_count"],
    #                                                         "region_three" : info["region_three"],
    #                                                         "region_four": info["region_four"],
    #                                                         "follow": info["follow"],
    #                                                         "chengjiao_url":info["chengjiao_url"],
    #                                                         "property_company": info["property_company"],
    #                                                         "developer": info["developer"],
    #                                                         "greening_rate": info["greening_rate"],
    #                                                         "traffic": info["traffic"],
    #                                                         "nerber_shop": info["nerber_shop"],
    #                                                         "trend": info['trend'],
    #                                                         "imgs": info["imgs"],
    #                                                         "longitude": info["longitude"],
    #                                                         "latitude": info["latitude"],
    #                                                         "status": info["status"],
    #                                                         "ts":info["ts"]
    #                                                  }} ,True)
    #
    # def f5j5j_xiaoqu_img_find(self):
    #     return self.db["F5J5JXiaoQuImg"].find({"status": 0}, {"image_urls": 1, "_id": 0,"id":1})
    #
    # def f5j5j_xiaoqu_img_update(self,item):
    #     return self.db["F5J5JXiaoQuImg"].update_many({"id":item["id"]},
    #                                                    {"$set":{"status":item["status"],
    #                                                             "ts":item["ts"]}})
    # #成交
    # def f5j5j_chengjiao_find_seed(self):
    #     '''
    #     成交种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询我爱我家成交房详细信息页种子")
    #     return self.db["F5J5JXiaoQuInfo"].find({"status": 1}, {"chengjiao_url": 1, "_id": 0})
    #
    # def f5j5j_chengjiao_update_info(self,seed):
    #     '''
    #     更新种子
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update F5J5JXiaoQuInfo=====>>>>>更新我爱我家小区成交房种子状态[chengjiao_url:{2}]".format(self.__class__.__name__, sys._getframe().f_lineno,seed["chengjiao_url"]), level=log.INFO)
    #     return self.db["F5J5JXiaoQuInfo"].update_one({"chengjiao_url": seed["chengjiao_url"]},
    #                                                  {"$set":{
    #                                                      "status":seed["status"],
    #                                                      "ts":seed["ts"]
    #                                                  }},True)
    #
    # #二手房
    # def f5j5j_ershoufang_insert_seed(self, seed):
    #     return self.db["F5J5JErShouFangSeed"].insert_many(seed)
    #
    # def f5j5j_ershoufang_find_seed1(self):
    #     '''
    #     我爱我家小区列表页种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询我爱我家小区列表页种子")
    #     return self.db["F5J5JErShouFangSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def f5j5j_ershoufang_find_seed2(self):
    #     '''
    #     我爱我家小区详细信息种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询我爱我家详细信息页种子")
    #     return self.db["F5J5JErShouFangInfo"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def f5j5j_zufang_insert_seed(self, seed):
    #     return self.db["F5J5JZuFangSeed"].insert_many(seed)
    #
    # def f5j5j_ershoufang_find_seed(self):
    #     return self.db["F5J5JErShouFangSeed"].find({"status": 0}, {"url": 1, "_id": 0}).limit(50)
    #
    # def f5j5j_ershoufang_update_seed(self,seed):
    #     print("update操作======》更新我爱我家二手房url状态： "+str(seed))
    #     return self.db["F5J5JErShouFangSeed"].update_one({"url":seed["url"]},
    #                                                    {"$set":{"status":seed["status"],
    #                                                             "ts":seed["ts"]}})
    #
    # def f5j5j_ershoufang_update_info(self, info):
    #     print("update操作======》更新我爱我家二手房详细信息： " + info['url'])
    #     self.db["F5J5JErShouFangInfo"].update({"url": info['url']},
    #                                             {"$set": {"buyPoint": info["buyPoint"],
    #                                                       "layout": info['layout'],
    #                                                       "floor": info["floor"],
    #                                                       "area": info["area"],
    #                                                       "publishDate": info["publishDate"],
    #                                                       "buildYear": info["buildYear"],
    #                                                       "layout_instru": info["layout_instru"],
    #                                                       "traffic" :info["traffic"],
    #                                                       "taxAnalysis": info["taxAnalysis"],
    #                                                       "loanSituation": info["loanSituation"],
    #                                                       "arroundMatch": info["arroundMatch"],
    #                                                       "propertyMortgage": info["propertyMortgage"],
    #                                                       "xiaoquInfo": info["xiaoquInfo"],
    #                                                       "arroundMatch": info["arroundMatch"],
    #                                                       "status":info["status"],
    #                                                       "ts": info["ts"]
    #                                                       }}, True)
    #
    # def f5j5j_zufang_find_seed(self):
    #     return self.db["F5J5JZuFangSeed"].find({"status": 0}, {"url": 1, "_id": 0}).limit(50)
    #
    # ###########################tc58#################################
    # ###五八同城小区
    # def tc58_xiaoqu_insert_seed(self, seed):
    #     '''
    #     小区列表页种子入库
    #     :param seed:
    #     :return:
    #     '''
    #     print("insert操作======》tc58小区列表页种子入库")
    #     return self.db["TC58XiaoQuSeed"].insert(seed)
    #
    # def tc58_xiaoqu_update_seed(self, info):
    #     log.msg("update操作======》tc58小区列表页种子更新{0}".format(info['url']),level=log.INFO)
    #     self.db["TC58XiaoQuSeed"].update({"url": info['url']},
    #                                      {"$set": {"status": info['status'],
    #                                                "ts": info["ts"]
    #                                                }})
    #
    # def tc58_xiaoqu_update_info(self, info):
    #     log.msg("update操作======》tc58小区详细页种子更新{0}".format(info['xiaoqu_url']),level=log.INFO)
    #     self.db["TC58XiaoQuInfo"].update({"xiaoqu_url": info['xiaoqu_url']},
    #                                      {"$set": {"xiaoqu_name_two": info['xiaoqu_name_two'],
    #                                                "huan_bi": info["huan_bi"],
    #                                                "address":info["address"],
    #                                                "build_type":info["build_type"],
    #                                                "house_count": info["house_count"],
    #                                                "property_type": info["property_type"],
    #                                                "property_cost": info["property_cost"],
    #                                                "far": info["far"],
    #                                                "build_year": info["build_year"],
    #                                                "greening_rate": info["greening_rate"],
    #                                                "building_foot_print": info["building_foot_print"],
    #                                                "building_area": info["building_area"],
    #                                                "property_company": info["property_company"],
    #                                                "developer": info["developer"],
    #                                                "xiaoqu_id": info["xiaoqu_id"],
    #                                                "trend": info["trend"],
    #                                                "latitude":info["latitude"],
    #                                                "longitude":info["longitude"],
    #                                                "ts": info["ts"],
    #                                                "status": info["status"]
    #                                                }},True)
    #
    # def tc58_xiaoqu_find_seed1(self):
    #     '''
    #     tc58小区列表页种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询链家小区列表页种子")
    #     return self.db["TC58XiaoQuSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def tc58_xiaoqu_find_seed2(self):
    #     '''
    #     tc58小区详细信息种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询链家详细信息页种子")
    #     return self.db["TC58XiaoQuInfo"].find({"status": 0}, {"xiaoqu_url": 1, "_id": 0})
    #
    #
    # def tc58_personfang_update_seed(self, info):
    #     log.msg("update操作======》个人房源列表页种子更新{0}".format(info['url']),level=log.INFO)
    #     self.db["TC58PersonFangSeed"].update({"url": info['url']},
    #                                      {"$set": {"status": info['status'],
    #                                                "ts": info["ts"]
    #                                                }},True)
    #
    # def tc58_personfang_update_info(self, info):
    #     log.msg("update操作======》tc58个人房源种子更新{0}".format(info['ershoufang_url']),level=log.INFO)
    #     self.db["TC58PersonFangInfo"].update({"ershoufang_url": info['ershoufang_url']},
    #                                      {"$set": {"publish_date": info['publish_date'],
    #                                                "total_price": info["total_price"],
    #                                                "unit_price": info['unit_price'],
    #                                                "xiaoqu_name_two": info["xiaoqu_name_two"],
    #                                                "floor": info["floor"],
    #                                                "layout": info["layout"],
    #                                                "decoration": info["decoration"],
    #                                                "area": info["area"],
    #                                                "right_year": info["right_year"],
    #                                                "orientation": info["orientation"],
    #                                                "build_year": info["build_year"],
    #                                                "buy_point": info["buy_point"],
    #                                                "house_use": info["house_use"],
    #                                                "transaction_attr": info["transaction_attr"],
    #                                                "status": info["status"],
    #                                                "ts": info["ts"]
    #                                                }},True)
    #
    # def tc58_personfang_find_seed(self):
    #     '''
    #     tc58小区详细信息种子提取
    #     :return:
    #     '''
    #     print("find操作======》查询tc58详细信息页种子")
    #     return self.db["TC58PersonFangInfo"].find({"status": 0}, {"ershoufang_url": 1, "_id": 0})
    #
    # def tc58_xiaoqu_list_insert_seed(self, seed):
    #     '''
    #     小区列表页种子入库
    #     :param seed:
    #     :return:
    #     '''
    #     print("insert操作======》小区信息")
    #     return self.db["TC58XiaoQu_list"].insert(seed)
    #
    #
    # #######################二手房
    #
    # def tc58_ershoufang_find_seed_from_xiaoqu(self):
    #     return self.db["TC58XiaoQu"].find({"status": 0}, {"erShouFangUrl": 1, "_id": 0}).limit(50)
    #
    # def tc58_ershoufang_insert_seed(self, seed):
    #     return self.db["TC58ErShouFangSeed"].insert_many(seed)
    #
    # def tc58_ershoufang_insert_info(self, info):
    #     return self.db["TC58ErShouFangInfo"].insert_one({"url":info["url"],
    #                                              "title" : info["title"],
    #                                              "address": info["address"],
    #                                              "totalPrice": info["totalPrice"],
    #                                              "unitPrice": info["unitPrice"],
    #                                              "area":info["area"],
    #                                              "status":0})
    #
    #
    # def tc58_ershoufang_find_seed(self):
    #     return self.db["TC58ErShouFangSeed"].find({"status": 0}, {"url": 1, "_id": 0}).limit(50)
    #
    # ####################租房
    #
    # def tc58_zufang_find_seed_from_xiaoqu(self):
    #     return self.db["TC58XiaoQu"].find({"status": 0}, {"zuFangUrl": 1, "_id": 0}).limit(50)
    #
    # def tc58_zufang_find_seed(self):
    #     return self.db["TC58ZuFangSeed"].find({"status": 0}, {"url": 1, "_id": 0}).limit(50)
    #
    # def tc58_zufang_insert_seed(self, seed):
    #     return self.db["TC58ZuFangSeed"].insert_many(seed)
    #
    # def tc58_zufang_insert_info(self, info):
    #     return self.db["TC58ZuFangInfo"].insert_one({"url":info["url"],
    #                                              "title" : info["title"],
    #                                              "address": info["address"],
    #                                              "unitPrice": info["unitPrice"],
    #                                              "status":0})
    # ### 麦田小区信息 ###
    #
    # def maitian_xiaoqu_insert_seed(self, seed):
    #     '''
    #     生成小区名称及相应的url
    #     '''
    #     print("insert操作======》麦田小区url")
    #     return self.db["maitianXiaoQuSeed"].insert(seed)
    #
    # def maitian_xiaqu_url(self):
    #     return self.db["maitianXiaoQuSeed"].find({}).limit(1400)
    #
    # def maitian_xiaoqu_insert(self, data):
    #     return self.db["maitian_xiaoqu_info"].insert(data)
    #
    #
    #
    # ################################房天下##########################################################
    # #####房天下小区
    # def fang_xiaoqu_insert_seed(self, seed):
    #     '''
    #     小区列表页种子入库
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] insert FangXiaoQuSeed=====>>>>>插入Fang天下小区url".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["FangXiaoQuSeed"].insert(seed)
    #
    # def fang_xiaoqu_find_seed1(self):
    #     return self.db["FangXiaoQuSeed"].find({"status": 0}).limit(50)
    #
    # def fang_xiaoqu_find_seed2(self):
    #     return self.db["FangXiaoQuInfo"].find({"status": 0},{"xiaoqu_url": 1, "_id": 0}).limit(50)
    #
    # def fang_xiaoqu_update_seed(self, info):
    #     log.msg("[{0} line:{1}] update FangXiaoQuSeed=====>>>>>Fang天下小区列表页种子更新".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     self.db["FangXiaoQuSeed"].update({"url": info['url']},
    #                                      {"$set": {"status": info['status'],
    #                                                "ts": info["ts"]
    #                                                }},True)
    #
    # def fang_xiaoqu_update_info(self, info):
    #     log.msg("[{0} line:{1}] update FangXiaoQuInfo=====>>>>>Fang天下小区详细页种子更新".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     self.db["FangXiaoQuInfo"].update({"xiaoqu_url": info['xiaoqu_url']},
    #                                      {"$set": {"xiaoqu_name_two": info['xiaoqu_name_two'],
    #                                                "sale_url": info["sale_url"],
    #                                                "chengjiao_url":info["chengjiao_url"],
    #                                                "rent_url":info["rent_url"],
    #                                                "sale_total": info["sale_total"],
    #                                                "chengjiao_total": info["chengjiao_total"],
    #                                                "rent_total": info["rent_total"],
    #                                                "xiaoqu_id": info["xiaoqu_id"],
    #                                                "region_two": info["region_two"],
    #                                                "region_three": info["region_three"],
    #                                                "longitude": info["longitude"],
    #                                                "latitude": info["latitude"],
    #                                                "trend": info["trend"],
    #                                                "unit_price": info["unit_price"],
    #                                                "huan_bi": info["huan_bi"],
    #                                                "tong_bi": info["tong_bi"],
    #                                                "address": info["address"],
    #                                                "property_type": info["property_type"],
    #                                                "build_year": info["build_year"],
    #                                                "developer": info["developer"],
    #                                                "build_type": info["build_type"],
    #                                                "building_area": info["building_area"],
    #                                                "building_foot_print": info["building_foot_print"],
    #                                                "property_company": info["property_company"],
    #                                                "greening_rate": info["greening_rate"],
    #                                                "far": info["far"],
    #                                                "property_cost":info["property_cost"],
    #                                                "follow":info["follow"],
    #                                                "imgs":info["imgs"],
    #                                                "layout_imgs": info["layout_imgs"],
    #                                                "status": info["status"],
    #                                                "ts": info["ts"]
    #                                                }},True)
    # ####房天下二手房
    # def fang_ershoufang_insert_seed(self, seed):
    #     '''
    #     房天下二手房列表页种子入库
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] insert FangErShouFangSeed=====>>>>>房天下二手房url".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["FangErShouFangSeed"].insert(seed)
    #
    # def fang_ershoufang_find_seed(self):
    #     return self.db["FangXiaoQuInfo"].find({"$and":[{"sale_url":{"$ne":None}},{"sale_url":""},{"status": 1}]},{"xiaoqu_url": 1, "_id": 0})
    #
    # def fang_ershoufang_info(self):
    #     return self.db["FangErShouFangInfo"].find({"status": 1,"zf_jjname": "业主"},{"formUrl": 1, "_id": 0})
    #
    # def fang_ershoufang_info_update(self, info):
    #     log.msg("update操作======》Fang天下二手房小区名称", level=log.INFO)
    #     self.db["FangErShouFangInfo"].update_many({"formUrl": info['formUrl']},
    #                                          {"$set": {"xiaoquName": info['xiaoquName'],
    #                                                    "status":info["status"]
    #                                                    }}, True)
    #
    # def fang_ershoufang_find_seed1(self):
    #     return self.db["FangErShouFangSeed"].find({"status": 0})
    #
    # def fang_ershoufang_find_seed2(self):
    #     return self.db["FangErShouFangInfo"].find({"status": 0},{"ershoufang_url": 1, "_id": 0})
    #
    # def fang_ershoufang_find_seed3(self):
    #     return self.db["FangXiaoQuInfo"].find({"$and":[{"sale_url":{"$ne":None}},{"sale_url":{"$ne":""}},{"status": 1}]},{"sale_url": 1, "_id": 0})
    #
    # def fang_ershoufang_update_seed(self, info):
    #     log.msg("update操作======》Fang天下二手房列表页种子更新{0}".format(info['url']),level=log.INFO)
    #     self.db["FangErShouFangSeed"].update({"url": info['url']},
    #                                      {"$set": {"status": info['status'],
    #                                                "ts": info["ts"]
    #                                                }},True,True)
    # def fang_xiaoqu_ershoufang_update_seed(self, info):
    #     log.msg("[{0} line:{1}] update FangXiaoQuInfo=====>>>>>Fang天下小区列表页种子更新".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     self.db["FangXiaoQuInfo"].update({"sale_url": info['sale_url']},
    #                                      {"$set": {"status": info['status'],
    #                                                "ts": info["ts"]
    #                                                }},True)
    #
    # def fang_xiaoqu_ershoufang_update_seed2(self, info):
    #     log.msg("[{0} line:{1}] update FangErShouFangSeed=====>>>>>Fang天下小区列表页种子更新".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     self.db["FangXiaoQuInfo"].update({"xiaoqu_url": info['xiaoqu_url']},
    #                                      {"$set": {"status": info['status'],
    #                                                "ts": info["ts"]
    #                                                }},True)
    #
    # def fang_ershoufang_update_info(self, info):
    #     log.msg("[{0} line:{1}] update FangErShouFangInfo=====>>>>>Fang天下二手房详细页种子更新".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     self.db["FangErShouFangInfo"].update({"ershoufang_url": info['ershoufang_url']},
    #                                      {"$set": {"total_price": info['total_price'],
    #                                                "decoration": info["decoration"],
    #                                                "floor":info["floor"],
    #                                                "layout":info["layout"],
    #                                                "build_area": info["build_area"],
    #                                                "unit_price": info["unit_price"],
    #                                                "orientation": info["orientation"],
    #                                                "region_three": info["region_three"],
    #                                                "build_year": info["build_year"],
    #                                                "has_elevator": info["has_elevator"],
    #                                                "house_use": info["house_use"],
    #                                                "build_structure": info["build_structure"],
    #                                                "build_type": info["build_type"],
    #                                                "publish_date": info["publish_date"],
    #                                                "buy_point": info["buy_point"],
    #                                                "fzzj": info["fzzj"],
    #                                                "xiaoqu_instru": info["xiaoqu_instru"],
    #                                                "ye_zhu": info["ye_zhu"],
    #                                                "status":info["status"],
    #                                                "ts": info["ts"]
    #                                                }},True)
    #
    #
    # ######################################中原房产################################
    # ###小区
    # def zhongyuan_xiaoqu_insert_seed(self, seed):
    #     '''
    #     小区列表页种子入库
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] insert ZhongYuanXiaoQuSeed =====>>>>> 中原房产小区列表页种子入库".format(self.__class__.__name__,sys._getframe().f_lineno),level=log.INFO)
    #     return self.db["ZhongYuanXiaoQuSeed"].insert(seed)
    #
    # def zhongyuan_xiaoqu_update_seed(self, seed):
    #     '''
    #     更新小区列表页种子状态
    #     :param seed:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update ZhongYuanXiaoQuSeed =====>>>>> 更新中原房产列表页种子".format(self.__class__.__name__,sys._getframe().f_lineno),level=log.INFO)
    #     return self.db["ZhongYuanXiaoQuSeed"].update_one({"url":seed["url"]},
    #                                                    {"$set":{"status":seed["status"],
    #                                                             "ts":seed["ts"]}})
    #
    # def zhongyuan_xiaoqu_update_info(self, info):
    #     '''
    #     更新中原详细页种子状态和详细信息
    #     :param info:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update ZhongYuanXiaoQuInfo=====>>>>>更新家中原地产详细页种子状态和详细信息".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     return self.db["ZhongYuanXiaoQuInfo"].update({"xiaoqu_url" : info["xiaoqu_url"]},
    #                                                {"$set":{"xiaoqu_name_other": info["xiaoqu_name_other"],
    #                                                         "address": info['address'],
    #                                                         "region_three": info['region_three'],
    #                                                         "region_four" : info["region_four"],
    #                                                         "property_type": info["property_type"],
    #                                                         "build_year": info["build_year"],
    #                                                         "property_cost" : info["property_cost"],
    #                                                         "property_company": info["property_company"],
    #                                                         "developer": info["developer"],
    #                                                         "far": info["far"],
    #                                                         "greening_rate": info["greening_rate"],
    #                                                         "sale_url":info["sale_url"],
    #                                                         "rent_url":info["rent_url"],
    #                                                         "chengjiao_url":info["chengjiao_url"],
    #                                                         "latitude":info["latitude"],
    #                                                         "longitude": info["longitude"],
    #                                                         "ts":info["ts"],
    #                                                         "status":info["status"],
    #                                                         "imgs": info["imgs"],
    #                                                         "trend": info["trend"],
    #                                                         "html": info["html"],
    #                                                         "xiaoqu_id":info["xiaoqu_id"]
    #                                                  }},True)
    #
    # def zhongyuan_xiaoqu_find_seed1(self):
    #     '''
    #     中原地产小区列表页种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询中原地产小区列表页种子")
    #     return self.db["ZhongYuanXiaoQuSeed"].find({"status": 0}, {"url": 1, "_id": 0})
    #
    # def zhongyuan_xiaoqu_find_seed2(self):
    #     '''
    #     中原小区详细信息种子提取
    #     :return:
    #     '''
    #     print("finid操作======》查询中原地产详细信息页种子")
    #     return self.db["ZhongYuanXiaoQuInfo"].find({"status": 0}, {"xiaoqu_url": 1, "_id": 0})
    #
    # def zhongyuan_chengjiaofang_find_seed(self):
    #     '''
    #     中原小区交易种子提取
    #     :return:
    #     '''
    #     return self.db["ZhongYuanXiaoQuInfo"].find({"status": 1,"chengjiao_url":{"$ne":""}}, {"chengjiao_url": 1, "_id": 0})
    #
    # def zhongyuan_xiaoqu_update_chengjiao_seed(self, info):
    #     '''
    #     中原小区
    #     :param info:
    #     :return:
    #     '''
    #     log.msg("[{0} line:{1}] update ZhongYuanXiaoQuInfo=====>>>>>更新链家小区成交房种子状态 status=2".format(self.__class__.__name__, sys._getframe().f_lineno), level=log.INFO)
    #     self.db["ZhongYuanXiaoQuInfo"].update({"chengjiao_url": info['chengjiao_url']},
    #                                             {"$set":{"status": info["status"],
    #                                                      "ts":info["ts"]
    #                                                }} ,True)
    #
    #
    # ###############################赶集网###############################################
    # def ganji_ershoufang_insert_seed(self, seed):
    #     return self.db["GanJiErShouFangSeed"].insert_many(seed)
    #
    # def ganji_ershoufang_find_seed(self):
    #     return self.db["GanJiErShouFangSeed"].find({"status": 0}, {"url": 1, "_id": 0}).limit(50)
    #
    # def ganji_ershoufang_insert_info(self, info):
    #     return self.db["GanJiErShouFangInfo"].insert_one({"url":info["url"],
    #                                              "title" : info["title"],
    #                                              "layout": info["layout"],
    #                                              "area": info["area"],
    #                                              "chaoXiang": info["chaoXiang"],
    #                                              "floor": info["floor"],
    #                                              "decoration": info["decoration"],
    #                                              "xiaoquName": info["xiaoquName"],
    #                                              "xiaoquUrl": info["xiaoquUrl"],
    #                                              "address": info["address"],
    #                                              "unitPrice": info["unitPrice"],
    #                                              "totalPrice": info["totalPrice"],
    #                                              "status":0})
    #
    # def ganji_zufang_insert_seed(self, seed):
    #     return self.db["GanJiZuFangSeed"].insert_many(seed)
    #
    # def ganji_zufang_find_seed(self):
    #     return self.db["GanJiZuFangSeed"].find({"status": 0}, {"url": 1, "_id": 0}).limit(50)
    #
    # def ganji_zufang_insert_info(self, info):
    #     return self.db["GanJiZuFangInfo"].insert_one({"url":info["url"],
    #                                              "title" : info["title"],
    #                                              "layout": info["layout"],
    #                                              "area": info["area"],
    #                                              "chaoXiang": info["chaoXiang"],
    #                                              "floor": info["floor"],
    #                                              "decoration": info["decoration"],
    #                                              "xiaoquName": info["xiaoquName"],
    #                                              "xiaoquUrl": info["xiaoquUrl"],
    #                                              "address": info["address"],
    #                                              "unitPrice": info["unitPrice"],
    #                                              "leasehold":info["leasehold"],
    #                                              "publishDate":info["publishDate"],
    #                                              "status":0})

##########img################################################
class ImgDownloadPipeline(ImagesPipeline):
    #yeild 调用下载
    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            yield scrapy.Request(image_url)

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        item['image_paths'] = image_paths
        return item

#########redis#############################################
class RedisPipeline(object):

    def __init__(self):
        if not hasattr(RedisPipeline, 'pool'):
            RedisPipeline.create_pool()
        self._connection = redis.Redis(connection_pool=RedisPipeline.pool)

    @staticmethod
    def create_pool():
        RedisPipeline.pool = redis.ConnectionPool(
            host="127.0.0.1",
            port=6379,
            db=0)

    def set_lianjia_seed(self, key, value):
        '''''set data with (key, value)
        '''
        return self._connection.lpush(key, value)

    def set_seed(self, key, value):
        '''''set data with (key, value)
        '''
        return self._connection.lpush(key, value)

    def list_len(self,key):
        '''
        获取长度
        :return:
        '''
        return self._connection.llen(key)

    def delete_key(self,key):
        return self._connection.delete(key)



####mysql######################################################
class MysqlPipline(object):
    # 定义静态变量实例
    __instance = None

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            try:
                Lock.acquire()
                # double check
                if not cls.__instance:
                    cls.conn = pymysql.connect(host=settings['MYSQL_HOST'],
                                           port=settings['MYSQL_PORT'],
                                           user=settings['MYSQL_USER'],
                                           passwd=settings['MYSQL_PASSWD'],
                                           db=settings['MYSQL_DB'])
                    cls.cursor = cls.conn.cursor()
                    cls.__instance = super().__new__(cls, *args, **kwargs)
            finally:
                Lock.release()
        return cls.__instance

    # 使用twisted将mysql插入变成异步执行
    def process_item(self, item, spider):
        pass

    def close(self):
        self.cursor.close()
        self.conn.close()

    def handle_error(self, failure, item, spider):
        #处理异步插入的异常
        print (failure)

    def excute_sql(self,sql):
        try:
            logger.info(f"excute_sql===>>> {sql}")
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            if "Duplicate" not in str(e):
                self.conn.rollback()


# excel
# class ExcelPipeline(object):
#     def __init__(self):
#         self.wb = Workbook()
#         self.ws = self.wb.active
#         self.ws.append(['文章url', '文章title', '文章发布时间', '文章内容', '文章作者连接', '文章作者', '文章评论数量'])  # 设置表头
#
#         self.wb2 = Workbook()
#         self.ws2 = self.wb2.active
#         self.ws2.append(['文章url', '评论人', '评论时间', '评论内容', '评论给那一条', '评论给谁'])  # 设置表头
#
#     def process_item(self, item, spider):
#         collection_name = item.__class__.__name__
#         if collection_name == "DouBanItem":
#             line = [item['article_url'], item['article_title'], item['article_publish_date'], item['article_content']
#                 , item['article_author_url'], item['article_author_name'],
#                     item['article_comment_quantity']]  # 把数据中每一项整理出来
#             self.ws.append(line)  # 将数据以行的形式添加到xlsx中
#             self.wb.save('content.xlsx')  # 保存xlsx文件
#             return item
#         if collection_name == "CommentItem":
#             line = [item['article_url'], item['comment_people'], item['comment_time'], item['comment_content']
#                 , item['comment_to_which_coment'], item['comment_to_Who']]  # 把数据中每一项整理出来
#             self.ws2.append(line)  # 将数据以行的形式添加到xlsx中
#             self.wb2.save('comment.xlsx')  # 保存xlsx文件
#             return item
