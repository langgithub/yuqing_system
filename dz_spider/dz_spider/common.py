# encoding: utf-8
"""
--------------------------------------
@describe 
@version: 1.0
@project: yuqing_system
@file: common.py
@author: yuanlang 
@time: 2019-07-26 17:50
---------------------------------------
"""

site_name=["凤凰山下","达州市人民政府","闽南网","新京报网"]

# 种子表
seed_table="""create table if not exists `seed`(
    `url` varchar(500) Not null,
    `title` varchar(500) default "",
    `site_name` char(10) default "",
    `status` int(2) default 0,
    `create_time` timestamp default current_timestamp,
    `update_time` timestamp default current_timestamp,
    primary key (`url`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

