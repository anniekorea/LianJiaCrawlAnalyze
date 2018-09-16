# -*- coding: utf-8 -*-
"""
Created on Sat Sep  1 10:22:35 2018

@author: Annie
"""

import pandas as pd
import pymongo
import os

#筛选小区
def resblock_filter_gaode(gaode_resblock,distance1_limit,distance2_limit,duration1_limit,duration2_limit):
    gaode_resblock_filter=gaode_resblock.loc[(gaode_resblock['distance1']<distance1_limit) &
                                             (gaode_resblock['distance2']<distance2_limit) &
                                             (gaode_resblock['duration1']<duration1_limit) &
                                             (gaode_resblock['duration2']<duration2_limit)]
    resblock_filter=gaode_resblock_filter.merge(district_resblock,on='resblock_id',how='inner')
    resblock_filter=resblock_filter.merge(single_resblock,on='resblock_id',how='inner')
    resblock_filter=resblock_filter[['name_cn','avg_price_sechand','build_type',
                                           'built_year_x','house_num','build_num',
                                           'rent_num','sechand_for_sale','deal_num',
                                           'small_district_cn','big_district_cn',
                                           'manage_fee','build_comp','manage_comp',
                                           'link','resblock_id','small_district_pinyin',
                                           'resblock_location','distance1','distance2',
                                           'duration1','duration2','city_x']]
    return resblock_filter

#筛选二手房房源
def sechand_resblock_filter(district_sechand,resblock_filter):
    sechand_filter=district_sechand[district_sechand['resblock'].isin(list(resblock_filter['name_cn']))]
    sechand_filter=sechand_filter.merge(resblock_filter,left_on='resblock',right_on='name_cn',how='left')
    
    sechand_filter=sechand_filter[['house_name','resblock','total_price','area',
                                   'unit_price','avg_price_sechand','house_type',
                                   'direction','decration','elevator','floor',
                                   'built_year_x','build_type','release_time',
                                   'looked_times','deal_num','build_num','house_num',
                                   'sechand_for_sale','rent_num','manage_fee','follow',
                                   'small_district_cn','big_district_cn','manage_comp',
                                   'build_comp','link','id','resblock_id','distance1',
                                   'distance2','duration1','duration2','city_x']]
    return sechand_filter

#筛选租房房源
def rent_resblock_filter(district_rent,resblock_filter):
    rent_filter=district_rent[district_rent['resblock'].isin(list(resblock_filter['name_cn']))]
    rent_filter=rent_filter.merge(resblock_filter,left_on='resblock',right_on='name_cn',how='left')
    rent_filter=rent_filter[['rent_name','link_x','resblock','rent_price','area','unit_price',
                             'house_type','direction','floor','built_year_x','build_type',
                             'update_date','rent_num','small_district_cn_x','big_district_cn',
                             'build_num','house_num','sechand_for_sale','avg_price_sechand',
                             'manage_fee','deal_num','manage_comp','build_comp','link_y',
                             'rent_id','resblock_id','distance1','distance2','duration1',
                             'duration2','city_x']]
    return rent_filter

#设置参数
class Para:
    city='sh'
    
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    
    #高德数据
    table_gaode_resblock = database['data_gaode_resblock']   #保存租房数据的数据表
    
    #按小区域爬取得到的小区数据
    table_district_resblock= database['data_district_resblock']
    datestr_district_resblock='20180828'
    
    #按居民区爬取得到的小区数据
    table_single_resblock= database['data_single_resblock']
    datestr_single_resblock='20180830'
    
    #按小区域爬取得到的二手房数据
    table_district_sechand=database['data_district_sechand']
    datestr_district_sechand='20180826'
    
    #按小区域爬取得到的租房数据
    table_district_rent=database['data_district_rent']
    datestr_district_rent='20180827'
    
    #保存分析结果的文件夹
    save_file='../LianJiaSaveData/save_analyze_resblock'
    
    #距离和时间要求
    distance1_limit=1.5 #单位：公里
    distance2_limit=30
    duration1_limit=30  #单位：分钟
    duration2_limit=40


if __name__ == '__main__':
    para=Para()
    distance1_limit=para.distance1_limit
    distance2_limit=para.distance2_limit
    duration1_limit=para.duration1_limit
    duration2_limit=para.duration2_limit
    
    #从数据库读入数据
    gaode_resblock=pd.DataFrame(list(para.table_gaode_resblock.find({'city':para.city})))
    gaode_resblock=gaode_resblock.loc[gaode_resblock['distance1']>0,['resblock_id','distance1','distance2',
                                  'duration1','duration2','city','datestr','resblock_location']]
    
    district_resblock=pd.DataFrame(list(para.table_district_resblock.find(\
                      {'city':para.city,'datestr':para.datestr_district_resblock})))
    single_resblock=pd.DataFrame(list(para.table_single_resblock.find(\
                      {'city':para.city,'datestr':para.datestr_single_resblock})))
    district_sechand=pd.DataFrame(list(para.table_district_sechand.find(\
                      {'city':para.city,'datestr':para.datestr_district_sechand})))    
    district_rent=pd.DataFrame(list(para.table_district_rent.find(\
                      {'city':para.city,'datestr':para.datestr_district_rent})))
    
    #筛选小区
    resblock_filter=resblock_filter_gaode(gaode_resblock,distance1_limit,distance2_limit,
                                          duration1_limit,duration2_limit)
#    resblock_filter.to_csv('../LianJiaSaveData/筛选结果-小区(距离1-%s,距离2-%s,时间1-%s,时间2-%s).csv'%
#                           (distance1_limit,distance2_limit,duration1_limit,duration2_limit),
#                           encoding='utf_8_sig')
    
    #筛选二手房房源
    sechand_filter=sechand_resblock_filter(district_sechand,resblock_filter)
#    sechand_filter.to_csv('../LianJiaSaveData/筛选结果-二手房(距离1-%s,距离2-%s,时间1-%s,时间2-%s).csv'%
#                           (distance1_limit,distance2_limit,duration1_limit,duration2_limit),
#                           encoding='utf_8_sig')
    
    
    #筛选租房房源
    rent_filter=rent_resblock_filter(district_rent,resblock_filter)
#    rent_filter.to_csv('../LianJiaSaveData/筛选结果-租房(距离1-%s,距离2-%s,时间1-%s,时间2-%s).csv'%
#                           (distance1_limit,distance2_limit,duration1_limit,duration2_limit),
#                           encoding='utf_8_sig')
    
    #若不存在保存文件的文件夹，则创建
    folder = os.path.exists(para.save_file)
    if not folder:
        os.makedirs(para.save_file)
        
    #保存数据在一个excel文件中
    writer = pd.ExcelWriter('%s/筛选结果（距离1-%s,距离2-%s,时间1-%s,时间2-%s）.xlsx'
                            %(para.save_file,distance1_limit,distance2_limit,duration1_limit,duration2_limit))
    resblock_filter.to_excel(writer,sheet_name='小区')
    sechand_filter.to_excel(writer,sheet_name='二手房')
    rent_filter.to_excel(writer,sheet_name='租房')
    writer.save()



