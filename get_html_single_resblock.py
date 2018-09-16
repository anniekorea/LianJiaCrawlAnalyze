# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 23:43:33 2018

@author: Annie
"""

import pandas as pd
import datetime
import pymongo
import requests
import random
import time
from get_html_district_sechand import save_df_mongodb

def get_html(url,headers):
    r=requests.get(url,headers=headers,timeout=30)
    html=r.content
    time_interval = random.uniform(1,5)
    time.sleep(time_interval)
    return html
    
#设置参数
class Para:
    city_list=['sh','hz','bj','sz','gz','nj','wh','cd','cs','xm']
    #url_base_list=['https://%s.lianjia.com/xiaoqu/'%x for x in city_list]
    #设置请求头部信息,我们最好在http请求中设置一个头部信息，否则很容易被封ip。
    headers={'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
       'Accept-Language': 'zh-CN,zh;q=0.9',
       'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
    datestr=datetime.datetime.now().strftime('%Y%m%d')
    
    district_path='./data_district'
    
    #文件存储方式设置：
    #html_sechand_path='../LianJiaSaveData/html_district_sechand'
    #save_analyze_path='../LianJiaSaveData/save_analyze_result'
    
    #数据库存储方式设置：
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table_input = database['data_district_resblock']
    table_output = database['html_single_resblock']
    #按小区域爬取的二手房html：'html_district_sechand'
    #按小区域爬取的小区html：'html_district_resblock'
    #按小区域爬取的租房html：'html_district_rent'
    #按小区resblock爬取的小区html：'html_resblock'
    #高德api爬取的小区路线规划answer：'gaode_route_resblock'

    
#爬数据，并保存在子文件夹save_html_data中，每个日期一个文件夹，同一天的数据放在以日期命名的子文件夹中
#链家只能显示100页的数据，每页30个，不分区域最多只能爬取3000个数据
#房源数据有几万个，必须分区域爬取，且按浦东这种大区域也会超出3000个数据，所以必须分小区域
if __name__ == '__main__':
    #参数实例化
    para=Para()    
    len_city=len(para.city_list)
    
    for k in range(0,len_city):
        city=para.city_list[k]
        datestr=para.datestr
        
        district_resblock=pd.DataFrame(list(para.table_input.find({'city':city})))
        resblock_link_list=district_resblock['link']  
        resblock_id_list=district_resblock['resblock_id']
        
        while True:
            error_log=''  #记录爬取过程中出错的链接，最后给出提示
            
            for i in range(0,len(resblock_link_list)):
                resblock_link=resblock_link_list[i]
                resblock_id=resblock_id_list[i]
                
                if para.table_output.find_one({'city':city,'resblock_id':resblock_id}):           
                    print('%s/%s,%s,文件已存在，跳过'%(i,len(resblock_link_list),resblock_id))
                    continue
                else:
                    try:
                        print('%s/%s,%s,正在爬取...'%(i,len(resblock_link_list),resblock_id))
                        html=get_html(resblock_link,headers=para.headers)                        
                        df=pd.DataFrame([{'resblock_id':resblock_id,'html':html.decode("utf-8")}])
                        
                        save_df_mongodb(df,para.database,para.table_output,city,para.datestr)
                    
                    except Exception as ex:
                        print(Exception,":",ex)
                        error_log=error_log+'%s,%s,%s出错\n'%(city,i,resblock_id)
                        continue
            #若有出错的district，则重新爬取   
            if not(error_log==''):
                print(city+'有数据未爬取成功，重新爬取......')
            else:
                print(city+'已完成！！！！！！')
                break

    para.client.close()