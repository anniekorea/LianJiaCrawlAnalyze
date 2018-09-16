# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 20:51:43 2018

@author: Annie
"""

import pymongo
import pandas as pd
from lxml import etree
from get_html_district_sechand import save_df_mongodb

def parse_html_resblock(html):

    #使用lxml库的xpath方法对页面进行解析
    link=etree.HTML(html,parser=etree.HTMLParser(encoding='utf-8'))
        
    #1、提取小区名称
    resblock_name=link.xpath('//div[@class="info"]/div[@class="title"]')
    rn=[]
    for a in resblock_name:
        x=a.xpath('.//text()')[1]
        rn.append(x)
    
    #2、提取小区链接
    href=link.xpath('//div[@class="info"]/div[@class="title"]/a/@href')
    resblock_id=link.xpath('//li[@class="clear xiaoquListItem"]/@data-id')
    
    #3、提取小区信息
    houseInfo=link.xpath('//div[@class="houseInfo"]')   
    deal=[]
    rent=[]
    for a in houseInfo:
        deal.append(a.xpath('.//text()')[2])
        rent.append(a.xpath('.//text()')[5])
    
    #4、提取小区位置信息
    positionInfo=link.xpath('//div[@class="positionInfo"]')
    big_district=[]
    small_district=[]
    built_year=[]
    for a in positionInfo:
        big_district.append(a.xpath('.//text()')[2])
        small_district.append(a.xpath('.//text()')[4])
        b=a.xpath('.//text()')[5]
        b=b.strip().split('/')[1].strip()
        built_year.append(b)
    
    #5、小区二手挂牌均价
    totalPrice=link.xpath('//div[@class="totalPrice"]/span/text()')
    
    #6、小区在售二手房套数
    houseNum=link.xpath('//div[@class="xiaoquListItemSellCount"]/a/span/text()')
    
    
    #创建数据表
    resblock=pd.DataFrame({'resblock_id':resblock_id,'name_cn':rn,'link':href,'deal_num':deal,'rent_num':rent,
                         'big_district_cn':big_district,'small_district_cn':small_district,'built_year':built_year,
                        'avg_price_sechand':totalPrice,'sechand_for_sale':houseNum}) 

    return resblock



#设置参数
class Para:
    city_datestr_list=[['sh','20180828'],['hz','20180828'],['sz','20180828'],
                       ['gz','20180828'],['nj','20180828'],['wh','20180828'],
                       ['cd','20180828'],['cs','20180828'],['xm','20180828']]

    headers={'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
       'Accept-Language': 'zh-CN,zh;q=0.9',
       'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
    district_path='./data_district' #小区域列表所在文件夹
    
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table_input = database['html_district_resblock']   #保存二手房html数据的数据表
    table_output = database['data_district_resblock']   #保存二手房整理后数据的数据表
    
    
#准备工作：运行get_html_resblock，得到各小区域的居民区（小区）html
#解析html数据，并保存整理后的二手房数据

if __name__ == '__main__':
    #参数实例化
    para=Para()
    len_city=len(para.city_datestr_list)
    
    for k in range(0,len_city):
        city=para.city_datestr_list[k][0]
        datestr=para.city_datestr_list[k][1]
        
        if para.table_output.find_one({'city':city,'datestr':datestr}):
            print('%s,%s,数据已存在，跳过'%(city,datestr))
            continue
        else:         
            district_df=pd.read_csv('%s/链家居民区小区域列表%s.csv'%(para.district_path,city),engine='python',encoding='utf_8_sig')
            
            district_link_list=district_df['link']
            district_list=district_df['name_pinyin']
            
            print('%s,%s,共%s个小区域，读取中......'%(city,datestr,len(district_link_list)))
            
            #按小区域逐个处理其html文件
            for i in range(0,len(district_link_list)):
                if i<len(district_link_list)-1:
                    print(i,end=',')
                else:
                    print(i)
                
                district=district_list[i]
                
                #从数据库读取html文件
                try:
                    html_dict= para.table_input.find_one({'city':city,'datestr':datestr,'district':district})
                except Exception as ex:
                    print(ex)
                    break
                
                #解析html文件
                if not(html_dict==None):
                    html=html_dict['html']
                    if not(html==''):
                        resblock=parse_html_resblock(html)
                        resblock['small_district_pinyin']=district
                        
                        if i==0:
                            data_resblock=resblock
                        else:
                            data_resblock=data_resblock.append(resblock,ignore_index=True)
                
                       
            #去重
            data_resblock_unique_id = data_resblock[~data_resblock['resblock_id'].duplicated()]
            
            #保存数据到数据库
            save_df_mongodb(data_resblock_unique_id,para.database,para.table_output,city,datestr)
            
            print('%s,%s已处理完！！！！！！'%(city,datestr))

    #关闭数据库
    para.client.close()
