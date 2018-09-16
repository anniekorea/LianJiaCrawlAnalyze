# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 22:08:44 2018

@author: Annie
"""

import pymongo
import pandas as pd
from lxml import etree
from get_html_district_sechand import save_df_mongodb

def parse_html_rent_house(html):

    #使用lxml库的xpath方法对页面进行解析
    link=etree.HTML(html,parser=etree.HTMLParser(encoding='utf-8'))
        
    #1、提取房源名称
    rent_name=link.xpath('//div[@class="info-panel"]/h2/a/text()')

    #2、提取房源链接
    href=link.xpath('//div[@class="info-panel"]/h2/a/@href')
    rent_id=link.xpath('//ul[@id="house-lst"]/li/@data-id')
    
    #3、提取房源位置信息
    where=link.xpath('//div[@class="where"]')
    resblock=[]
    house_type=[]
    area=[]
    direction=[]
    for a in where:
        resblock.append(a.xpath('./a/span/text()')[0])
        house_type.append(a.xpath('./span[@class="zone"]/span/text()')[0])
        b=a.xpath('./span/text()')
        area.append(float(b[0][:-4]))
        direction.append(b[1])

    #4、提取房源信息
    con=link.xpath('//div[@class="con"]')
    small_district_cn=[]
    floor=[]
    built_year=[]
    for a in con:
        small_district_cn.append(a.xpath('./a/text()')[0][:-2])
        b=a.xpath('./text()')
        floor.append(b[0])
        built_year.append(b[1])
    
    #5、月租
    price=link.xpath('//div[@class="price"]/span/text()')
    rent_price=[]
    for a in price:
        rent_price.append(int(a))
    
    #6、更新时间
    update_date=link.xpath('//div[@class="price-pre"]/text()')
    
    #7、是否是自如房,若是，值为“自如”，若否，值为空
    pic_panel=link.xpath('//div[@class="pic-panel"]')
    ziru=[]
    for a in pic_panel:
        b=a.xpath('./div/text()')
        if len(b)==0:
            ziru.append('')
        else:
            ziru.append(b[0])
       
    #创建数据表
    rent=pd.DataFrame({'rent_id':rent_id,'rent_name':rent_name,'rent_price':rent_price,
                             'link':href,'resblock':resblock,'house_type':house_type,'area':area,
                             'direction':direction,'small_district_cn':small_district_cn,
                             'floor':floor,'built_year':built_year,'ziru':ziru,'update_date':update_date}) 
    
    rent['unit_price']=rent['rent_price']/rent['area']
    
    #去除字符串的头尾空格
    for j in range(0,rent.columns.size):
        try:
            colname=rent.columns[j]
            rent.loc[rent.loc[:,colname].isnull()==False,colname]=\
            rent.loc[rent.loc[:,colname].isnull()==False,colname].map(str.strip)
        except:
            pass    
    
    return rent



#设置参数
class Para:
    city_datestr_list=[['sh','20180916']] #,['hz','20180828']

    district_path='./data_district' #小区域列表所在文件夹
    
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table_input = database['html_district_rent']   #保存二手房html数据的数据表
    table_output = database['data_district_rent']   #保存二手房整理后数据的数据表
    
    
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
            district_df=pd.read_csv('%s/链家租房小区域列表%s.csv'%(para.district_path,city),engine='python',encoding='utf_8_sig')
            
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
                        rent=parse_html_rent_house(html)
                        rent['small_district_pinyin']=district
                        
                        if i==0:
                            data_rent=rent
                        else:
                            data_rent=data_rent.append(rent,ignore_index=True)
                
                       
            #去重
            data_rent_unique_id = data_rent[~data_rent['rent_id'].duplicated()]
            
            #保存数据到数据库
            save_df_mongodb(data_rent_unique_id,para.database,para.table_output,city,datestr)
            
            print('%s,%s已处理完！！！！！！'%(city,datestr))

    #关闭数据库
    para.client.close()
