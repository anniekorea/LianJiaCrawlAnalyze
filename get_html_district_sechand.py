# -*- coding: utf-8 -*-
"""
Created on Fri Aug 17 16:02:32 2018

@author: Administrator
"""

import requests
import time
import random
from lxml import etree
import pandas as pd
import os
import datetime
import json
import pymongo

#爬取区域的链接：文字text，链接href，拼音pinyin
def get_big_district_link(url,headers):
    #url='https://sh.lianjia.com/ershoufang/'
    r=requests.get(url,headers=headers,timeout=30)
    html=r.content
    link=etree.HTML(html,parser=etree.HTMLParser(encoding='utf-8'))

    name_cn=link.xpath('//div[@data-role="ershoufang"]/div[1]/a/text()')    
    href=link.xpath('//div[@data-role="ershoufang"]/div[1]/a/@href')
    district=pd.DataFrame({'name_cn':name_cn,'href':href})
    href_split=pd.DataFrame(x.split('/') for x in district['href'])
    district['name_pinyin']=href_split[2]
    
    district['link']=pd.DataFrame(url+x+'/' for x in district['name_pinyin'])

    return district

def get_small_district_link(big_district_link,headers):
    #big_district_link='https://sh.lianjia.com/ershoufang/pudong/'
    r=requests.get(big_district_link,headers=headers,timeout=30)
    html=r.content
    link=etree.HTML(html,parser=etree.HTMLParser(encoding='utf-8'))
    
    name_cn=link.xpath('//div[@data-role="ershoufang"]/div[2]/a/text()')
    href=link.xpath('//div[@data-role="ershoufang"]/div[2]/a/@href')
    district=pd.DataFrame({'name_cn':name_cn,'href':href})
    href_split=pd.DataFrame(x.split('/') for x in district['href'])
    district['name_pinyin']=href_split[2]

    district_link=big_district_link[:-(len(big_district_link.split('/')[-2])+1)] #作用是删掉'大区域/'，因为小区域的链接是ershoufang/小区域，而不是ershoufang/大区域/小区域
    district['link']=pd.DataFrame(district_link+x+'/' for x in district['name_pinyin'])
    
    return district

#获取小区域的链接
def get_district_link(url,city,headers,save_path='./data_district'):
    #url='https://sh.lianjia.com/ershoufang/'
    try:
        #小区域数据来源一：直接从文件读取小区域的链接
        df=pd.read_csv('%s/链家二手房小区域列表%s.csv'%(save_path,city),engine='python',encoding='utf_8_sig')
        print('查询到“链家二手房小区域列表%s.csv”，直接导入...'%city)
        #district_list.head()
    except:
        ##小区域数据来源二：从网站上爬取
        big_district=get_big_district_link(url,headers)
        len_big_district=len(big_district)
        print('未查询到“链家二手房小区域列表%s.csv”，共%s个大区域，爬取并保存...'%(city,len_big_district))
        for i in range(0,len(big_district)):
            if i<len(big_district)-1:
                print(i,end=',')
            else:
                print(i)
            
            big_district_link=big_district['link'][i]
            try:
                small_district=get_small_district_link(big_district_link,headers)
                if i==0:
                    df=small_district
                else:
                    df=df.append(small_district,ignore_index=True)
            except:
                print(big_district_link+'有问题，已忽略！')
                continue
            
            #为了避免被反爬，设置间隔时间，经测试，设成1-5秒比较合适，再短可能就会中断
            time_interval = random.uniform(1,5)
            time.sleep(time_interval)
        save_df_to_csv(df,'链家二手房小区域列表'+city,save_path)
    return df #(district_name_pinyin,district_link)

#获取页码，用于构造分页的链接
def get_totalpage_num(url,headers):
    #url='https://sh.lianjia.com/ershoufang/beicai/'
    r=requests.get(url,headers=headers,timeout=30)
    html=r.content
    link=etree.HTML(html,parser=etree.HTMLParser(encoding='utf-8'))
    page=link.xpath('//div[@class="contentBottom clear"] \
                    /div[@class="page-box fr"] \
                    /div[@class="page-box house-lst-page-box"] \
                    /@page-data')
    if len(page)==0:
        total_page=0
    else:
        page=page[0]#获取页码所在字符串
        page_dic=json.loads(page)
        total_page=page_dic.get("totalPage")
    
    return total_page

#循环抓取列表页信息
def get_html_many_pages(url,total_page,headers):
    #total_page=get_totalpage_num(url,headers)
    for i in range(0+1,total_page+1):
        if i<total_page:
            print(i,end=',')
        else:
            print(i)
        
        a='%spg%s/'%(url,i)
        r=requests.get(a,headers=headers,timeout=30)
            
        if i == 1:
            html=r.content
        else:
            html = html + r.content
        
        #为了避免被反爬，设置间隔时间，经测试，设成1-5秒比较合适，再短可能就会中断
        time_interval = random.uniform(1,5)
        time.sleep(time_interval)
    return html

#保存字符串到txt文件
def save_str_to_txt(file,file_name,save_path='../LianJiaSaveData/save_html_data/city_date'):    
    if isinstance(file,str):
        file_str=file
    else:
        file_str=file.decode("utf-8")
    
    #判断是否存在文件夹如果不存在则创建为文件夹
    folder = os.path.exists(save_path)
    if not folder: 
        os.makedirs(save_path)
    
    fh = open('%s/%s.txt'%(save_path,file_name), 'w', encoding='utf-8')
    fh.write(file_str)
    fh.close()

#保存dataframe到csv文件
def save_df_to_csv(file,file_name,save_path):
    #判断是否存在文件夹如果不存在则创建为文件夹
    folder = os.path.exists(save_path)
    if not folder:
        os.makedirs(save_path)
    
    file.to_csv('%s/%s.csv'%(save_path,file_name),index=False,encoding='utf_8_sig')

#保存dataframe到数据库database中
def save_df_mongodb(df,database,table,city,datestr):
    # import pymongo
    # import pandas as pd
    # df为dataframe
    #client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    #database = client[database_name] # 数据库的名字（mongo中没有这个数据库就创建）  
    #table = database[table_name]  # 表名
    df1=df.copy()
    df1['city']=city
    df1['datestr']=datestr
    data=df1.to_dict(orient='records') #dataframe转为dict
    table.insert_many(data)

#从数据库database中读取数据
def read_df_mongodb(database,table,city,datestr):
    #import pymongo
    #import pandas as pd
    #client = pymongo.MongoClient('localhost', 27017)
    #database = client[database_name]
    #table = database[table_name]
    df=pd.DataFrame(list(table.find({'city':city,'datestr':datestr})))
    del df['_id']
    
    return df

#设置参数
class Para:
    city_list=['sh','hz'] #,'bj','sz','gz','nj','wh','cd','hui','cs','xa','xm'
    url_base_list=['https://%s.lianjia.com/ershoufang/'%x for x in city_list]
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
    table = database['html_district_sechand']
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
        url_base=para.url_base_list[k]
        #获取小区域列表
        district_link_list=get_district_link(url_base,city,headers=para.headers,save_path=para.district_path)
        
        while True:
            error_log=''  #记录爬取过程中出错的链接，最后给出提示
            
            for i in range(0,len(district_link_list)):
                district_link=district_link_list['link'][i]
                district=district_link_list['name_pinyin'][i]
                
                #文件存储、数据库存储，二选一：
                #filename='html_%s'%district
                #save_path='%s/%s_%s'%(para.html_sechand_path,city,para.datestr)
                #if os.path.exists(save_path+'/'+filename+'.txt'):
                if para.table.find_one({'city':city,'datestr':para.datestr,'district':district}):
                
                    print('%s/%s,%s,文件已存在，跳过'%(i,len(district_link_list),district))
                    continue
                else:
                    try:
                        total_page=get_totalpage_num(district_link,headers=para.headers)
                        print('%s/%s,%s,共%s页'%(i,len(district_link_list),district,total_page))
                        if total_page==0:
                            html=''
                            df=pd.DataFrame([{'district':district,'html':''}])
                        else:
                            html=get_html_many_pages(district_link,total_page,headers=para.headers)
                            df=pd.DataFrame([{'district':district,'html':html.decode("utf-8")}])
                            
                            #文件存储、数据库存储，二选一
                            #save_str_to_txt(html,filename,save_path)
                            save_df_mongodb(df,para.database,para.table,city,para.datestr)
                    
                    except Exception as ex:
                        print(Exception,":",ex)
                        error_log=error_log+'%s,%s,%s出错\n'%(city,i,district)
                        continue
            #若有出错的district，则重新爬取   
            if not(error_log==''):
                print(city+'有数据未爬取成功，重新爬取......')
            else:
                print(city+'已完成！！！！！！')
                break

    para.client.close()



