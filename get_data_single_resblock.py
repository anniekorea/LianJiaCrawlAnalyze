# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 20:49:03 2018

@author: Annie
"""
import pymongo
import pandas as pd
from lxml import etree
import re
from get_html_district_sechand import save_df_mongodb

def parse_html_single_resblock(html):   
    link=etree.HTML(html,parser=etree.HTMLParser(encoding='utf-8'))
    #经纬度
    a=link.xpath('//body/script[@type="text/javascript"]/text()')
    b=str(a[0])
    resblockPosition=re.findall(r"resblockPosition:\'(.*)\'",b)[0]
    [lon,lat]=resblockPosition.split(',')
    lon=float(lon)
    lat=float(lat)
    
    #小区信息:建筑年代，建筑类型，物业费用，物业公司，开发商，楼栋总数，房屋总数
    xiaoquInfoContent=link.xpath('//span[@class="xiaoquInfoContent"]/text()')
    [built_year,build_type,manage_fee,manage_comp,build_comp,build_num,
     house_num]=xiaoquInfoContent[:-1]
    
    built_year=str(built_year).strip()
    build_type=str(build_type).strip()
    manage_fee=str(manage_fee).strip()
    manage_comp=str(manage_comp).strip()
    build_comp=str(build_comp).strip()
    build_num=int(build_num[:-1])
    house_num=int(house_num[:-1])
    
    single_resblock_dict={'lon':lon,'lat':lat,'built_year':built_year,
                          'build_type':build_type, 'manage_fee':manage_fee,
                          'manage_comp':manage_comp,'build_comp':build_comp,
                          'build_num':build_num,'house_num':house_num}
    
    single_resblock=pd.DataFrame.from_dict(single_resblock_dict,orient='index').T
    
    return single_resblock


class Para:
    city_datestr_list=[['sh','20180830']]#,['hz','20180830']] 
    #,['sz','20180828'],['gz','20180828'],['nj','20180828'],['wh','20180828'],['cd','20180828'],['cs','20180828'],['xm','20180828']
    
    #数据库存储方式设置：
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table_input = database['html_single_resblock']    
    table_output = database['data_single_resblock']


#准备工作：运行get_html_single_resblock，得到所有单个居民区（小区）html
#解析html数据，并保存整理后的二手房数据

if __name__ == '__main__':
    #参数实例化
    para=Para()
    len_city=len(para.city_datestr_list)
    
    for k in range(0,len_city):
        city=para.city_datestr_list[k][0]
        datestr=para.city_datestr_list[k][1]
        
        error_log=''
        
        if para.table_output.find_one({'city':city,'datestr':datestr}):
            print('%s,%s,数据已存在，跳过'%(city,datestr))
            continue
        else:         
            #从数据库读取html文件
            try:
                html_dict=pd.DataFrame(list(para.table_input.find({'city':city,'datestr':datestr})))
            except Exception as ex:
                print(ex)
                break
            
            #解析html文件
            if len(html_dict)==0:
                print('%s数据库中无此数据！'%city)
                continue
            else:
                print('共%s个小区数据待处理：'%len(html_dict))
                for i in range(0,len(html_dict)):
                    if i<len(html_dict)-1:
                        print(i,end=',')
                    else:
                        print(i)
                    
                    html=html_dict.loc[i,'html']
                    if not(html==''):
                        try:
                            single_resblock=parse_html_single_resblock(html)
                            single_resblock['resblock_id']=html_dict.loc[i,'resblock_id']
                        
                            if i==0:
                                data_single_resblock=single_resblock
                            else:
                                data_single_resblock=data_single_resblock.append(single_resblock,ignore_index=True)
                        except Exception as ex:
                            error_log=error_log+'%s,%s,%s出错\n'%(city,i,html_dict.loc[i,'resblock_id'])
                            continue
            #去重
            data_single_resblock_unique_id = data_single_resblock[~data_single_resblock['resblock_id'].duplicated()]
            
            #保存数据到数据库
            save_df_mongodb(data_single_resblock_unique_id,para.database,para.table_output,city,datestr)
            
            #若有出错的resblock，进行提示   
            if not(error_log==''):
                print(city+'有数据未解析成功:')
                print(error_log)
            else:
                print('%s,%s已处理完！！！！！！'%(city,datestr))
            
    #关闭数据库
    para.client.close()
