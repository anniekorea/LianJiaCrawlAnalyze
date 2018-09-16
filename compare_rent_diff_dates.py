# -*- coding: utf-8 -*-
"""
Created on Sun Sep 16 09:44:10 2018

@author: Annie
"""

import pandas as pd
import numpy as np
import datetime
import pymongo

#对比同一个城市，不同时期的房源数据
def compare_two_date(house1,house2,city1,datestr1,city2,datestr2):    
    house_merge=house1.merge(house2,on='rent_id',how='inner')
    house_merge['diff_rent_price']=house_merge['rent_price_y']-house_merge['rent_price_x']
    house_merge['diffpct_rent_price']=house_merge['rent_price_y']/house_merge['rent_price_x']-1
    
    diff_tp=house_merge['diff_rent_price']
    diffpct_tp=house_merge['diffpct_rent_price']
    
    diff_count=len(house2)-len(house1)
    if diff_count>0:
        a1='%s-%s有%d套房源，%s-%s有%d套房源，增加%d套'%(city1,datestr1,len(house1),city2,datestr2,len(house2),diff_count)
    elif diff_count<0:
        a1='%s-%s有%d套房源，%s-%s有%d套房源，减少%d套'%(city1,datestr1,len(house1),city2,datestr2,len(house2),diff_count)
    else:
        a1='%s-%s有%d套房源，%s-%s有%d套房源，二手房房源数无变化'%(city1,datestr1,len(house1),city2,datestr2,len(house2),diff_count)

    a2='，两个文件共同的房源%d套'%len(diff_tp)
    
    diff_mean_price=np.mean(house2['unit_price'])-np.mean(house1['unit_price'])
    if diff_mean_price>0.01:
        a9='%s-%s的单价平均值为%.2f元，%s-%s的单价平均值为%.2f元，上涨%.2f元'%\
            (city1,datestr1,np.mean(house1['unit_price']),
            city2,datestr2,np.mean(house2['unit_price']),diff_mean_price)
    elif diff_mean_price<-0.01:
        a9='%s-%s的单价平均值为%.2f元，%s-%s的单价平均值为%.2f元，下跌%.2f元'%\
            (city1,datestr1,np.mean(house1['unit_price']),
            city2,datestr2,np.mean(house2['unit_price']),diff_mean_price)
    else:
        a9='%s-%s的单价平均值为%.2f元，%s-%s的单价平均值为%.2f元，基本保持不变'%\
            (city1,datestr1,np.mean(house1['unit_price']),
            city2,datestr2,np.mean(house2['unit_price']))
    
    a3='其中%d套房源价格上调，占比%.4f%%，平均上涨%.2f元，平均涨幅%.2f%%'%\
        (sum(diff_tp>0),sum(diff_tp>0)/len(house_merge),
         np.mean(diff_tp[diff_tp>0]),np.mean(diffpct_tp[diffpct_tp>0])*100)
    a4='其中%d套房源价格下调，占比%.4f%%，平均下跌%.2f元，平均跌幅%.2f%%'%\
        (sum(diff_tp<0),sum(diff_tp<0)/len(house_merge),
         np.mean(diff_tp[diff_tp<0]),np.mean(diffpct_tp[diffpct_tp<0])*100)
    
    house_merge_sortby_diffpct=house_merge.sort_values(by='diffpct_rent_price')
    a5='上调幅度最大的房源ID：%d，原价格：%.2f元-->现价格：%.2f元，涨幅%.2f%%'\
            %(house_merge_sortby_diffpct.tail(1)['rent_id'],
            house_merge_sortby_diffpct.tail(1)['rent_price_x'],
            house_merge_sortby_diffpct.tail(1)['rent_price_y'],
            house_merge_sortby_diffpct.tail(1)['diffpct_rent_price']*100)
    a6='下调幅度最大的房源ID：%d，原价格：%.2f元-->现价格：%.2f元，跌幅%.2f%%'\
            %(house_merge_sortby_diffpct.head(1)['rent_id'],
            house_merge_sortby_diffpct.head(1)['rent_price_x'],
            house_merge_sortby_diffpct.head(1)['rent_price_y'],
            house_merge_sortby_diffpct.head(1)['diffpct_rent_price']*100)
    
    house_merge_sortby_diffpct=house_merge[house_merge['rent_price_x']<=10000].sort_values(by='diffpct_rent_price')
    a7='月租1万元以下，上调幅度最大的房源ID：%d，原价格：%.2f元-->现价格：%.2f元，涨幅%.2f%%'\
            %(house_merge_sortby_diffpct.tail(1)['rent_id'],
            house_merge_sortby_diffpct.tail(1)['rent_price_x'],
            house_merge_sortby_diffpct.tail(1)['rent_price_y'],
            house_merge_sortby_diffpct.tail(1)['diffpct_rent_price']*100)  
    a8='月租1万元以下，下调幅度最大的房源ID：%d，原价格：%.2f元-->现价格：%.2f元，跌幅%.2f%%'\
            %(house_merge_sortby_diffpct.head(1)['rent_id'],
            house_merge_sortby_diffpct.head(1)['rent_price_x'],
            house_merge_sortby_diffpct.head(1)['rent_price_y'],
            house_merge_sortby_diffpct.head(1)['diffpct_rent_price']*100)
    txt=a1+a2+'\r\n'+a9+'\r\n'+a3+'\r\n'+a4+'\r\n'+a5+'\r\n'+a6+'\r\n'+a7+'\r\n'+a8+'\r\n'
    return txt,house_merge

class Para:
    city_datestr_list=[['sh','20180827'],['sh','20180916']]
    district_path='./data_district' #小区域列表所在文件夹
    client = pymongo.MongoClient('localhost', 27017) # 链接本地的数据库 默认端口号的27017
    database = client['LianJia'] # 数据库的名字（mongo中没有这个数据库就创建）
    table = database['data_district_rent']   #保存租房数据的数据表
    save_file='../LianJiaSaveData/save_analyze_rent'

if __name__ == '__main__':
    para=Para()
    
    city1=para.city_datestr_list[0][0]
    datestr1=para.city_datestr_list[0][1]
    city2=para.city_datestr_list[1][0]
    datestr2=para.city_datestr_list[1][1]
    
    house1=pd.DataFrame(list(para.table.find({'city':city1,'datestr':datestr1,'ziru':''}) )) #非自如房
    house2=pd.DataFrame(list(para.table.find({'city':city2,'datestr':datestr2,'ziru':''}) )) #非自如房
    
    analyze_txt,house_merge=compare_two_date(house1,house2,city1,datestr1,city2,datestr2)
   
    todaystr=datetime.datetime.now().strftime('%Y%m%d')
    fh = open('%s/%s不同日期（%s-%s)租房分析对比结果.txt'%(para.save_file,city1,datestr1,datestr2),
             'w', encoding='utf-8')
    fh.write(analyze_txt)
    fh.close()
    
    house_merge=house_merge[['rent_id','link_x','rent_name_x','area_x','house_type_x',
                            'direction_x','floor_x','resblock_x','built_year_x','small_district_cn_x',
                            'rent_price_x','rent_price_y','unit_price_x','unit_price_y',
                            'diff_rent_price','diffpct_rent_price','ziru_x','update_date_x',
                            'update_date_y','datestr_x','datestr_y','city_x']]

    
    house_merge.to_csv('%s/%s不同日期（%s-%s)租房分析对比结果.csv'%(para.save_file,city1,datestr1,datestr2),
                      index=False,encoding='utf_8_sig')