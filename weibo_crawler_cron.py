from shapely.geometry import Polygon
import shapefile
from geopy.distance import vincenty
from weibo import Client
import time
import schedule
import datetime
from geopy.distance import vincenty
import pytz

DATAPATH='/data/AirPollution/data2016fa_day/'


def run_crawler(Crawl_date, Shp_for_data):#Set_of_Geo_location list Crawl_data str shp_for_data
    print "running"
    print datetime.datetime.now()
    try:
        aa = d6.get('place/nearby_timeline',page=1,lat=39.95,long=116.47,range=10000,count=50)
    #print aa
        print type(aa)
    #try:
        if 'statuses' in aa and 'states' in aa:
            for i in range (len(aa['statuses'])):
                if 'geo' in aa['statuses'][i].keys():
                        #if aa['statuses'][i]['id'] not in dicts:
                    #    dicts[aa['statuses'][i]['id']]=aa['statuses'][i]['text'].encode('utf-8')
                    #print (aa['statuses'][i]['geo']['coordinates'][0],aa['statuses'][i]['geo']['coordinates'][1])
                    Shp_for_data.point(aa['statuses'][i]['geo']['coordinates'][1],aa['statuses'][i]['geo']['coordinates'][0])
                    a=aa['states'][i]['id']
                    b=aa['statuses'][i]['created_at']
                    c=aa['statuses'][i]['text'].encode('utf-8')
                    d='Beijing'
                    e=get_distance_mile((39.5,116.47),(aa['statuses'][i]['geo']['coordinates'][0],aa['statuses'][i]['geo']['coordinates'][1]))
                    f=get_distance_meter((39.5,116.47),(aa['statuses'][i]['geo']['coordinates'][0],aa['statuses'][i]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Beijing has problem"

    try:
        bb= d7.get('place/nearby_timeline',page=1,lat=30.63,long=104.07,range=10000,count=50)
        if 'statuses' in bb and 'states' in bb:
            for j in range (len(bb['statuses'])):
                if 'geo' in bb['statuses'][j].keys():
                    Shp_for_data.point(bb['statuses'][j]['geo']['coordinates'][1],bb['statuses'][j]['geo']['coordinates'][0])
                    a=bb['states'][j]['id']
                    c=bb['statuses'][j]['text'].encode('utf-8')
                    b=bb['statuses'][j]['created_at']
                    d='Chengdu'
                    e=get_distance_mile((30.63,104.07),(bb['statuses'][j]['geo']['coordinates'][0],bb['statuses'][j]['geo']['coordinates'][1]))
                    f=get_distance_meter((30.63,104.07),(bb['statuses'][j]['geo']['coordinates'][0],bb['statuses'][j]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Chengdu has problem"

    try:
        cc= d8.get('place/nearby_timeline',page=1,lat=23.12,long=113.32,range=10000,count=50)
    
        if 'statuses' in cc and 'states' in cc:
            for z in range (len(cc['statuses'])):
                if 'geo' in cc['statuses'][z].keys():
                    Shp_for_data.point(cc['statuses'][z]['geo']['coordinates'][1],cc['statuses'][z]['geo']['coordinates'][0])
                    a=cc['states'][z]['id']
                    c=cc['statuses'][z]['text'].encode('utf-8')
                    b=cc['statuses'][z]['created_at']
                    d='Guangzhou'
                    e=get_distance_mile((23.12,113.32),(cc['statuses'][z]['geo']['coordinates'][0],cc['statuses'][z]['geo']['coordinates'][1]))
                    f=get_distance_meter((23.12,113.32),(cc['statuses'][z]['geo']['coordinates'][0],cc['statuses'][z]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Guangzhou has problem"

    try:
        dd = d9.get('place/nearby_timeline',page=1,lat=31.21,long=121.44,range=10000,count=50)
    
        if 'statuses' in dd and 'states' in dd:
            for i in range (len(dd['statuses'])):
                if 'geo' in dd['statuses'][i].keys():
                    Shp_for_data.point(dd['statuses'][i]['geo']['coordinates'][1],dd['statuses'][i]['geo']['coordinates'][0])
                    a=dd['states'][i]['id']
                    c=dd['statuses'][i]['text'].encode('utf-8')
                    b=dd['statuses'][i]['created_at']
                    d='Shanghai'
                    e=get_distance_mile((31.21,121.44),(dd['statuses'][i]['geo']['coordinates'][0],dd['statuses'][i]['geo']['coordinates'][1]))
                    f=get_distance_meter((31.21,121.44),(dd['statuses'][i]['geo']['coordinates'][0],dd['statuses'][i]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Shanghai has problem"

    try:
        ee = d10.get('place/nearby_timeline',page=1,lat=41.78,long=123.42,range=10000,count=50)
        if 'statuses' in ee and 'states' in ee:
            for i in range (len(ee['statuses'])):
                if 'geo' in ee['statuses'][i].keys():
                    Shp_for_data.point(ee['statuses'][i]['geo']['coordinates'][1],ee['statuses'][i]['geo']['coordinates'][0])
                    a=ee['states'][i]['id']
                    c=ee['statuses'][i]['text'].encode('utf-8')
                    b=ee['statuses'][i]['created_at']
                    d='Shenyang'
                    e=get_distance_mile((41.78,123.42),(ee['statuses'][i]['geo']['coordinates'][0],ee['statuses'][i]['geo']['coordinates'][1]))
                    f=get_distance_meter((41.78,123.42),(ee['statuses'][i]['geo']['coordinates'][0],ee['statuses'][i]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Shenyang has problem"
    print "finish running"
    return None


def run_crawler_edit(Crawl_date, Shp_for_data):#Set_of_Geo_location list Crawl_data str shp_for_data
    print "running"
    print datetime.datetime.now()
    try:
        aa = d6.get('place/nearby_timeline',page=1,lat=39.95,long=116.47,range=10000,count=50)
    #print aa
        print type(aa)
    #try:
        if 'statuses' in aa and 'states' in aa:
            for i in range (len(aa['statuses'])):
                if 'geo' in aa['statuses'][i].keys():
                        #if aa['statuses'][i]['id'] not in dicts:
                    #    dicts[aa['statuses'][i]['id']]=aa['statuses'][i]['text'].encode('utf-8')
                    #print (aa['statuses'][i]['geo']['coordinates'][0],aa['statuses'][i]['geo']['coordinates'][1])
                    Shp_for_data.point(0,0,aa['statuses'][i]['geo']['coordinates'][1],aa['statuses'][i]['geo']['coordinates'][0])
                    a=aa['states'][i]['id']
                    b=aa['statuses'][i]['created_at']
                    c=aa['statuses'][i]['text'].encode('utf-8')
                    d='Beijing'
                    e=get_distance_mile((39.5,116.47),(aa['statuses'][i]['geo']['coordinates'][0],aa['statuses'][i]['geo']['coordinates'][1]))
                    f=get_distance_meter((39.5,116.47),(aa['statuses'][i]['geo']['coordinates'][0],aa['statuses'][i]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Beijing has problem"

    try:
        bb= d7.get('place/nearby_timeline',page=1,lat=30.63,long=104.07,range=10000,count=50)
        if 'statuses' in bb and 'states' in bb:
            for j in range (len(bb['statuses'])):
                if 'geo' in bb['statuses'][j].keys():
                    Shp_for_data.point(0,0,bb['statuses'][j]['geo']['coordinates'][1],bb['statuses'][j]['geo']['coordinates'][0])
                    a=bb['states'][j]['id']
                    c=bb['statuses'][j]['text'].encode('utf-8')
                    b=bb['statuses'][j]['created_at']
                    d='Chengdu'
                    e=get_distance_mile((30.63,104.07),(bb['statuses'][j]['geo']['coordinates'][0],bb['statuses'][j]['geo']['coordinates'][1]))
                    f=get_distance_meter((30.63,104.07),(bb['statuses'][j]['geo']['coordinates'][0],bb['statuses'][j]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Chengdu has problem"

    try:
        cc= d8.get('place/nearby_timeline',page=1,lat=23.12,long=113.32,range=10000,count=50)
    
        if 'statuses' in cc and 'states' in cc:
            for z in range (len(cc['statuses'])):
                if 'geo' in cc['statuses'][z].keys():
                    Shp_for_data.point(0,0,cc['statuses'][z]['geo']['coordinates'][1],cc['statuses'][z]['geo']['coordinates'][0])
                    a=cc['states'][z]['id']
                    c=cc['statuses'][z]['text'].encode('utf-8')
                    b=cc['statuses'][z]['created_at']
                    d='Guangzhou'
                    e=get_distance_mile((23.12,113.32),(cc['statuses'][z]['geo']['coordinates'][0],cc['statuses'][z]['geo']['coordinates'][1]))
                    f=get_distance_meter((23.12,113.32),(cc['statuses'][z]['geo']['coordinates'][0],cc['statuses'][z]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Guangzhou has problem"

    try:
        dd = d9.get('place/nearby_timeline',page=1,lat=31.21,long=121.44,range=10000,count=50)
    
        if 'statuses' in dd and 'states' in dd:
            for i in range (len(dd['statuses'])):
                if 'geo' in dd['statuses'][i].keys():
                    Shp_for_data.point(0,0,dd['statuses'][i]['geo']['coordinates'][1],dd['statuses'][i]['geo']['coordinates'][0])
                    a=dd['states'][i]['id']
                    c=dd['statuses'][i]['text'].encode('utf-8')
                    b=dd['statuses'][i]['created_at']
                    d='Shanghai'
                    e=get_distance_mile((31.21,121.44),(dd['statuses'][i]['geo']['coordinates'][0],dd['statuses'][i]['geo']['coordinates'][1]))
                    f=get_distance_meter((31.21,121.44),(dd['statuses'][i]['geo']['coordinates'][0],dd['statuses'][i]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Shanghai has problem"

    try:
        ee = d10.get('place/nearby_timeline',page=1,lat=41.78,long=123.42,range=10000,count=50)
        if 'statuses' in ee and 'states' in ee:
            for i in range (len(ee['statuses'])):
                if 'geo' in ee['statuses'][i].keys():
                    Shp_for_data.point(0,0,ee['statuses'][i]['geo']['coordinates'][1],ee['statuses'][i]['geo']['coordinates'][0])
                    a=ee['states'][i]['id']
                    c=ee['statuses'][i]['text'].encode('utf-8')
                    b=ee['statuses'][i]['created_at']
                    d='Shenyang'
                    e=get_distance_mile((41.78,123.42),(ee['statuses'][i]['geo']['coordinates'][0],ee['statuses'][i]['geo']['coordinates'][1]))
                    f=get_distance_meter((41.78,123.42),(ee['statuses'][i]['geo']['coordinates'][0],ee['statuses'][i]['geo']['coordinates'][1]))
                    Shp_for_data.record(a,b,c,d,e,f,get_date_hour(),get_hour())
    except:
        print "Shenyang has problem"
    print "finish running"
    return None




a6=547191276
b6='d053826b2de8f4f36c687655570e0909'
c6='https://github.com/ssong0429'
token6={u'access_token': u'2.00OCw7ID0k8xBb940305c3130oJ9rV', u'remind_in': u'157679999', u'uid': u'2873028802', u'expires_at': 1618817558}
d6=Client(a6,b6,c6,token6)

a7=566044226
b7='d8708645e9f4e4d793c175003a98e38c'
c7='https://github.com/ssong0429'
token7={u'access_token': u'2.00OCw7ID0axD_c23a29106baEn4s6C', u'remind_in': u'157679999', u'uid': u'2873028802', u'expires_at': 1618817713}
d7=Client(a7,b7,c7,token7)

a8=3230547592
b8='5400732031651646bfe382fd31152176'
c8='https://github.com/ssong0429'
token8={u'access_token': u'2.006hE39G1ADdWD7599d53a31TZVJMB', u'remind_in': u'157679999', u'uid': u'5915795683', u'expires_at': 1619938591}
d8 = Client(a8, b8, c8,token8 )

a9=138067624
b9='77a6f713b6e12551331658ccc3e6437f'
c9='https://github.com/ssong0429'
token9={u'access_token': u'2.006hE39G0Kh_2Jdd620f34db0CgjTW', u'remind_in': u'157679999', u'uid': u'5915795683', u'expires_at': 1619938864}
d9= Client(a9, b9, c9,token9 )

a10=701267132
b10='69cbee8bc3249126c62057147a548c8a'
c10='https://github.com/ssong0429'
token10={u'access_token': u'2.006hE39G0Mc89lc10a4c4c9eAWhcTC', u'remind_in': u'157679999', u'uid': u'5915795683', u'expires_at': 1619939137}
d10= Client(a10, b10, c10,token10)



def get_date():
    print "running get_date"
    weekday=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    month=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz)
    Crawl_date="{} {} {}". format(weekday[now.weekday()], month[now.month-1], now.day)
    return  Crawl_date

def get_date_name_file():
    print "running get_date"
    weekday=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    month=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz)
    #now=datetime.datetime.now()
    Crawl_date=weekday[now.weekday()]+'_'+month[now.month-1]+'_'+str(now.day)
    return  Crawl_date

def save_to_Shp(w,date):
    print "running save_to_Shp"
    w.save('shapefiles_FIVE_CITIES_hour/'+str(date)+'/'+str(date))


def get_distance_mile(a,b):
    result=vincenty(a, b).miles  
    return result 

def get_distance_meter(a,b):
    result=get_distance_mile(a,b)
    return result*1.6

def get_date_hour():
    print "running get_date"
    weekday=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    month=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz)
    #print now
    Crawl_date="{} {} {} {}". format(weekday[now.weekday()], month[now.month-1], now.day,now.hour)
    return  Crawl_date

def get_hour():
    print "running get_date"
    weekday=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    month=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz)
    print now
    Crawl_date="{}". format(now.hour)
    return  Crawl_date






def job():
    print "in job"
    running_time = str(get_date())
    PATH = DATAPATH + "/"+running_time
    if not os.path.exists(PATH):
        w = shapefile.Writer(shapefile.POINT)
        w.field('ID','C','30')                       #Create fields
        w.field('created_time','C','100')
        w.field('Chinese','C','200')
        w.field('City','C','40')
        w.field('Distance_mile','C','140')
        w.field('Distance_meter','C','140')
        w.field('Date_Hour','C','140')
        w.field('Hour','C','140')

        run_crawler(running_time,w)

        w.save(PATH +'/'+ running_time)

    else:
        e = shapefile.Editor(shapefile=PATH +'/'+ running_time+".shp")
        
        run_crawler_edit(running_time,e)

        e.save(PATH +'/'+ running_time)

    return


job()
time.sleep(1800)
job()
