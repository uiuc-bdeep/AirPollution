import requests
import json
import schedule
import requests
import pytz
import datetime
from pymongo import MongoClient


client = MongoClient('141.142.209.153')
db = client['weather_IL']

def timezone():
   weekday=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
   month=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
   tz = pytz.timezone('America/Chicago')
   now = datetime.datetime.now(tz)
   tztime1=weekday[now.weekday()]+'_'+month[now.month-1]+'_'+str(now.day)+'_'+str(now.hour)+'_'+str(now.minute)
   return tztime1

while (True):
    result = requests.get("http://api.openweathermap.org/data/2.5/box/city?bbox=-91,37,-87,42,1000&cluster=yes&appid=42b6a23a2c7ea5c6ad1dc859507023a0")
    jsonresult = json.loads(result.text)
    for i in range(len(jsonresult['list'])):
        time = timezone()
        cityname = jsonresult['list'][i]['name']
        lat1 = jsonresult['list'][i]['coord']['lat']
        lon1 = jsonresult['list'][i]['coord']['lon']
        weather1 =jsonresult['list'][i]['weather'][0]['main']
        descrip = jsonresult['list'][i]['weather'][0]['description']
        winds_speed =  jsonresult['list'][i]['wind']['speed']
        winds_degree= jsonresult['list'][i]['wind']['deg']
        weather_record = {
            "time":time,
            "city":cityname,
            "lat":lat1,
            "lon":long1,
            "weather":weather1,
            "weather_description":descrip,
            "Wind_speed":winds_speed,
            "Wind_degree":winds_degree,
            }
        db.weather.insert(weather_record)
        time.sleep(1800)