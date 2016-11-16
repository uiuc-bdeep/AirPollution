import requests
import json
import schedule
import requests
import pytz
import datetime
from pymongo import MongoClient
import xml.etree.ElementTree as ET

client = MongoClient('141.142.209.153')
db = client['AirPollution']

def readnumber(cityname, URL):
    url = "https://hooks.slack.com/services/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"
    schedule_trips_msg = cityname + " AQI runs succesful."
    payload={"text": schedule_trips_msg}
    try:
        r = requests.post(url, data=json.dumps(payload))
    except requests.exceptions.RequestException as e:
        print e
    result = requests.get(URL)
    root = ET.fromstring(result.text)
    for item in root[0]:
        if(item.tag == "item"):
            time = item.find("title").text
            Concentration = item.find("Conc").text
            AQI = item.find("AQI").text
            AQI_India = {
                "time":time,
                "city":cityname,
                "Concentration":Concentration,
                "AQI":AQI
            }
            db.India_AQI.insert(AQI_India)
    
def main():
    while (True):
        readnumber('NewDelhi','http://stateair.net/dos/RSS/NewDelhi/NewDelhi-PM2.5.xml')
        readnumber('Mumbai','http://stateair.net/dos/RSS/Mumbai/Mumbai-PM2.5.xml')
        readnumber('Chennai','http://stateair.net/dos/RSS/Chennai/Chennai-PM2.5.xml')
        readnumber('Hyderabad','http://stateair.net/dos/RSS/Hyderabad/Hyderabad-PM2.5.xml')
        readnumber('Kolkata','http://stateair.net/dos/RSS/Kolkata/Kolkata-PM2.5.xml')
        time.sleep(3600)

main()