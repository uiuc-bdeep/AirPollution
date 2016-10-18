#!/usr/bin/env python

import requests
import json
import csv
import os
import time
import pytz
import datetime
import xml.etree.ElementTree as ET
import logging

from bdeep.context import generateOutputPath, getJobArgs, generateProjectPath

log = logging.getLogger("BDEEP")

def air_parser(URL, URL2, city_name):

	result = requests.get(URL)
	result2 = requests.get(URL2)

	root = ET.fromstring(result.text)

	temp = []

	for item in root[0]:

		if(item.tag == "item"):
			# from URL
			time = item.find("title").text
			time = get_date_name_file()
			PM25 = item.find("Conc").text
			if(PM25 == "-999.0"):
				PM25 = ""
			AQI = item.find("AQI").text
			if(AQI == "-999"):
				AQI = ""
			## end of URL
			# from URL2
			xml_data = result2.text.split('\n')[1:]
			data1 = xml_data[10]
			data2 = xml_data[12]
			weather = data1.split(': ')[1].split(', ')[0]
			windSpeed = data2.split('Wind Speed: ')[1].split('mph')[0] #unit mph
			windDirection = data2.split('Wind Direction: ')[1].split(', ')[0]
			if(weather == "null"):
				weather = ""
			if(windDirection == "null"):
				windDirection = ""
				windSpeed = ""
			## end of URL2
			entry = {"city":city_name ,"time":time, "Concentration":PM25, "AQI":AQI, "weather":weather, "windSpeed":windSpeed, "windDirection":windDirection}
			temp.append(entry)

			break

	log.debug(temp)
	return temp[0]

def get_date_name_file():
   log.debug("running get_date")
   weekday=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
   month=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
   tz = pytz.timezone('Asia/Shanghai')
   now = datetime.datetime.now(tz)
   #now=datetime.datetime.now()
   Crawl_date=weekday[now.weekday()]+'_'+month[now.month-1]+'_'+str(now.day)+'_'+str(now.hour)
   return  Crawl_date

args = getJobArgs()
citiesJSON = generateProjectPath(args['cities'])
cities = None
try:
    with open(citiesJSON, 'r') as f:
        cities = json.load(f)
except Exception as error:
    log.error(error)
    assert False, "Cities JSON missing."


cityOutput = []
for city in cities:
    cityOutput.append(air_parser(city['AQIURL'], city['weatherURL'], city['name']))

outPath = generateOutputPath('air_data.csv')
with open(outPath, 'a') as outfile:
	samplewriter = csv.writer(outfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
	if(os.path.isfile(outPath)): # new file
		samplewriter.writerow(['Time', 'City', 'Concentration', 'AQI', 'Weather', 'Wind_Speed', 'Wind_Direction'])
	for city_ele in cityOutput:
		samplewriter.writerow([city_ele['time'],city_ele['city'],city_ele['Concentration'],city_ele['AQI'],city_ele['weather'],city_ele['windSpeed'],city_ele['windDirection']])
	log.debug("job is done")












