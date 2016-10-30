#!/usr/bin/env python

import urllib2
import time
import os

import logging

from bdeep.context import generateOutputPath

log = logging.getLogger("BDEEP")

new_folder_path = generateOutputPath(time.strftime("%d-%m-%Y"))
new_file_name = generateOutputPath('air_data-' + time.strftime("%d-%m-%Y-%M") + '.csv')

if not os.path.exists(new_folder_path):
	os.makedirs(new_folder_path)
os.system('mv ' + generateOutputPath('air_data.csv') + ' ' + new_folder_path)


log.debug("finished job2 for weekly")

