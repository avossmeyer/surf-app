import traceback
from tqdm import tqdm
import time
import os.path
import pandas as pd
import multiprocessing
import math
from datetime import datetime, timezone
import psycopg2
from selenium import webdriver
from bs4 import BeautifulSoup
import requests

import flight_data


def scrape_top_surf_airports():

	HEADERS = ({'User-Agent':
		'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
		(KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
		'Accept-Language': 'en-US, en;q=0.5'})

	connection = psycopg2.connect(user="avossmeyer",
									password="surfbro1#",
									host="surf-forecasts.c6bioghb9ybm.us-east-2.rds.amazonaws.com",
									port="5432",
									database="postgres")

	cursor = connection.cursor()

	try:
		from queries import create_flight_prices_table_query
		result = cursor.execute(create_flight_prices_table_query)
		connection.commit()
	except Exception as error:
		print('Table Already exists')

	# from queries import best_iata_surf_query
	# cursor.execute(best_iata_surf_query)
	from queries import app_query

	connection = psycopg2.connect(user="avossmeyer",
									password="surfbro1#",
									host="surf-forecasts.c6bioghb9ybm.us-east-2.rds.amazonaws.com",
									port="5432",
									database="postgres")

	cursor = connection.cursor()

	cursor.execute(app_query)

	airports_data = cursor.fetchall()
	cursor.close()
	driver = webdriver.Chrome()

	# import pdb; pdb.set_trace()
	#
	# airport_codes = []
	for a in airports_data:
		iata = a[0]
		rating = a[2]
		etl_time = a[9]
		min_2mo_price = a[10]
		min_2wk_price = a[11]

		# (datetime.utcnow() - airports_data[0][9]).seconds/3600
		if rating > 2: # this is using average surf forecast over a 10 day period
			if etl_time is None:
				print(['LAX', iata])

				flight_data.scrape_skiplagged(driver, 'LAX', iata)
				flight_data.scrape_skiplagged(driver, iata, 'LAX')

			# If we haven't pulled flight prices in 8 hours then pull
			elif (datetime.utcnow() - airports_data[0][9]).seconds/3600 > 8:
				print(['LAX', iata])

				flight_data.scrape_skiplagged(driver, 'LAX', iata)
				flight_data.scrape_skiplagged(driver, iata, 'LAX')
			else:
				# Do Nothing data fresh
				print('Skipping weve pulled this in the last 8 hrs')
				pass

	# flight_data.scrape_fast(airport_codes)



if __name__ == '__main__':
	scrape_top_surf_airports()
