import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import multiprocessing
import datetime
import time
import tqdm
import requests
import psycopg2
from queries import app_query, us_airports_query, create_flight_prices_table_query


def read_table_write_rows(date_cal, origin, dest):

	for table in date_cal:
		soup = BeautifulSoup(table.get_attribute('innerHTML')
							 , "html.parser")
		records = []
		for i in soup.find_all('td'):
			try:
				month = soup.find('span', {
					'class':'ui-datepicker-month'}).text
				
				date = datetime.datetime.strptime(
					i['data-year']+month+i.text.split()[0], '%Y%B%d')
			
				price = int(i.text.split()[1].replace(',', ''))

				last_date = date
				# print(origin, dest, date, price, current_date)
				record = (origin, dest, date, price)

				print(record)

				records.append(record)

			except Exception as error:
				if 'list index out of range' in str(error):
					# Greyed out date
					pass
				elif 'data-year' in str(error):
					# Within 3 days of today doesn't show price
					pass
				else:
					import pdb; pdb.set_trace()
					pass

		HEADERS = ({'User-Agent':
						'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                        (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36', \
					'Accept-Language': 'en-US, en;q=0.5'})

		connection = psycopg2.connect(user="avossmeyer",
									  password="surfbro1#",
									  host="surf-forecasts.c6bioghb9ybm.us-east-2.rds.amazonaws.com",
									  port="5432",
									  database="postgres")

		cursor = connection.cursor()

		postgres_insert_query = """ 
							INSERT INTO flight_prices 
							(origin, dest, flight_date, price, etl_insert_timestamp) 
							VALUES 
							(%s,%s,%s,%s,current_timestamp)
						"""

		result = cursor.executemany(postgres_insert_query, records)
		connection.commit()


# Selenium (requires browser)
def scrape_skiplagged(driver, origin='LAX', dest='DPS'):
	try:
		tom = datetime.datetime.today().date() + datetime.timedelta(days=1)
		URL = "https://skiplagged.com/flights/{}/{}/{}".format(
			origin,
			dest,
			tom.strftime("%Y-%m-%d"))
		driver.get(URL)

		time.sleep(5)
		# trip - cost

		date = driver.find_element(By.CLASS_NAME, "hasDatepicker")
		date.click()

		date_cal = driver.find_elements(By.CLASS_NAME, "ui-datepicker-group")
		read_table_write_rows(date_cal, origin, dest)

		nex = driver.find_elements(By.CLASS_NAME, "ui-icon-circle-triangle-e")
		nex[0].click()
		nex = driver.find_elements(By.CLASS_NAME, "ui-icon-circle-triangle-e")
		nex[0].click()

		date_cal = driver.find_elements(By.CLASS_NAME, "ui-datepicker-group")
		read_table_write_rows(date_cal, origin, dest)

	except Exception as error:
		print(error)


def scrape_skiplagged_bs(date, origin='LAX', dest='DPS'):
	HEADERS = ({'User-Agent':
					'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
					(KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36', \
				'Accept-Language': 'en-US, en;q=0.5'})

	# tom = datetime.datetime.today().date() + datetime.timedelta(days=1)
	URL = "https://skiplagged.com/flights/{}/{}/{}".format(
		origin, dest, date)

	webpage = requests.get(URL, headers=HEADERS)
	soup = BeautifulSoup(webpage.content, "html.parser")
	soup.find_all('div', class_='trip-cost')
	import pdb; pdb.set_trace()

	# .find_all('tr')[2].find_all('td')


# BeautifulSoup (doesn't require selenium)
def scrape_expedia(origin='LAX', dest='DPS'):
	HEADERS = ({'User-Agent':
		'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
		(KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
		'Accept-Language': 'en-US, en;q=0.5'})


	URL = 'https://www.expedia.com/Flights-Search?flight-type=on&mode=search&trip=oneway&leg1=from:({}),to:({}),departure:{}'+\
	'TANYT&options=cabinclass:economy&fromDate=9/3/2023&passengers=adults:1,infantinlap:N'.format(origin, dest, '9/3/2023')

	webpage = requests.get(URL, headers=HEADERS)
	soup = BeautifulSoup(webpage.content, "html.parser")

	prices = soup.find('ul', class_='uitk-date-range').find_all('tr')[2].find_all('td')


def scrape_one_param(od):
	scrape_skiplagged(od[0], od[1])


def scrape_fast(input_data, num_processes=48):
	p = multiprocessing.Pool(48)  # Create a multiprocessing Pool
	for i in tqdm(p.imap_unordered(scrape_one_param, input_data)):
		i




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
		result = cursor.execute(create_flight_prices_table_query)
		connection.commit()
	except Exception as error:
		print('Table Already exists')

	# from queries import best_iata_surf_query
	# cursor.execute(best_iata_surf_query)

	connection = psycopg2.connect(user="avossmeyer",
									password="surfbro1#",
									host="surf-forecasts.c6bioghb9ybm.us-east-2.rds.amazonaws.com",
									port="5432",
									database="postgres")

	cursor = connection.cursor()
	cursor.execute(app_query)
	app_data = cursor.fetchall()
	app_data_df = pd.DataFrame(app_data, columns=[desc[0] for desc in cursor.description])

	cursor.execute(us_airports_query)
	us_airports = cursor.fetchall()

	cursor.close()
	driver = webdriver.Chrome()

	# import pdb; pdb.set_trace()
	# airport_codes = []

	for i, cur in app_data_df.iterrows():
		print(cur['Airport'])
		destination_iata = cur['Airport']

		# for home_airport in us_airports:
		for home_airport in [['LAX', 'SAN', 'SFO', 'JFK', 'ORD', 'SYD', 'MEL', 'PER']]:
			home_airport = home_airport[0]

			# if destination_iata == 'FLN':
			# 	import pdb; pdb.set_trace()

			if pd.isnull(cur['etl_insert_timestamp_outbound']):
				print([home_airport, destination_iata])
				scrape_skiplagged(driver, home_airport, destination_iata)
			elif (datetime.datetime.utcnow() - cur['etl_insert_timestamp_outbound']).seconds / 3600 > 8:
				print([home_airport, destination_iata])
				scrape_skiplagged(driver, home_airport, destination_iata)
			else:
				# Do Nothing data fresh
				print('Skipping weve pulled this in the last 8 hrs')
				pass

			if pd.isnull(cur['etl_insert_timestamp_return']):
				scrape_skiplagged(driver, destination_iata, home_airport)
			# If we haven't pulled flight prices in 8 hours then pull
			elif (datetime.datetime.utcnow() - cur['etl_insert_timestamp_return']).seconds / 3600 > 8:
				scrape_skiplagged(driver, destination_iata, home_airport)
			else:
				# Do Nothing data fresh
				print('Skipping weve pulled this in the last 8 hrs')
				pass

	# flight_data.scrape_fast(airport_codes)



if __name__ == '__main__':
	print(5)
	scrape_top_surf_airports()
