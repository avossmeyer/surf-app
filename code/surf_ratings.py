import traceback
from tqdm import tqdm
import time
import os.path
import pandas as pd
import multiprocessing
import math
import psycopg2
from bs4 import BeautifulSoup
import requests



def scrape(elem):
	try:	
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

		postgres_insert_query = """ INSERT INTO surf_ratings 
			(break_name, break_url, time, rating, period, wind, 
			wind_state, wave_height, wave_direction, lat_decimal, lon_decimal, 
			ith_forecast, country, etl_insert_timestamp) 
			VALUES 
			(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,current_timestamp)
		"""

		# cursor = connection.cursor()
		records = []

		URL = elem[2] + '/forecasts/latest'

		webpage = requests.get(URL, headers=HEADERS)
		soup = BeautifulSoup(webpage.content, "html.parser")

		ratings = soup.find('tbody', class_='forecast-table__basic').find_all('tr')[2].find_all('td')
		times = soup.find('tbody', class_='forecast-table__basic').find_all('tr')[1].find_all('td')
		periods = soup.find('tbody', class_='forecast-table__basic').find_all('tr')[5].find_all('td')    
		winds = soup.find('tbody', class_='forecast-table__basic').find_all('tr')[8].find_all('td')
		wave_heights = soup.find('tbody', class_='forecast-table__basic').find_all('tr')[4].find_all('td')
		wind_states = soup.find('tbody', class_='forecast-table__basic').find_all('tr')[9].find_all('td')
		
		try:
			latitude = soup.find_all('span', class_='latitude')[0]['title']
			longitude = soup.find_all('span', class_='longitude')[0]['title']
		except:
			latitude = None 
			longitude = None

		try:
			country = soup.find_all('div', {"id": "breadcrumbs"})[0]
			country = country.find_all('a')[-1].text
		except:
			country = None

		for i, (time, rating, period, wind, wind_state, wave_height) in enumerate(zip(
				times, ratings, periods, winds, wind_states, wave_heights)):
			

			try:
				r = rating.text
				if r == '!':
					r = '11'
			except:
				cur_rating = None

			try: 
				t = time.text
			except:
				t = None

			try:
				wh = float(wave_height.find('text').text)
			except:
				wh = None

			try:
				wd = wave_height.find('div', class_='swell-icon__letters').text
			except:
				wd = None


			try:
				p = int(period.text)
			except:
				p = None


			try:
				w = int(wind.find('text').text)
			except:
				w = None


			try:
				ws = wind_state.text
			except:
				ws = None


			record_to_insert = (
				elem[1]
				, elem[2]
				, t # time
				, r # rating
				, p # period
				, w # wind
				, ws # wind state
				, wh # wind height
				, wd # Wave direction
				, latitude
				, longitude
				, i
				, country
			)

			records.append(record_to_insert)

		result = cursor.executemany(postgres_insert_query, records)
		connection.commit()
	except Exception as error:
		# handle the exception
		print(elem[0],  '| URL: ', elem[2], ' | Break: ', elem[1])
		print("An exception occurred:", error) 
		traceback.print_exc()
		# import pdb; pdb.set_trace()




# def scrape_with_try(elem):
# 	try:	
# 		scrape(elem)
# 	except Exception as error:
# 		# handle the exception
# 		print(elem[0],  ') URL: ', elem[1])
# 		print("An exception occurred:", error) 
# 		traceback.print_exc()

def create_surf_ratings_table():
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
		from queries import create_surf_ratings_table_query
		result = cursor.execute(create_surf_ratings_table_query)
		cursor.close()
		connection.commit()
	except Exception as error:
		print('Table Already exists')
		print(error)


# airports_closest_breaks table
def create_acb_materialized_view():
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
		from queries import create_acb_materialized_view_query
		result = cursor.execute(create_acb_materialized_view_query)
		cursor.close()
		connection.commit()
	except Exception as error:
		print(error)
		print('Error with Materialized View')




def scrape_fast(input_data, num_processes=48):
	p = multiprocessing.Pool(num_processes)  # Create a multiprocessing Pool
	create_surf_ratings_table()

	for i in tqdm(p.imap_unordered(scrape, input_data)):
		i


def get_pages():
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
	cursor.execute(
		"""SELECT * FROM pages; -- WHERE u.userid = %s; """, # [someuserid,]
	)

	pages = cursor.fetchall()
	cursor.close()

	return pages




if __name__ == '__main__':
	create_surf_ratings_table()
	pages = get_pages()
	scrape_fast(pages)
	create_acb_materialized_view()


	# 25, 35, 49
	# 91



