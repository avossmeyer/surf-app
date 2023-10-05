from tqdm import tqdm
import time
import os.path
import pandas as pd
import multiprocessing
import math
import psycopg2
from bs4 import BeautifulSoup
import requests


# df = pd.read_csv('breaks.csv', ignore_index)

df = pd.DataFrame({
	'break_name': []
	, 'break_url': []
	, 'time': []
	, 'rating': []
	, 'period': []
	, 'wind': []
	, 'wind_state': []
	, 'wave_height': []
	, 'wave_direction': []
	, 'latitude': []
	, 'longitude': []
})


# pages_df = pd.read_csv('pages.csv')



def scrape(elem):
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
		wind_state, wave_height, wave_direction, lat_decimal, lon_decimal) 
		VALUES 
		(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
	"""

	# cursor = connection.cursor()
	records = []

	cur_break_df = pd.DataFrame({
			'break_name': []
			, 'break_url': []
			, 'time': []
			, 'rating': []
			, 'period': []
			, 'wind': []
			, 'wind_state': []
			, 'wave_height': []
			, 'latitude': []
			, 'longitude': []
		})

	URL = elem[2] + '/forecasts/latest'
	print(URL)

	webpage = requests.get(URL, headers=HEADERS)
	soup = BeautifulSoup(webpage.content, "html.parser")

	ratings = soup.find_all('tbody')[1].find_all('tr')[2].find_all('td')
	times = soup.find_all('tbody')[1].find_all('tr')[1].find_all('td')
	periods = soup.find_all('tbody')[1].find_all('tr')[5].find_all('td')    
	winds = soup.find_all('tbody')[1].find_all('tr')[8].find_all('td')
	wave_heights = soup.find_all('tbody')[1].find_all('tr')[4].find_all('td')
	wind_states = soup.find_all('tbody')[1].find_all('tr')[9].find_all('td')
	
	try:
		latitude = soup.find_all('span', class_='latitude')[0]['title']
		longitude = soup.find_all('span', class_='longitude')[0]['title']
	except:
		latitude = None 
		longitude = None

	for time, rating, period, wind, wind_state, wave_height in zip(
			times, ratings, periods, winds, wind_states, wave_heights):
		wh = wave_height.find('text').text
		if wh == '!':
			import pdb; pdb.set_trace()
			wh = '11'
		cur_break_df = pd.concat([cur_break_df, pd.DataFrame({
			'break_name': [elem[1]]
			, 'break_url': [elem[2]]
			, 'time': [time.text]
			, 'rating': [rating.text]
			, 'period': [period.text]
			, 'wind': [wind.find('text').text]
			, 'wind_state': [wind_state.text]
			, 'wave_height': [wh]
			, 'wave_direction': [wave_height.find('div', class_='swell-icon__letters').text]
			, 'latitude': [latitude]
			, 'longitude': [longitude]
		})], ignore_index=True)    


		record_to_insert = (
			elem[1]
			, elem[2]
			, time.text
			, rating.text
			, period.text
			, wind.find('text').text
			, wind_state.text
			, wh
			, wave_height.find('div', class_='swell-icon__letters').text # Wave direction
			, latitude
			, longitude
		)

		records.append(record_to_insert)

	result = cursor.executemany(postgres_insert_query, records)
	connection.commit()


if __name__ == '__main__':
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
	    """
	    SELECT * 
	    FROM pages;
	    -- WHERE u.userid = %s;
	    """,
	    # [someuserid,]
	)

	pages = cursor.fetchall()

	cursor.close()

	p = multiprocessing.Pool(48)                         # Create a multiprocessing Pool

	start = time.time()
	print('Using imap_unordered')
	for i in tqdm(p.imap_unordered(scrape, pages)):
	    print(i)
	print('Time elapsed: %s' % (time.time() - start))


