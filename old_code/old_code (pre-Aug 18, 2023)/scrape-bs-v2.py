from tqdm import tqdm
import time
import os.path
import pandas as pd
import multiprocessing
import math
import psycopg2
from bs4 import BeautifulSoup
import requests

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


pages_df = pd.read_csv('pages.csv')

if len(pages_df) < 3000:
	pages_df = pd.DataFrame({'break_name': [], 'url': [], 'page_number': []})

	c = True
	while c:
		print('Page {}'.format(page_num) )
		URL = 'https://www.surf-forecast.com/breaks?page={}'.format(5)
		webpage = requests.get(URL, headers=HEADERS)
		soup = BeautifulSoup(webpage.content, "html.parser")
		breaks = soup.find_all('table', class_='list_table')[0].find_all('a')

		if len(breaks) == 0:
			c = False
			continue

		for surf_spot in breaks:
			pages_df = pd.concat([pages_df, pd.DataFrame({
				'break_name': [surf_spot.text]
				, 'url': [surf_spot['href']]
				, 'page_number': [int(page_num)]
			})], ignore_index=True)

def scrape(row):
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

	URL = row['url'] + '/forecasts/latest'
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
		cur_break_df = pd.concat([cur_break_df, pd.DataFrame({
			'break_name': [row['break_name']]
			, 'break_url': [row['url']]
			, 'time': [time.text]
			, 'rating': [rating.text]
			, 'period': [period.text]
			, 'wind': [wind.find('text').text]
			, 'wind_state': [wind_state.text]
			, 'wave_height': [wave_height.find('text').text]
			, 'wave_direction': [wave_height.find('div', class_='swell-icon__letters').text]
			, 'latitude': [latitude]
			, 'longitude': [longitude]
		})], ignore_index=True)    


		record_to_insert = (
			row['break_name']
			, row['url']
			, time.text
			, rating.text
			, period.text
			, wind.find('text').text
			, wind_state.text
			, wave_height.find('text').text
			, wave_height.find('div', class_='swell-icon__letters').text # Wave direction
			, latitude
			, longitude
		)

		records.append(record_to_insert)

	result = cursor.executemany(postgres_insert_query, records)
	connection.commit()



# for index, row in pages_df.iterrows():
#     i += 1
#     # If this were in production Remove code until HERE and timestamp each pull
#     if (df['break_name'] == row['break_name']).sum() > 40:
#         continue
#     print(row['break_name'])
#     # HERE

#     p = multiprocessing.Process(target=scrape, args=(row,))
#     p.start()
#     ulti.append(p)
	
#     if i > 10:
#         break

#     df = pd.concat([df, scrape(row)], ignore_index=True)



a = [, row in pages_df.iterrows()]
import pdb; pdb.set_trace()
# p = multiprocessing.Process(target=scrape, args=(row,))
# p.start()
# p.join()


# for index, row in tqdm(pages_df.iterrows()):
# 	# # start = time.time()
# 	# # scrape(row)

# 	# # end = time.time()
# 	# # print(end - start)

# cursor.close()


