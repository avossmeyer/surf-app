import os.path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import math
import multiprocessing
import threading


pages_df = pd.read_csv('pages.csv')
# df = pd.read_csv('breaks.csv', ignore_index)
max_page = 0

if len(pages_df) < 3000:
	pages_df = pd.DataFrame({'break_name': [], 'url': [], 'page_number': []})
	
# if len(pages_df) == 0:
df = pd.DataFrame({
	'break_name': []
	, 'break_url': []
	, 'time': []
	, 'rating': []
	, 'period': []
	, 'wind': []
	, 'wind_state': []
	, 'wave_heights': []
	, 'latitude': []
	, 'longitude': []
})

# driver.set_window_position(1000,2000)
# ChromeOptions = webdriver.ChromeOptions()
# ChromeOptions.add_argument('--disable-browser-side-navigation')
# chrome_options=ChromeOptions


try: max_page 
except: 
	driver = webdriver.Chrome()
	driver.get('https://www.surf-forecast.com/breaks/')
	breaks = driver.find_elements(By.CLASS_NAME, 'list_table')

	for i in driver.find_element(By.CLASS_NAME, 'pagination').find_elements(By.TAG_NAME, 'a'):
		try:
			cur_page = int(i.text)
			if cur_page > max_page:
				max_page = cur_page
		except:
			continue
	driver.close()
		
if max_page > pages_df['page_number'].max() or math.isnan(pages_df['page_number'].max()):
	try: start_page = int(pages_df.page_number.max()) - 1 
	except: start_page = 0; print('No Pages Yet')
		
	for page_num in range(start_page, max_page + 1):
		print('Page {}'.format(page_num) )
		driver = webdriver.Chrome()
		driver.get('https://www.surf-forecast.com/breaks?page={}'.format(page_num))
		driver.find_elements(By.CLASS_NAME, 'pagination') 
		breaks = driver.find_elements(By.CLASS_NAME, 'list_table')

		for surf_spot in breaks[0].find_elements(By.TAG_NAME, 'a'):
			pages_df = pd.concat([pages_df, pd.DataFrame({
				'break_name': [surf_spot.text]
				, 'url': [surf_spot.get_attribute('href')]
				, 'page_number': [int(page_num)]
			})], ignore_index=True)
		driver.close
		
def scrape(row):
	cur_break_df = pd.DataFrame({
			'break_name': []
			, 'break_url': []
			, 'time': []
			, 'rating': []
			, 'period': []
			, 'wind': []
			, 'wind_state': []
			, 'wave_heights': []
			, 'latitude': []
			, 'longitude': []
		})

	driver = webdriver.Chrome()
	driver.get(row['url'] + '/forecasts/latest')
	print('----')

	# ratings_xpath = '/html/body/div[2]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div[2]/div/div[3]/div/table/tbody/tr[2]'
	# ratings = driver.find_elements(By.XPATH, ratings_xpath)[0].find_elements(By.TAG_NAME, 'td')

	ratings = driver.find_elements(By.CLASS_NAME, 'star-rating__rating')
	
	times_xpath = '/html/body/div[2]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div[2]/div/div[3]/div/table/tbody/tr[2]'
	times = driver.find_elements(By.XPATH, times_xpath)[0].find_elements(By.TAG_NAME, 'td')
	
	period_xpath = '/html/body/div[2]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div[2]/div/div[3]/div/table/tbody/tr[6]'
	periods = driver.find_elements(By.XPATH, period_xpath)[0].find_elements(By.TAG_NAME, 'td')

	wind_xpath = '/html/body/div[2]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div[2]/div/div[3]/div/table/tbody/tr[9]'
	winds = driver.find_elements(By.XPATH, wind_xpath)[0].find_elements(By.TAG_NAME, 'text')

	wave_height_xpath = '/html/body/div[2]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div[2]/div/div[3]/div/table/tbody/tr[5]'
	wave_heights = driver.find_elements(By.XPATH, wave_height_xpath)[0].find_elements(By.TAG_NAME, 'text')

	wind_state_xpath = '/html/body/div[2]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div[2]/div/div[3]/div/table/tbody/tr[10]'
	wind_states = driver.find_elements(By.XPATH, wind_state_xpath)[0].find_elements(By.TAG_NAME, 'td')

	try:
		latitude = driver.find_elements(By.CLASS_NAME, 'latitude')[0].get_attribute("title")
		longitude = driver.find_elements(By.CLASS_NAME, 'longitude')[0].get_attribute("title")
	except:
		latitude = None 
		longitude = None


	for time, rating, period, wind, wind_state, wave_height in zip(times, ratings, periods, winds, wind_states, wave_heights):        
		cur_break_df = pd.concat([cur_break_df, pd.DataFrame({
			'break_name': [row['break_name']]
			, 'break_url': [row['url']]
			, 'time': [time.text]
			, 'rating': [rating.text]
			, 'period': [period.text]
			, 'wind': [wind.text]
			, 'wind_state': [wind_state.text]
			, 'wave_heights': [wave_height.text]
			, 'latitude': [latitude]
			, 'longitude': [longitude]
		})], ignore_index=True)    

	driver.close()
	print(cur_break_df)
	return cur_break_df

for index, row in pages_df.iterrows():
	# If this were in production Remove code until HERE and timestamp each pull
	if (df['break_name'] == row['break_name']).sum() > 40:
		continue
	print(row['break_name'])
	# HERE

	# df = pd.concat([df, scrape(row)], ignore_index=True)

	# print(scrape(row))
	data = scrape(row)

	csv_name = 'breaks_temp.csv'
	if not os.path.isfile(csv_name):
		data.to_csv(csv_name)
	else:
		data.to_csv(csv_name, mode='a', index=True, header=False)


# for index, row in pages_df.iterrows():
#     i += 1
#     # If this were in production Remove code until HERE and timestamp each pull
#     if (df['break_name'] == row['break_name']).sum() > 40:
#         continue
#     print(row['break_name'])
#     # HERE

#     p = multiprocessing.Process(target=scrape, args=(row,))
#     p.start()
#     processes.append(p)
	
#     if i > 10:
#         break

#     df = pd.concat([df, scrape(row)], ignore_index=True)
