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
