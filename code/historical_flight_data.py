import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import multiprocessing
import datetime
import time
import tqdm
import requests
import psycopg2
from queries import app_query, us_airports_query, create_fare_detective_table_query

f = open("password.txt", "r")
password = f.read()

def read_table_write_rows(date_cal, origin, dest):
    for table in date_cal:
        soup = BeautifulSoup(table.get_attribute('innerHTML')
                             , "html.parser")
        records = []
        for i in soup.find_all('td'):
            try:
                month = soup.find('span', {
                    'class': 'ui-datepicker-month'}).text

                date = datetime.datetime.strptime(
                    i['data-year'] + month + i.text.split()[0], '%Y%B%d')

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
                    import pdb;
                    pdb.set_trace()
                    pass

        HEADERS = ({'User-Agent':
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                        (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36', \
                    'Accept-Language': 'en-US, en;q=0.5'})

        connection = psycopg2.connect(user="avossmeyer",
                                      password=password,
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


def scrape_faredetective(driver, origin='LAX', dest='DPS'):
    try:
        URL = "https://www.faredetective.com/farehistory"
        driver.get(URL)

        elem = driver.find_element(By.ID, "fromAir")
        elem.send_keys(origin)

        elem = driver.find_element(By.ID, "toAir")
        elem.send_keys(dest)

        btn = driver.find_element(By.CLASS_NAME, 'btn-fare')
        btn.click()

        historical_prices = driver.find_elements(By.TAG_NAME, 'table')[0].find_elements(By.TAG_NAME, 'td')

        print(driver.current_url)

        lowest_historical_price = historical_prices[0].text
        avg_historical_price = historical_prices[1].text
        cheapest_month = historical_prices[2].text

        print(lowest_historical_price)
        print(avg_historical_price)
        print(cheapest_month)

        record = (origin, dest, lowest_historical_price, avg_historical_price, cheapest_month)

        connection = psycopg2.connect(user="avossmeyer",
                                      password=password,
                                      host="surf-forecasts.c6bioghb9ybm.us-east-2.rds.amazonaws.com",
                                      port="5432",
                                      database="postgres")

        cursor = connection.cursor()

        postgres_insert_query = """ 
        							INSERT INTO fare_detective 
        							(origin, dest, lowest_historical_price, avg_historical_price, cheapest_month, etl_insert_timestamp) 
        							VALUES 
        							(%s,%s,%s,%s,%s,current_timestamp)
        						"""

        result = cursor.execute(postgres_insert_query, record)
        connection.commit()

    except Exception as error:
        print(error)

    # url = 'https://www.faredetective.com/farehistory'


def scrape_top_surf_airports():
    HEADERS = ({'User-Agent':
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                    (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36', \
                'Accept-Language': 'en-US, en;q=0.5'})

    connection = psycopg2.connect(user="avossmeyer",
                                  password=password,
                                  host="surf-forecasts.c6bioghb9ybm.us-east-2.rds.amazonaws.com",
                                  port="5432",
                                  database="postgres")

    cursor = connection.cursor()

    try:
        result = cursor.execute(create_fare_detective_table_query)
        connection.commit()
    except Exception as error:
        print('Table Already exists')

    # from queries import best_iata_surf_query
    # cursor.execute(best_iata_surf_query)

    connection = psycopg2.connect(user="avossmeyer",
                                  password=password,
                                  host="surf-forecasts.c6bioghb9ybm.us-east-2.rds.amazonaws.com",
                                  port="5432",
                                  database="postgres")

    cursor = connection.cursor()
    cursor.execute(app_query)
    app_data = cursor.fetchall()
    app_data_df = pd.DataFrame(app_data, columns=[desc[0] for desc in cursor.description])
    print(app_data_df)

    cursor.close()

    options = Options()
    options.add_argument('--headless=new')
    # options.add_argument('--disable-gpu')  # Last I checked this was necessary.

    driver = webdriver.Chrome(options=options)

    # import pdb; pdb.set_trace()
    # airport_codes = []

    for i, cur in app_data_df.iterrows():
        print(cur['Airport'])
        destination_iata = cur['Airport']

        # for home_airport in us_airports:
        for home_airport in [['LAX', 'SAN', 'SFO', 'JFK', 'ORD', 'SYD', 'MEL', 'PER', 'AKL']]:
            home_airport = home_airport[0]

            # if destination_iata == 'FLN':
            # 	import pdb; pdb.set_trace()

            if pd.isnull(cur['etl_insert_timestamp_outbound']):
                print([home_airport, destination_iata])
                scrape_faredetective(driver, home_airport,  )
            elif (datetime.datetime.utcnow() - cur['etl_insert_timestamp_outbound']).seconds / 3600 > 168:
                print([home_airport, destination_iata])
                scrape_faredetective(driver, home_airport, destination_iata)
            else:
                # Do Nothing data fresh
                print('Skipping weve pulled this in the last 8 hrs')
                pass

            if pd.isnull(cur['etl_insert_timestamp_return']):
                scrape_faredetective(driver, destination_iata, home_airport)
            # If we haven't pulled flight prices in 8 hours then pull
            elif (datetime.datetime.utcnow() - cur['etl_insert_timestamp_return']).seconds / 3600 > 168:
                scrape_faredetective(driver, destination_iata, home_airport)
            else:
                # Do Nothing data fresh
                print('Skipping we\'ve pulled this in the last 1 week')
                pass


# flight_data.scrape_fast(airport_codes)


if __name__ == '__main__':
    print(5)
    scrape_top_surf_airports()
