import streamlit as st

import matplotlib.pyplot as plt
import numpy as np
from shiny import App, render, ui
import psycopg2
from IPython import display
import pandas as pd

# display.set_matplotlib_formats("svg")
# plt.rcParams["axes.spines.top"] = False
# plt.rcParams["axes.spines.right"] = False


def server():

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
    from queries import app_query
    # x = input.input_box()
    # print(x)
    cursor.execute(app_query.format('LAX', 'LAX'))

    airports_data = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(airports_data, columns=[desc[0] for desc in cursor.description])
    # import pdb; pdb.set_trace()

    # return df[['Airport', 'City', 'Rating', 'Ordering', 'Spot Dist', 'Spots', '% 5+',
    #        'avg_of_best_session_for_each_break', 'Best', 'max_10d_rating',
    #        'num_good_surf_spots', 'avg_dist', 'array_agg', 'Out Price',
    #        'Return Price']].fillna('')

    st.write(df[['City', 'Airport', 'Rating', 'Best', 'Outbound', 'Return', 'Spots', 'Avg Dist']])


if __name__ == '__main__':
    server()