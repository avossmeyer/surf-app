import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# display.set_matplotlib_formats("svg")
# plt.rcParams["axes.spines.top"] = False
# plt.rcParams["axes.spines.right"] = False

@st.cache_data
def pull_data():


    HEADERS = ({'User-Agent':
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
        'Accept-Language': 'en-US, en;q=0.5'})

    connection = psycopg2.connect(user=st.secrets["db_username"],
                                    password=st.secrets["db_password"],
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
    df['Rating'] = df['Rating'].astype(float)
    df['airport_lat'] = df['airport_lat'].astype(float)
    df['airport_lon'] = df['airport_lon'].astype(float)
    return df

def run(df):
    # import pdb; pdb.set_trace()

    # return df[['Airport', 'City', 'Rating', 'Ordering', 'Spot Dist', 'Spots', '% 5+',
    #        'avg_of_best_session_for_each_break', 'Best', 'max_10d_rating',
    #        'num_good_surf_spots', 'avg_dist', 'array_agg', 'Out Price',
    #        'Return Price']].fillna('')
    option = st.selectbox(
        "Home Airport",
        ("LAX"),
        # ("SAN", "LAX", "SFO"),
        index=None,
        placeholder="Select Your Home Airport...",
    )

    st.write(df[['City', 'Airport', 'Rating', 'Best', 'Outbound', 'Return', 'Breaks', 'Dist to Breaks']])

    # map_df = df.nlargest(15, 'Rating')
    # map_df = map_df[['airport_lat', 'airport_lon', 'Airport']]
    # map_df.columns = ['lat', 'lon', 'Airport']
    n_spots = 15
    n_spots = st.slider('Top N Airports', 0, df.shape[0], n_spots)

    # st.map(df.nlargest(n_spots, 'Rating'), latitude='airport_lat', longitude='airport_lon')
    n_spots_df = df.nlargest(n_spots, 'Rating')
    n_spots_df['Avg Dist Airport to Break'] = n_spots_df['Dist to Breaks']

    fig = px.scatter_mapbox(n_spots_df, lat=n_spots_df["airport_lat"], lon=n_spots_df["airport_lon"],
                            zoom=0, hover_name="City", hover_data={
            'Airport':True,
            'Rating': True,
            'Avg Dist Airport to Break': True,
            'airport_lat': False,
            'airport_lon': False,
        },
    )
    # df = px.data.carshare()
    # fig = px.scatter_mapbox(df, lat="centroid_lat", lon="centroid_lon", color="peak_hour", size="car_hours",
    #                         color_continuous_scale=px.colors.cyclical.IceFire, size_max=10, zoom=10)

    fig.update_layout(mapbox_style="open-street-map")
    # fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig)

    # st.write("I'm ", age, 'years old')


if __name__ == '__main__':
    df = pull_data()
    run(df)