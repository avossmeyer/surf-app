import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# display.set_matplotlib_formats("svg")
# plt.rcParams["axes.spines.top"] = False
# plt.rcParams["axes.spines.right"] = False

@st.cache_data
def pull_data():
    
    from queries import app_query

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
    cursor.execute(app_query.format('LAX', 'LAX'))
    airports_data = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(airports_data, columns=[desc[0] for desc in cursor.description])
    df['Rating'] = df['Rating'].astype(float)
    df['airport_lat'] = df['airport_lat'].astype(float)
    df['airport_lon'] = df['airport_lon'].astype(float)
    
    return df

def run(df):
    df.index += 1 

    st.title("Best Surf Worldwide (Where to Fly)")
    
    x = df[['City', 'Airport', 'Rating', 'Best', 'Breaks', 'Dist to Breaks']]
    x.columns = ['City', 'Airport', 'Average Rating', 'Best Rating', 'Number of Breaks', 'Distance to Breaks']
    
    st.write(x)

    n_spots = 15
    n_spots = st.slider('How many airports do you want to show?', 0, df.shape[0], n_spots)

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

    fig.update_layout(mapbox_style="open-street-map")
    st.plotly_chart(fig)

if __name__ == '__main__':
    df = pull_data()
    run(df)
