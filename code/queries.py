# from queries import best_iata_surf


app_query = app_query = """
select                 
	iata_code as "Airport"
	, INITCAP(city) || ', ' || INITCAP(country) as "City"
	, airport_lat
	, airport_lon
 
	, round(avg(max_10d_rating), 1) as "Rating"
	, round(avg(max_10d_rating), 1) + (case when count(*) > 50 then 1.5 when count(*) > 10 then 1 when count(5) > 5 then 0.25 when count(5) > 2 then -0.5 else -1.5 end) as "Ordering"
	, round(avg(dist)::numeric, 0)::varchar || ' mi' as "Dist to Breaks"
	, count(*) as "Breaks"
	, round(avg(perc_5_plus)*100, 1)::varchar as "% 5+"
	, max(avg_10d_rating) as avg_of_best_session_for_each_break
	, max(max_10d_rating) as "Best"
	, max(max_10d_rating) as max_10d_rating
 
 	, round(avg(avg_wave_height)::numeric, 1) as "Average Height Spot"
 	, round(min(avg_wave_height)::numeric, 1) as "Min Height Spot"
  	, round(max(avg_wave_height)::numeric, 1) as "Max Height Spot"
   
	, count(*) as num_good_surf_spots
	, avg(acb.dist) as avg_dist
	, array_agg((break_name, acb.dist))

-- , sqrt(sum(rating_variance) / sum(n)) as rating_stddev
-- , sum(sum_10d_rating) / sum(n) as avg_10d_rating
-- , array_agg((break_name, b.dist))
-- , array_agg(break_name) as Breaks
from airports_closest_breaks acb
where airport_size = 'large_airport'
and dist < 100
and (not iata_code in ('VBG', 'DNA'))
group by 1,2,3,4
order by "Ordering" desc
"""



create_acb_materialized_view_query = """
drop materialized view airports_closest_breaks;

CREATE MATERIALIZED VIEW airports_closest_breaks
AS
with airport as (
	SELECT 
		a.iata_code
		, lat_decimal 
		, lon_decimal 
		, type as airport_size
		, name,country,city,municipality
	FROM airports a 
	left join airport_size asize
		on upper(asize.iata_code) = upper(a.iata_code)
--	where type ='large_airport'
--	ORDER BY distance;
)

, surf_rankings_etl_order as (
	select 
		RANK () OVER (
			PARTITION BY break_name, ith_forecast
			ORDER BY etl_insert_timestamp DESC
		) etl_insert_order
		, *
	from surf_ratings
--	where origin='LAX'
--	and dest='DUR'
)


, good_surf as (
select 
	break_name
	, lat_decimal
	, lon_decimal
	, max(rating) as max_10d_rating
	, avg(rating) as avg_10d_rating
	, sum(rating) as sum_10d_rating
 	, avg(wave_height) as avg_wave_height
	, variance(rating)*count(*) as rating_variance
	, count(*) as n
	, avg(case when rating >= 5 then 1 else 0 end) as perc_5_plus
from surf_rankings_etl_order sr
where lat_decimal is not null and rating <= 10
	and etl_insert_order = 1
group by 1,2,3
)

select 
	iata_code
	, airport_size
	, A.name
	, A.country
	, A.city
	, A.municipality
	, avg_10d_rating
	, perc_5_plus
	, max_10d_rating
 	, avg_wave_height
	, A.dist
	, break_name
	, A.lat_decimal as airport_lat
	, A.lon_decimal as airport_lon
	, good_surf.lat_decimal as break_lat
	, good_surf.lon_decimal as break_lon
from good_surf
JOIN LATERAL (
	SELECT 
		point(A.lat_decimal, A.lon_decimal)::point <@>  (point(good_surf.lat_decimal, good_surf.lon_decimal)::point) as dist
		, A.*
	FROM airport A
	ORDER BY point(A.lat_decimal, A.lon_decimal)::point <@>  (point(good_surf.lat_decimal, good_surf.lon_decimal)::point)
	limit 1
) AS A
on true
--where A.dist < 75
WITH NO data;

REFRESH MATERIALIZED VIEW airports_closest_breaks ;
"""


create_surf_ratings_table_query = """
-- drop table surf_ratings;
-- ALTER TABLE surf_ratings RENAME TO surf_ratings_old2;
CREATE TABLE surf_ratings
  (
     break_name     VARCHAR(50),
     break_url      VARCHAR(100),
     time           VARCHAR(15),
     rating         INT,
     period         INT,
     wind           INT,
     wind_state     VARCHAR(20),
     wave_height    DOUBLE PRECISION,
     wave_direction VARCHAR(20),
     --   latitude     int               ,
     --   latitude     char   ( 1)       ,
     lat_decimal    DOUBLE PRECISION,
     lon_decimal    DOUBLE PRECISION,
     etl_insert_timestamp    TIMESTAMP,
     ith_forecast   INT,
     country        VARCHAR(25),
     id             SERIAL PRIMARY KEY
  ); 
"""



us_airports_query = """
select iata_code from airport_size as2 	
where type = 'large_airport'
and iso_country = 'US'
"""
















create_fare_detective_table_query = """
 DROP TABLE fare_detective; 

    CREATE TABLE fare_detective
    (
        origin                          VARCHAR(5),
        dest                            VARCHAR(5),
        lowest_historical_price         VARCHAR(10),
        avg_historical_price            VARCHAR(10),
        cheapest_month                  VARCHAR(20),
        etl_insert_timestamp            TIMESTAMP
    ); 
"""




create_flight_prices_table_query = """
-- DROP TABLE flight_prices; 

    CREATE TABLE flight_prices
    (
        origin        VARCHAR(5),
        dest          VARCHAR(5),
        flight_date   DATE,
        price         DOUBLE PRECISION,
        etl_insert_timestamp   TIMESTAMP
    ); 
"""

