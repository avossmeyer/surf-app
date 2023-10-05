# from queries import best_iata_surf


app_query = """

with flights_etl_order as (
	select 
		RANK () OVER (
			PARTITION BY origin, dest,flight_date
			ORDER BY etl_insert_timestamp ASC
		) etl_insert_order
		, *
	from flight_prices
--	where origin='LAX'
--	and dest='DUR'
)

 
, flights as (
	select 
		origin
		, dest
		, max(etl_insert_timestamp) as etl_insert_timestamp
		, max(etl_insert_order)
		, min(price) as min_2mo_price
		, min(case when EXTRACT(epoch from flight_date::timestamp - current_date)/3600/24 <= 14 then price else null end) as min_2wk_price
	from flights_etl_order
	where etl_insert_order = 1
	group by 1,2
)

, breaks as (
	select                 
	    iata_code as "Airport"
	    , INITCAP(city) || ', ' || INITCAP(country) as "City"
	    , round(avg(max_10d_rating), 1) as "Rating"
	    , round(avg(max_10d_rating), 1) + (case when count(*) > 50 then 1.5 when count(*) > 10 then 1 when count(5) > 5 then 0.25 when count(5) > 2 then -0.5 else -1.5 end) as "Ordering"
	    , round(avg(dist)::numeric, 0)::varchar || ' mi' as "Avg Dist"
	    , count(*) as "Spots"
	    , round(avg(perc_5_plus)*100, 1)::varchar as "% 5+"
	    
--	    , sum(sum_10d_rating) / sum(n) as avg_10d_rating
        , max(avg_10d_rating) as avg_of_best_session_for_each_break
        , max(max_10d_rating) as "Best"
        , max(max_10d_rating) as max_10d_rating
--        , sqrt(sum(rating_variance) / sum(n)) as rating_stddev
        , count(*) as num_good_surf_spots
        , avg(acb.dist) as avg_dist
        , array_agg((break_name, acb.dist))

	    --, array_agg((break_name, b.dist))
	    --, array_agg(break_name) as Breaks
	from airports_closest_breaks acb
	where airport_size = 'large_airport'
	and dist < 100
	and (not iata_code in ('VBG', 'DNA'))
	group by 1,2
	order by "Ordering" desc
)


SELECT
    b.*
--    , outbound_flight.min_2mo_price as "2 Month Low Price"
    , to_char(outbound_flight.min_2wk_price::numeric, '$FM999,999') as "Outbound"
    , to_char(return_flight.min_2wk_price::numeric, '$FM999,999') as "Return"
    , outbound_flight.etl_insert_timestamp as etl_insert_timestamp_outbound
    , return_flight.etl_insert_timestamp as etl_insert_timestamp_return

--    , outbound_flight.*
FROM breaks b
left join flights outbound_flight
	on b."Airport" = outbound_flight.dest
	and outbound_flight.origin = '{}'
left join flights return_flight
	on b."Airport" = return_flight.origin
	and return_flight.dest = '{}'	
--where num_good_surf_spots > 3

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
			ORDER BY etl_insert_timestamp ASC
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
	, variance(rating)*count(*) as rating_variance
	, count(*) as n
	, avg(case when rating >= 5 then 1 else 0 end) as perc_5_plus
from surf_rankings_etl_order sr
where lat_decimal is not null and rating <= 10
	and etl_insert_order = 1
group by 1,2,3
)

--, B as (
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


us_airports_query = """
select iata_code from airport_size as2 	
where type = 'large_airport'
and iso_country = 'US'
"""