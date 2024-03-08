old_app_query_flights = """

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
	    , airport_lat
	    , airport_lon
	    , round(avg(max_10d_rating), 1) as "Rating"
	    , round(avg(max_10d_rating), 1) + (case when count(*) > 50 then 1.5 when count(*) > 10 then 1 when count(5) > 5 then 0.25 when count(5) > 2 then -0.5 else -1.5 end) as "Ordering"
	    , round(avg(dist)::numeric, 0)::varchar || ' mi' as "Dist to Breaks"
	    , count(*) as "Breaks"
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
	group by 1,2,3,4
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
