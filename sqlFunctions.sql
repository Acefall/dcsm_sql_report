-- PARAMETRIZED VIEWS
-- Creates a functions that returns the data of a given hobo from a given term
create or replace function hourly_temp (p_device_id character varying, p_term_id int) returns table (
		device_id character varying,
		tstamp timestamp without time zone,
		value real
	) language plpgsql as $$ begin return query
SELECT p_device_id,
	d.tstamp,
	d.value
from data d
	JOIN metadata md ON d.meta_id = md.id
WHERE md.term_id = p_term_id
	AND md.device_id = p_device_id
	AND d.variable_id = 1;
end;
$$

-- Creates a function that returns the data from a given term
create or replace function hourly_temp (p_term_id int) returns table (
		device_id character varying,
		tstamp timestamp without time zone,
		value real
	) language plpgsql as $$ begin return query
SELECT p_device_id,
	d.tstamp,
	d.value
from data d
	JOIN metadata md ON d.meta_id = md.id
WHERE md.term_id = p_term_id
	AND d.variable_id = 1;
end;
$$ 

-- HELPERS
-- Creates a function that decides whether a given timestamp is during day time
create or replace function is_day (p_timestamp timestamp without time zone) returns bool language plpgsql as $$ begin return date_part('hour', tstamp) >= 6
	AND date_part('hour', tstamp) <= 17;
end;
$$ 

-- Creates a funciton that is the negated result of is_day
create or replace function is_night (p_timestamp timestamp without time zone) returns bool language plpgsql as $$ begin return NOT is_day(p_timestamp);
end;
$$ 

-- Creates a function that calculates the pair of closest devices from two given terms
create or replace function closest_device (p_term_id_1 int, p_term_id_2 int) returns table (
		device_id_1 character varying,
		device_id_2 character varying
	) language plpgsql as $$ begin return query
SELECT term_1_device,
	term_2_device
FROM (
		SELECT t.term_1_device,
			t.term_2_device,
			row_number() OVER (PARTITION BY t.term_1_device) distance_rank,
			t.distance
		FROM(
				SELECT md.device_id AS term_1_device,
					md.location AS term_1_device_location,
					term_2.device_id AS term_2_device,
					term_2.location AS term_2_device_location,
					ST_Distance(
						ST_Transform(md.location, 25832),
						ST_Transform(term_2.location, 25832)
					) as distance
				FROM metadata md
					JOIN (
						SELECT device_id,
							location,
							term_id
						FROM metadata
						WHERE term_id = p_term_id_2
					) term_2 ON 1 = 1
				WHERE md.term_id = p_term_id_1
				ORDER BY term_1_device,
					distance ASC
			) t
	) q
WHERE q.distance_rank = 1;
end;
$$ 

-- Creates a function that normalizes the first moment of a given device and term id to zero
create or replace function normalize_first_moment_to_zero (p_device_id character varying, p_term_id int) returns table (
		device_id character varying,
		tstamp timestamp without time zone,
		value real
	) language plpgsql as $$ begin return query
SELECT p_device_id,
	ht.tstamp,
	ht.value - t_avg(p_device_id, p_term_id)
FROM hourly_temp(p_device_id, p_term_id) ht;
end;
$$ -- Creates a function that computes the correlation coefficient of two normalized time series identified by device_id and term_id
create or replace function corr_two_devices (
		p_device_id_1 character varying,
		p_term_id_1 int,
		p_device_id_2 character varying,
		p_term_id_2 int
	) returns real language plpgsql as $$ begin return corr(norm1.value, norm2.value)
FROM normalize_first_moment_to_zero(p_device_id_1, p_term_id_1) norm1
	JOIN normalize_first_moment_to_zero(p_device_id_2, p_term_id_2) norm2 ON (
		date_part('month', norm1.tstamp) = date_part('month', norm2.tstamp)
		AND date_part('day', norm1.tstamp) = date_part('day', norm2.tstamp)
		AND date_part('hour', norm1.tstamp) = date_part('hour', norm2.tstamp)
	);
end;
$$ 

-- Creates a function that computes the correlation coefficient to the closest device from the specified term of a specified device and term id
create or replace function corr_closest_device (
		p_device_id character varying,
		p_term_id_1 int,
		p_term_id_2 int
	) returns real language plpgsql as $$ begin return corr_two_devices(
		p_device_id,
		p_term_id_1,
		(
			SELECT device_id_2
			FROM closest_device(p_term_id_1, p_term_id_2)
			WHERE device_id_1 = p_device_id
		),
		p_term_id_2
	);
end;
$$ 

-- INDICES
-- Creates a function that computes the average temperature of a given device id and term id
create or replace function t_avg (p_device_id character varying, p_term_id int) returns real language plpgsql as $$ begin return avg(value)
FROM hourly_temp(p_device_id, p_term_id);
end;
$$ 

-- Creates a function that computes the average temperature of a term id for all devices
create or replace function t_avg (p_term_id int) returns table (device_id character varying, avg real) language plpgsql as $$ begin return query
SELECT ht.device_id,
	avg(value)::real
FROM hourly_temp(p_term_id) ht
GROUP BY ht.device_id;
end;
$$ 

-- Creates a function that computes the average day time temperature of a given device id and term id
create or replace function t_d (p_device_id character varying, p_term_id int) returns real language plpgsql as $$ begin return avg(value)
FROM hourly_temp(p_device_id, p_term_id)
WHERE is_day(tstamp);
end;
$$ 

-- Creates a function that computes the average day time temperature of a term id for all devices
create or replace function t_d (p_term_id int) returns table (device_id character varying, avg real) language plpgsql as $$ begin return query
SELECT ht.device_id,
	avg(value)::real
FROM hourly_temp(p_term_id) ht
WHERE is_day(ht.tstamp)
GROUP BY ht.device_id;
end;
$$ 

-- Creates a function that computes the average night time temperature of a given device id and term id
create or replace function t_n (p_device_id character varying, p_term_id int) returns real language plpgsql as $$ begin return avg(value)
FROM hourly_temp(p_device_id, p_term_id)
WHERE is_night(tstamp);
end;
$$ 

-- Creates a function that computes the average night time temperature of a term id for all devices
create or replace function t_n (p_term_id int) returns table (device_id character varying, avg real) language plpgsql as $$ begin return query
SELECT ht.device_id,
	avg(value)::real
FROM hourly_temp(p_term_id) ht
WHERE is_night(ht.tstamp)
GROUP BY ht.device_id;
end;
$$ 

-- Creates a function that computes the average difference between day and night temperature over all days of a given device id and term id
create or replace function t_nd (p_device_id character varying, p_term_id int) returns real language plpgsql as $$ begin return avg(
		t_d(p_device_id, p_term_id) - t_n(p_device_id, p_term_id)
	)::real;
end;
$$ 

-- Creates a function that computes the average difference between day and night temperature over all days of a term id for all devices
create or replace function t_nd (p_term_id int) returns table (device_id character varying, t_nd real) language plpgsql as $$ begin return query
SELECT t_d.device_id,
	avg(t_d.avg - t_n.avg)::real AS t_nd
FROM t_d(p_term_id) t_d
	JOIN t_n(p_term_id) t_n ON t_d.device_id = t_n.device_id
GROUP BY t_d.device_id;
end;
$$ 

-- Computes the correlation to the closest hobo from one and two years ago
SELECT md.device_id,
	corr_closest_device(md.device_id, 11, 9) as t_corr1y,
	corr_closest_device(md.device_id, 11, 7) as t_corr2y
FROM metadata md
WHERE term_id = 11 
ORDER BY md.device_id 


-- The final view!!!!
	CREATE VIEW indices AS
SELECT md.device_id,
	t_avg(md.device_id, 11) as t_avg,
	t_d(md.device_id, 11) as t_d,
	t_n(md.device_id, 11) as t_n,
	t_nd(md.device_id, 11) as t_nd,
	corr_closest_device(md.device_id, 11, 9) as t_corr1y,
	corr_closest_device(md.device_id, 11, 7) as t_corr2y
FROM metadata md
WHERE term_id = 11
ORDER BY md.device_id