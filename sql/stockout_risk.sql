CREATE OR replace VIEW v_stockout_risk AS
WITH params AS (
SELECT
	90::int AS lookback_days
),
as_of AS (
SELECT
	COALESCE(max(order_date), current_date) AS as_of_date
FROM
	sales
),
date_spine AS (
SELECT
	generate_series(
(SELECT as_of_date FROM as_of) - (SELECT lookback_days FROM params) * INTERVAL '1 day' + INTERVAL '1 day',
(SELECT as_of_date FROM as_of),
INTERVAL '1 day'
)::date AS dt
),
inv AS (
SELECT
	i.sku_id,
	i.warehouse_id,
	(i.current_stock - COALESCE(i.reserved_stock, 0))::NUMERIC AS available_stock,
	i.unit_price,
	sm.lead_time_default
FROM
	inventory i
JOIN sku_master sm
		USING (sku_id)
),
sales_per_day AS (
SELECT
	s.sku_id,
	s.warehouse_id,
	s.order_date AS dt,
	sum(s.quantity_sold)::NUMERIC AS qty
FROM
	sales s
GROUP BY
	1,
	2,
	3
),
dense AS (
SELECT
	inv.sku_id,
	inv.warehouse_id,
	ds.dt,
	COALESCE(spd.qty, 0) AS qty,
	inv.available_stock,
	inv.unit_price,
	inv.lead_time_default
FROM
	inv
CROSS JOIN date_spine ds
LEFT JOIN sales_per_day spd
ON
	spd.sku_id = inv.sku_id
	AND spd.warehouse_id = inv.warehouse_id
	AND spd.dt = ds.dt
),
agg AS (
SELECT
	sku_id,
	warehouse_id,
	max(available_stock) AS available_stock,
	max(unit_price) AS unit_price,
	max(lead_time_default) AS lead_time_days,
	avg(qty) AS avg_daily_units_lookback,
	avg(qty) FILTER (
WHERE
	dt > (
	SELECT
		as_of_date
	FROM
		as_of) - INTERVAL '30 days'
) AS avg_daily_units_30d
avg(qty) FILTER (
WHERE
	dt > (
	SELECT
		as_of_date
	FROM
		as_of) - INTERVAL '7 days'
) AS avg_daily_units_7d
FROM
	dense
GROUP BY
	sku_id,
	warehouse_id
),
risk AS (
SELECT
	sku_id,
	warehouse_id,
	available_stock,
	unit_price,
	lead_time_days,
	COALESCE(
NULLIF(avg_daily_units_30d, 0),
NULLIF(avg_daily_units_lookback, 0),
NULLIF(avg_daily_units_7d, 0),
0
) AS velocity_units_per_day
FROM
	agg
)
SELECT
	r.sku_id,
	r.warehouse_id,
	r.available_stock,
	r.velocity_units_per_day,
	CASE
		WHEN r.velocity_units_per_day = 0 THEN NULL
		ELSE round(r.available_stock / r.velocity_units_per_day, 2)
	END AS days_until_stockout,
	r.lead_time_days,
	(CASE
		WHEN r.velocity_units_per_day = 0 THEN FALSE
		ELSE (r.available_stock / r.velocity_units_per_day) < r.lead_time_days
	END) AS at_risk_flag,
	round(
greatest(r.lead_time_days - COALESCE(
CASE WHEN r.velocity_units_per_day = 0 THEN NULL
ELSE r.available_stock / r.velocity_units_per_day END,
r.lead_time_days
), 0) * r.velocity_units_per_day * r.unit_price
, 2)AS revenue_at_risk_estimate
FROM
	risk r;