create or replace
view v_stockout_risk as
with params as (
select
	90::int as lookback_days
),
as_of as (
select
	coalesce(max(order_date), current_date) as as_of_date
from
	sales
),
date_spine as (
select
	generate_series(
(select as_of_date from as_of) - (select lookback_days from params) * interval '1 day' + interval '1 day',
(select as_of_date from as_of),
interval '1 day'
)::date as dt
),
inv as (
select
	i.sku_id,
	i.warehouse_id,
	(i.current_stock - coalesce(i.reserved_stock, 0))::numeric as available_stock,
	i.unit_price,
	sm.lead_time_default
from
	inventory i
join sku_master sm
		using (sku_id)
),
sales_per_day as (
select
	s.sku_id,
	s.warehouse_id,
	s.order_date as dt,
	sum(s.quantity_sold)::numeric as qty
from
	sales s
group by
	1,
	2,
	3
),
dense as (
select
	inv.sku_id,
	inv.warehouse_id,
	ds.dt,
	coalesce(spd.qty, 0) as qty,
	inv.available_stock,
	inv.unit_price,
	inv.lead_time_default
from
	inv
cross join date_spine ds
left join sales_per_day spd
on
	spd.sku_id = inv.sku_id
	and spd.warehouse_id = inv.warehouse_id
	and spd.dt = ds.dt
),
agg as (
select
	sku_id,
	warehouse_id,
	MAX(available_stock) as available_stock,
	MAX(unit_price) as unit_price,
	MAX(lead_time_default) as lead_time_days,
	AVG(qty) as avg_daily_units_lookback,
	AVG(qty) filter (
	where dt > (
	select
		as_of_date
	from
		as_of) - interval '30 days') as avg_daily_units_30d,
	AVG(qty) filter (
	where dt > (
	select
		as_of_date
	from
		as_of) - interval '7 days') as avg_daily_units_7d
from
	dense
group by
	sku_id,
	warehouse_id
)
,
risk as (
select
	sku_id,
	warehouse_id,
	available_stock,
	unit_price,
	lead_time_days,
	coalesce(
nullif(avg_daily_units_30d, 0),
nullif(avg_daily_units_lookback, 0),
nullif(avg_daily_units_7d, 0),
0
) as velocity_units_per_day
from
	agg
)
select
	r.sku_id,
	r.warehouse_id,
	r.available_stock,
	r.velocity_units_per_day,
	case
		when r.velocity_units_per_day = 0 then null
		else round(r.available_stock / r.velocity_units_per_day, 2)
	end as days_until_stockout,
	r.lead_time_days,
	(case
		when r.velocity_units_per_day = 0 then false
		else (r.available_stock / r.velocity_units_per_day) < r.lead_time_days
	end) as at_risk_flag,
	round(
greatest(r.lead_time_days - coalesce(
case when r.velocity_units_per_day = 0 then null
else r.available_stock / r.velocity_units_per_day end,
r.lead_time_days
), 0) * r.velocity_units_per_day * r.unit_price
, 2)as revenue_at_risk_estimate
from
	risk r;