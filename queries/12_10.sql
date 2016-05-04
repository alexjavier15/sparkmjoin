select
	L_SHIPMODE,
	sum(case
		when O_ORDERPRIORITY = '1-URGENT'
			or O_ORDERPRIORITY = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when O_ORDERPRIORITY <> '1-URGENT'
			and O_ORDERPRIORITY <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	O_ORDERKEY = L_ORDERKEY
	and L_SHIPMODE in ('RAIL', 'AIR')
	and L_COMMITDATE < L_RECEIPTDATE
	and L_SHIPDATE < L_COMMITDATE
	and L_RECEIPTDATE >= date '1993-01-01'
	and L_RECEIPTDATE < date '1993-01-01' + interval '1 year'
group by
	L_SHIPMODE
order by
	L_SHIPMODE;
