select
	O_ORDERPRIORITY,
	count(*) as order_count
from
	orders
where
	O_ORDERDATE >= date '1995-03-01'
	and O_ORDERDATE < date '1995-03-01' + interval '3 months'
	and exists (
		select
			*
		from
			lineitem
		where
			L_ORDERKEY = O_ORDERKEY
			and L_COMMITDATE < L_RECEIPTDATE
	)
group by
	O_ORDERPRIORITY
order by
	O_ORDERPRIORITY;
