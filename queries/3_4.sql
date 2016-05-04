select
	L_ORDERKEY,
	sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,
	O_ORDERDATE,
	O_SHIPPRIORITY
from
	customer,
	orders,
	lineitem
where
	C_MKTSEGMENT = 'AUTOMOBILE'
	and C_CUSTKEY = O_CUSTKEY
	and L_ORDERKEY = O_ORDERKEY
	and O_ORDERDATE < date '1995-03-29'
	and L_SHIPDATE > date '1995-03-29'
group by
	L_ORDERKEY,
	O_ORDERDATE,
	O_SHIPPRIORITY
order by
	revenue desc,
	O_ORDERDATE
