select
	N_NAME,
	sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	C_CUSTKEY = O_CUSTKEY
	and L_ORDERKEY = O_ORDERKEY
	and L_SUPPKEY = S_SUPPKEY
	and C_NATIONKEY = S_NATIONKEY
	and S_NATIONKEY = N_NATIONKEY
	and N_REGIONKEY = R_REGIONKEY
	and R_NAME = 'MIDDLE EAST'
	and O_ORDERDATE >= date '1996-01-01'
	and O_ORDERDATE < date '1996-01-01' + interval 1 year
group by
	N_NAME
