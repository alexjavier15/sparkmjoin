select
	C_NAME,
	C_CUSTKEY,
	O_ORDERKEY,
	O_ORDERDATE,
	O_TOTALPRICE,
	sum(L_QUANTITY)
from
	customer,
	orders,
	lineitem,
	(
		select
			L_ORDERKEY as LO1
		from
			lineitem
		group by
			L_ORDERKEY having
				sum(L_QUANTITY) > 313
	) SUM1
where
	O_ORDERKEY = LO1
	and C_CUSTKEY = O_CUSTKEY
	and O_ORDERKEY = L_ORDERKEY
group by
	C_NAME,
	C_CUSTKEY,
	O_ORDERKEY,
	O_ORDERDATE,
	O_TOTALPRICE
order by
	O_TOTALPRICE desc,
	O_ORDERDATE
limit 100;
