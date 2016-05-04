select
	sum(L_EXTENDEDPRICE) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	P_PARTKEY = L_PARTKEY
	and P_BRAND = 'Brand#54'
	and P_CONTAINER = 'WRAP BOX'
	and L_QUANTITY < (
		select
			0.2 * avg(L_QUANTITY)
		from
			lineitem
		where
			L_PARTKEY = P_PARTKEY
	);
