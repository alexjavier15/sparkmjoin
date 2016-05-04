select
	sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue
from
	lineitem
where
	L_SHIPDATE >= date '1997-01-01'
	and L_SHIPDATE < date '1997-01-01' + interval '1 year'
	and L_DISCOUNT between 0.04 - 0.01 and 0.04 + 0.01
	and L_QUANTITY < 25;
