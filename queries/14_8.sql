select
	100.00 * sum(case
		when P_TYPE like 'PROMO%'
			then L_EXTENDEDPRICE * (1 - L_DISCOUNT)
		else 0
	end) / sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as promo_revenue
from
	lineitem,
	part
where
	L_PARTKEY = P_PARTKEY
	and L_SHIPDATE >= date '1993-03-01'
	and L_SHIPDATE < date '1993-03-01' + interval '1 month';
