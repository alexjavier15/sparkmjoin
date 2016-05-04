select
	sum(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) as revenue
from
	lineitem,
	part
where
	(
		P_PARTKEY = L_PARTKEY
		and P_BRAND = 'Brand#42'
		and P_CONTAINER in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and L_QUANTITY >= 10 and L_QUANTITY <= 10 + 10
		and P_SIZE between 1 and 5
		and L_SHIPMODE in ('AIR', 'AIR REG')
		and L_SHIPINSTRUCT = 'DELIVER IN PERSON'
	)
	or
	(
		P_PARTKEY = L_PARTKEY
		and P_BRAND = 'Brand#23'
		and P_CONTAINER in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and L_QUANTITY >= 14 and L_QUANTITY <= 14 + 10
		and P_SIZE between 1 and 10
		and L_SHIPMODE in ('AIR', 'AIR REG')
		and L_SHIPINSTRUCT = 'DELIVER IN PERSON'
	)
	or
	(
		P_PARTKEY = L_PARTKEY
		and P_BRAND = 'Brand#31'
		and P_CONTAINER in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and L_QUANTITY >= 23 and L_QUANTITY <= 23 + 10
		and P_SIZE between 1 and 15
		and L_SHIPMODE in ('AIR', 'AIR REG')
		and L_SHIPINSTRUCT = 'DELIVER IN PERSON'
	);
