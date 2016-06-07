select
	S_NAME,
	S_ADDRESS
from
	supplier,
	nation,
	 (
		select
			PS_SUPPKEY as PS_SUPPKEY2
		from
			partsupp,
			(
				select
					PS_PARTKEY as PS_PARTKEY1,
					PS_SUPPKEY as PS_SUPPKEY1,
					0.5 * sum(L_QUANTITY) as MOY
				from	part,
					partsupp,
					lineitem
				where
					L_PARTKEY = PS_PARTKEY
					and P_PARTKEY = PS_PARTKEY
					and P_NAME like 'bisque%'
					and L_SUPPKEY = PS_SUPPKEY
					and L_SHIPDATE >= date '1994-01-01'
					and L_SHIPDATE < date '1994-01-01' + interval 1 year
				group by 
				PS_PARTKEY,
				PS_SUPPKEY
			) tmp1
		where
	
			PS_PARTKEY = PS_PARTKEY1
			and PS_SUPPKEY = PS_SUPPKEY1
			and PS_AVAILQTY > MOY
	) tmp2
	where
	S_SUPPKEY = PS_SUPPKEY2
	and S_NATIONKEY = N_NATIONKEY
	and N_NAME = 'ROMANIA'
order by
	S_NAME;
