select
	nation,
	O_YEAR,
	sum(amount) as sum_profit
from
	(
		select
			N_NAME as nation,
			extract(year from O_ORDERDATE) as O_YEAR,
			L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			S_SUPPKEY = L_SUPPKEY
			and PS_SUPPKEY = L_SUPPKEY
			and PS_PARTKEY = L_PARTKEY
			and P_PARTKEY = L_PARTKEY
			and O_ORDERKEY = L_ORDERKEY
			and S_NATIONKEY = N_NATIONKEY
			and P_NAME like '%navajo%'
	) as profit
group by
	nation,
	O_YEAR
order by
	nation,
	O_YEAR desc;
