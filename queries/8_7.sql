select
	O_YEAR,
	sum(case
		when nation = 'SAUDI ARABIA' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from O_ORDERDATE) as O_YEAR,
			L_EXTENDEDPRICE * (1 - L_DISCOUNT) as volume,
			n2.N_NAME as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			P_PARTKEY = L_PARTKEY
			and S_SUPPKEY = L_SUPPKEY
			and L_ORDERKEY = O_ORDERKEY
			and O_CUSTKEY = C_CUSTKEY
			and C_NATIONKEY = n1.N_NATIONKEY
			and n1.N_REGIONKEY = R_REGIONKEY
			and R_NAME = 'MIDDLE EAST'
			and S_NATIONKEY = n2.N_NATIONKEY
			and O_ORDERDATE between date '1995-01-01' and date '1996-12-31'
			and P_TYPE = 'ECONOMY BRUSHED TIN'
	) as all_nations
group by
	O_YEAR
order by
	O_YEAR;
