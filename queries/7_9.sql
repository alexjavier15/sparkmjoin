select
	supp_nation,
	cust_nation,
	L_YEAR,
	sum(volume) as revenue
from
	(
		select
			n1.N_NAME as supp_nation,
			n2.N_NAME as cust_nation,
			extract(year from L_SHIPDATE) as L_YEAR,
			L_EXTENDEDPRICE * (1 - L_DISCOUNT) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			S_SUPPKEY = L_SUPPKEY
			and O_ORDERKEY = L_ORDERKEY
			and C_CUSTKEY = O_CUSTKEY
			and S_NATIONKEY = n1.N_NATIONKEY
			and C_NATIONKEY = n2.N_NATIONKEY
			and (
				(n1.N_NAME = 'MOROCCO' and n2.N_NAME = 'EGYPT')
				or (n1.N_NAME = 'EGYPT' and n2.N_NAME = 'MOROCCO')
			)
			and L_SHIPDATE between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	L_YEAR
order by
	supp_nation,
	cust_nation,
	L_YEAR;
