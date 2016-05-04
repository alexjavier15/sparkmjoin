select
	C_COUNT,
	count(*) as custdist
from
	(
		select
			C_CUSTKEY,
			count(O_ORDERKEY)
		from
			customer left outer join orders on
				C_CUSTKEY = O_CUSTKEY
				and O_COMMENT not like '%express%accounts%'
		group by
			C_CUSTKEY
	) as C_ORDERS (C_CUSTKEY, C_COUNT)
group by
	C_COUNT
order by
	custdist desc,
	C_COUNT desc;
