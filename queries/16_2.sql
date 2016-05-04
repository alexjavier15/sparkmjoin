select
	P_BRAND,
	P_TYPE,
	P_SIZE,
	count(distinct PS_SUPPKEY) as supplier_cnt
from
	partsupp,
	part
where
	P_PARTKEY = PS_PARTKEY
	and P_BRAND <> 'Brand#14'
	and P_TYPE not like 'ECONOMY POLISHED%'
	and P_SIZE in (26, 25, 50, 22, 36, 23, 47, 15)
	and PS_SUPPKEY not in (
		select
			S_SUPPKEY
		from
			supplier
		where
			S_COMMENT like '%Customer%Complaints%'
	)
group by
	P_BRAND,
	P_TYPE,
	P_SIZE
order by
	supplier_cnt desc,
	P_BRAND,
	P_TYPE,
	P_SIZE;
