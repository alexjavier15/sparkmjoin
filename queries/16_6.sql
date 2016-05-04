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
	and P_BRAND <> 'Brand#45'
	and P_TYPE not like 'SMALL BRUSHED%'
	and P_SIZE in (39, 9, 38, 3, 24, 40, 8, 31)
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
