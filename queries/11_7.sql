select
	PS_PARTKEY,
	sum(PS_SUPPLYCOST * PS_AVAILQTY) as value
from
	partsupp,
	supplier,
	nation
where
	PS_SUPPKEY = S_SUPPKEY
	and S_NATIONKEY = N_NATIONKEY
	and N_NAME = 'IRAN'
group by
	PS_PARTKEY having
		sum(PS_SUPPLYCOST * PS_AVAILQTY) > (
			select
				sum(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				PS_SUPPKEY = S_SUPPKEY
				and S_NATIONKEY = N_NATIONKEY
				and N_NAME = 'IRAN'
		)
order by
	value desc;
